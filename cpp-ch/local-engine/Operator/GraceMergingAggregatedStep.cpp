/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "GraceMergingAggregatedStep.h"
#include <Interpreters/JoinUtils.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/CurrentThread.h>
#include <Common/formatReadable.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
static DB::ITransformingStep::Traits getTraits()
{
    return DB::ITransformingStep::Traits
    {
        {
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

static DB::Block buildOutputHeader(const DB::Block & input_header_, const DB::Aggregator::Params params_)
{
    return params_.getHeader(input_header_, true);
}

GraceMergingAggregatedStep::GraceMergingAggregatedStep(
    DB::ContextPtr context_,
    const DB::DataStream & input_stream_,
    DB::Aggregator::Params params_)
    : DB::ITransformingStep(
        input_stream_, buildOutputHeader(input_stream_.header, params_), getTraits())
    , context(context_)
    , params(std::move(params_))
{
}

void GraceMergingAggregatedStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings &)
{
    auto num_streams = pipeline.getNumStreams();
    auto transform_params = std::make_shared<DB::AggregatingTransformParams>(pipeline.getHeader(), params, true);
    pipeline.resize(1);
    auto build_transform = [&](DB::OutputPortRawPtrs outputs)
    {
        DB::Processors new_processors;
        for (auto & output : outputs)
        {
            auto op = std::make_shared<GraceMergingAggregatedTransform>(pipeline.getHeader(), transform_params, context);
            new_processors.push_back(op);
            DB::connect(*output, op->getInputs().front());
        }
        return new_processors;
    };
    pipeline.transform(build_transform);
    pipeline.resize(num_streams, true);
}

void GraceMergingAggregatedStep::describeActions(DB::IQueryPlanStep::FormatSettings & settings) const
{
    return params.explain(settings.out, settings.offset);
}

void GraceMergingAggregatedStep::describeActions(DB::JSONBuilder::JSONMap & map) const
{
    params.explain(map);
}

void GraceMergingAggregatedStep::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), buildOutputHeader(input_streams.front().header, params), getDataStreamTraits());
}

GraceMergingAggregatedTransform::GraceMergingAggregatedTransform(const DB::Block &header_, DB::AggregatingTransformParamsPtr params_, DB::ContextPtr context_)
    : IProcessor({header_}, {params_->getHeader()})
    , header(header_)
    , params(params_)
    , context(context_)
    , tmp_data_disk(std::make_unique<DB::TemporaryDataOnDisk>(context_->getTempDataOnDisk()))
{
    current_data_variants = std::make_shared<DB::AggregatedDataVariants>();
    // bucket 0 is for in-memory data, it's just a placeholder.
    buckets.emplace(0, BufferFileStream());
}

GraceMergingAggregatedTransform::~GraceMergingAggregatedTransform()
{
    LOG_INFO(
        logger,
        "Metrics. total_input_blocks: {}, total_input_rows: {}, total_output_blocks: {}, total_output_rows: {}, total_spill_disk_bytes: "
        "{}, total_spill_disk_time: {}, total_read_disk_time: {}, total_scatter_time: {}",
        total_input_blocks,
        total_input_rows,
        total_output_blocks,
        total_output_rows,
        total_spill_disk_bytes,
        total_spill_disk_time,
        total_read_disk_time,
        total_scatter_time);
}

GraceMergingAggregatedTransform::Status GraceMergingAggregatedTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();
    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }
    if (has_output)
    {
        if (output.canPush())
        {
            total_output_rows += output_chunk.getNumRows();
            total_output_blocks++;
            output.push(std::move(output_chunk));
            has_output = false;
        }
        return Status::PortFull;
    }

    if (has_input)
        return Status::Ready;

    if (!input_finished)
    {
        if (input.isFinished())
        {
            input_finished = true;
            return Status::Ready;
        }
        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;
        input_chunk = input.pull(true);
        total_input_rows += input_chunk.getNumRows();
        total_input_blocks++;
        has_input = true;
        return Status::Ready;
    }

    if (current_bucket_index >= getBucketsNum() && current_final_blocks.empty())
    {
        output.finish();
        return Status::Finished;
    }
    return Status::Ready;
}

void GraceMergingAggregatedTransform::work()
{
    if (has_input)
    {
        assert(!input_finished);
        auto block = header.cloneWithColumns(input_chunk.detachColumns());
        mergeOneBlock(block);
        has_input = false;
    }
    else
    {
        assert(input_finished);
        auto pop_one_chunk = [&]()
        {
            while (!current_final_blocks.empty())
            {
                if (!current_final_blocks.front().rows())
                {
                    current_final_blocks.pop_front();
                    continue;
                }
                
                auto & block = current_final_blocks.front();
                output_chunk = DB::Chunk(block.getColumns(), block.rows());
                current_final_blocks.pop_front();
                has_output = true;
                return;
            }
        };

        if (current_final_blocks.empty())
        {
            if (current_bucket_index >= getBucketsNum())
                return;
            prepareBucketOutputBlocks();
            current_bucket_index++;
            current_data_variants = nullptr;
        }
        pop_one_chunk();
    }
}

void GraceMergingAggregatedTransform::extendBuckets()
{
    auto current_size = getBucketsNum();
    auto next_size = current_size * 2;
    if (next_size > max_buckets)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Too many buckets, limit is {}. Please consider increate offhead size or partitoin number",
            max_buckets);
    LOG_INFO(logger, "extend buckets from {} to {}", current_size, next_size);
    for (size_t i = current_size; i < next_size; ++i)
        buckets.emplace(i, BufferFileStream());
}

void GraceMergingAggregatedTransform::rehashDataVariants()
{
    auto blocks = params->aggregator.convertToBlocks(*current_data_variants, false, 1);

    size_t block_rows = 0;
    size_t block_memory_usage = 0;
    for (const auto & block : blocks)
    {
        block_rows += block.rows();
        block_memory_usage += block.allocatedBytes();
    }
    if (block_rows)
        per_key_memory_usage = block_memory_usage * 1.0 / block_rows;

    current_data_variants = std::make_shared<DB::AggregatedDataVariants>();
    no_more_keys = false;
    for (auto & block : blocks)
    {
        auto scattered_blocks = scatterBlock(block);
        block = {};
        for (size_t i = current_bucket_index + 1; i < getBucketsNum(); ++i)
        {
            addBlockIntoFileBucket(i, scattered_blocks[i]);
            scattered_blocks[i] = {};
        }
        params->aggregator.mergeOnBlock(scattered_blocks[current_bucket_index], *current_data_variants, no_more_keys);
    }
};

DB::Blocks GraceMergingAggregatedTransform::scatterBlock(const DB::Block & block)
{
    if (!block.rows())
        return {};
    Stopwatch watch;
    size_t bucket_num = getBucketsNum();
    if (static_cast<size_t>(block.info.bucket_num) == bucket_num)
        return {block};
    auto blocks = DB::JoinCommon::scatterBlockByHash(params->params.keys, block, bucket_num);
    for (auto & new_block : blocks)
    {
        new_block.info.bucket_num = static_cast<Int32>(bucket_num);
    }
    total_scatter_time += watch.elapsedMilliseconds();
    return blocks;
}

void GraceMergingAggregatedTransform::addBlockIntoFileBucket(size_t bucket_index, const DB::Block & block)
{
    if (!block.rows())
        return;
    auto & file_stream = buckets[bucket_index];
    file_stream.blocks.push_back(block);
}

void GraceMergingAggregatedTransform::flushBuckets()
{
    auto before_mem = getMemoryUsage();
    size_t flush_bytes = 0;
    Stopwatch watch;
    for (size_t i = current_bucket_index + 1; i < getBucketsNum(); ++i)
        flush_bytes += flushBucket(i);
    total_spill_disk_time += watch.elapsedMilliseconds();
    total_spill_disk_bytes += flush_bytes;
    LOG_INFO(logger, "flush {} in {} ms, memoery usage: {} -> {}", ReadableSize(flush_bytes), watch.elapsedMilliseconds(), ReadableSize(before_mem), ReadableSize(getMemoryUsage()));
}

size_t GraceMergingAggregatedTransform::flushBucket(size_t bucket_index)
{
    auto & file_stream = buckets[bucket_index];
    if (file_stream.blocks.empty())
        return 0;
    if (!file_stream.file_stream)
        file_stream.file_stream = &tmp_data_disk->createStream(header);
    DB::Blocks blocks;
    size_t flush_bytes = 0;
    while (!file_stream.blocks.empty())
    {
        while (!file_stream.blocks.empty())
        {
            if (!blocks.empty() && blocks.back().info.bucket_num != file_stream.blocks.front().info.bucket_num)
                break;
            blocks.push_back(file_stream.blocks.front());
            file_stream.blocks.pop_front();
        }
        auto bucket = blocks.front().info.bucket_num;
        auto merged_block = DB::concatenateBlocks(blocks);
        merged_block.info.bucket_num = bucket;
        blocks.clear();
        flush_bytes += merged_block.bytes();
        if (merged_block.rows())
        {
            file_stream.file_stream->write(merged_block);
        }
    }
    return flush_bytes;
}

void GraceMergingAggregatedTransform::prepareBucketOutputBlocks()
{
    size_t read_bytes = 0;
    size_t read_rows = 0;
    Stopwatch watch;
    if (!current_data_variants)
    {
        current_data_variants =  std::make_shared<DB::AggregatedDataVariants>();
        no_more_keys = false;
    }
    auto & buffer_file_stream = buckets[current_bucket_index];

    if (buffer_file_stream.file_stream)
    {
        buffer_file_stream.file_stream->finishWriting();
        while (true)
        {
            auto block = buffer_file_stream.file_stream->read();
            if (!block.rows())
                break;
            read_bytes += block.bytes();
            read_rows += block.rows();
            mergeOneBlock(block);
            block = {};
        }
        buffer_file_stream.file_stream = nullptr;
        total_read_disk_time += watch.elapsedMilliseconds();
    }
    if (!buffer_file_stream.blocks.empty())
    {
        for (auto & block : buffer_file_stream.blocks)
        {
            mergeOneBlock(block);
            block = {};
        }
    }
    current_final_blocks = params->aggregator.convertToBlocks(*current_data_variants, true, 1);
    LOG_INFO(logger, "prepare to output bucket {}, read bytes: {}, read rows: {}, time: {} ms", current_bucket_index, ReadableSize(read_bytes), read_rows, watch.elapsedMilliseconds());
}

void GraceMergingAggregatedTransform::mergeOneBlock(const DB::Block &block)
{
    if (!block.rows())
        return;

    if (isMemoryOverflow())
        flushBuckets();

    if (isMemoryOverflow())
    {
        extendBuckets();
        rehashDataVariants();
    }

    LOG_TRACE(
        logger,
        "merge on block, rows: {}, bytes:{}, bucket: {}. current bucket: {}, total bucket: {}, mem used: {}",
        block.rows(),
        ReadableSize(block.bytes()),
        block.info.bucket_num,
        current_bucket_index,
        getBucketsNum(),
        ReadableSize(getMemoryUsage()));

    if (block.info.bucket_num == static_cast<Int32>(getBucketsNum()) || getBucketsNum() == 1)
    {
        params->aggregator.mergeOnBlock(block, *current_data_variants, no_more_keys);
    }
    else
    {
        auto scattered_blocks = scatterBlock(block);
        for (size_t i = current_bucket_index + 1; i < getBucketsNum(); ++i)
        {
            addBlockIntoFileBucket(i, scattered_blocks[i]);
        }
        params->aggregator.mergeOnBlock(scattered_blocks[current_bucket_index], *current_data_variants, no_more_keys);
    }
}

size_t GraceMergingAggregatedTransform::getMemoryUsage()
{
    Int64 current_memory_usage = 0;
    if (auto * memory_tracker_child = DB::CurrentThread::getMemoryTracker())
        if (auto * memory_tracker = memory_tracker_child->getParent())
            current_memory_usage = memory_tracker->get();
    return current_memory_usage < 0 ? 0 : current_memory_usage;
}

bool GraceMergingAggregatedTransform::isMemoryOverflow()
{
    /// More greedy memory usage strategy.
    if (!context->getSettingsRef().max_memory_usage)
        return false;
    auto max_mem_used = context->getSettingsRef().max_memory_usage * 8 / 10;
    auto current_result_rows = current_data_variants->size();
    auto current_mem_used = getMemoryUsage();
    if (per_key_memory_usage > 0)
    {
        if (current_mem_used + per_key_memory_usage * current_result_rows >= max_mem_used)
        {
            LOG_INFO(
                logger,
                "Memory is overflow. current_mem_used: {}, max_mem_used: {}, per_key_memory_usage: {}, aggregator keys: {}, buckets: {}",
                ReadableSize(current_mem_used),
                ReadableSize(max_mem_used),
                ReadableSize(per_key_memory_usage),
                current_result_rows,
                getBucketsNum());
            return true;
        }
    }
    else
    {
        if (current_mem_used * 2 >= context->getSettingsRef().max_memory_usage)
        {
            LOG_INFO(
                logger,
                "Memory is overflow on half of max usage. current_mem_used: {}, max_mem_used: {}, buckets: {}",
                ReadableSize(current_mem_used),
                ReadableSize(context->getSettingsRef().max_memory_usage),
                getBucketsNum());
            return true;
        }
    }
    return false;
}
}