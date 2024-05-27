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

#pragma once

#include "operators/writer/VeloxParquetDatasource.h"
#include "utils/ConfigExtractor.h"
#include "utils/VeloxArrowUtils.h"

#include <string>

#include "arrow/c/bridge.h"
#include "compute/VeloxRuntime.h"

#include "velox/common/compression/Compression.h"
#include "velox/core/QueryConfig.h"
#include "velox/core/QueryCtx.h"
#include "velox/dwio/common/Options.h"

namespace gluten {

class VeloxParquetDatasourceABFS final : public VeloxParquetDatasource {
 public:
  VeloxParquetDatasourceABFS(
      const std::string& filePath,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
      std::shared_ptr<facebook::velox::memory::MemoryPool> sinkPool,
      std::shared_ptr<arrow::Schema> schema)
      : VeloxParquetDatasource(filePath, veloxPool, sinkPool, schema) {}
  void initSink(const std::unordered_map<std::string, std::string>& sparkConfs) override {
    auto hiveConf = getHiveConfig(std::make_shared<facebook::velox::core::MemConfig>(sparkConfs));
    auto fileSystem = filesystems::getFileSystem(filePath_, hiveConf);
    auto* abfsFileSystem = dynamic_cast<filesystems::abfs::AbfsFileSystem*>(fileSystem.get());
    sink_ = std::make_unique<dwio::common::WriteFileSink>(
        abfsFileSystem->openFileForWrite(filePath_, {{}, sinkPool_.get()}), filePath_);
  }
};
} // namespace gluten
