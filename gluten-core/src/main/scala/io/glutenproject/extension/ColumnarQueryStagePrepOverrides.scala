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
package io.glutenproject.extension

import io.glutenproject.{GlutenConfig, GlutenSparkExtensionsInjector}
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.extension.columnar._
import io.glutenproject.utils.PhysicalPlanSelector

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Final, Partial}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarBroadcastExchangeExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

// BroadcastHashJoinExec and it's child BroadcastExec will be cut into different QueryStages,
// so the columnar rules will be applied to the two QueryStages separately, and they cannot
// see each other during transformation. In order to prevent BroadcastExec being transformed
// to columnar while BHJ fallbacks, BroadcastExec need to be tagged not transformable when applying
// queryStagePrepRules.
case class FallbackBroadcastExchange(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = PhysicalPlanSelector.maybe(session, plan) {
    val columnarConf: GlutenConfig = GlutenConfig.getConf
    plan.foreach {
      case bhj: BroadcastHashJoinExec =>
        val buildSidePlan = bhj.buildSide match {
          case BuildLeft => bhj.left
          case BuildRight => bhj.right
        }
        val maybeExchange = buildSidePlan.find {
          case BroadcastExchangeExec(_, _) => true
          case _ => false
        }
        maybeExchange match {
          case Some(exchange @ BroadcastExchangeExec(mode, child)) =>
            val isTransformable =
              if (
                !columnarConf.enableColumnarBroadcastExchange ||
                !columnarConf.enableColumnarBroadcastJoin
              ) {
                ValidationResult.notOk(
                  "columnar broadcast exchange is disabled or " +
                    "columnar broadcast join is disabled")
              } else {
                if (TransformHints.isAlreadyTagged(bhj) && TransformHints.isNotTransformable(bhj)) {
                  ValidationResult.notOk("broadcast join is already tagged as not transformable")
                } else {
                  val bhjTransformer = BackendsApiManager.getSparkPlanExecApiInstance
                    .genBroadcastHashJoinExecTransformer(
                      bhj.leftKeys,
                      bhj.rightKeys,
                      bhj.joinType,
                      bhj.buildSide,
                      bhj.condition,
                      bhj.left,
                      bhj.right,
                      bhj.isNullAwareAntiJoin)
                  val isBhjTransformable = bhjTransformer.doValidate()
                  if (isBhjTransformable.isValid) {
                    val exchangeTransformer = ColumnarBroadcastExchangeExec(mode, child)
                    exchangeTransformer.doValidate()
                  } else {
                    isBhjTransformable
                  }
                }
              }
            TransformHints.tagNotTransformable(bhj, isTransformable)
            TransformHints.tagNotTransformable(exchange, isTransformable)
          case _ =>
          // Skip. This might be the case that the exchange was already
          // executed in earlier stage
        }
      case _ =>
    }
    plan
  }
}

case class FallbackAggregate(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = PhysicalPlanSelector.maybe(session, plan) {
    plan.foreach {
      case agg: ObjectHashAggregateExec if agg.aggregateExpressions.forall(_.mode.equals(Final)) =>
        val transformer = BackendsApiManager.getSparkPlanExecApiInstance
          .genHashAggregateExecTransformer(
            agg.requiredChildDistributionExpressions,
            agg.groupingExpressions,
            agg.aggregateExpressions,
            agg.aggregateAttributes,
            agg.initialInputBufferOffset,
            agg.resultExpressions,
            agg.child
          )
        val validateResult = transformer.doValidate()
        if (!validateResult.isValid) {
          TransformHints.tagNotTransformable(agg, validateResult.reason.get)
          agg.child match {
            case ex: Exchange =>
              ex.child match {
                case partialAgg: ObjectHashAggregateExec
                    if partialAgg.aggregateExpressions.forall(_.mode.equals(Partial)) =>
                  TransformHints.tagNotTransformable(
                    partialAgg,
                    "Make partial agg fall back as final agg falls back")
                case _ =>
              }
            case partialAgg: ObjectHashAggregateExec
                if partialAgg.aggregateExpressions.forall(_.mode.equals(Partial)) =>
              TransformHints.tagNotTransformable(
                partialAgg,
                "Make partial agg fall back as final agg falls back")
            case _ =>
          }
        }
      case _ =>
    }
    plan
  }
}

object ColumnarQueryStagePrepOverrides extends GlutenSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    val builders =
      TagBeforeTransformHits.ruleBuilders :+ FallbackBroadcastExchange :+ FallbackAggregate
    builders.foreach(extensions.injectQueryStagePrepRule)
  }
}
