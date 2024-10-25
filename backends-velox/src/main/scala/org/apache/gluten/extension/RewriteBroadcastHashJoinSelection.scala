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
package org.apache.gluten.extension

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.expressions.RowOrdering
import org.apache.spark.sql.catalyst.optimizer.{BuildSide, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, JoinHint, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

import java.util.Locale

object Helper extends JoinSelectionHelper {
  private val conf = SQLConf.get
  private val hintErrorHandler = conf.hintErrorHandler

  def checkHintBuildSide(
      onlyLookingAtHint: Boolean,
      buildSide: Option[BuildSide],
      joinType: JoinType,
      hint: JoinHint,
      isBroadcast: Boolean): Unit = {
    def invalidBuildSideInHint(hintInfo: HintInfo, buildSide: String): Unit = {
      hintErrorHandler.joinHintNotSupported(
        hintInfo,
        s"build $buildSide for ${joinType.sql.toLowerCase(Locale.ROOT)} join")
    }

    if (onlyLookingAtHint && buildSide.isEmpty) {
      if (isBroadcast) {
        // check broadcast hash join
        if (hintToBroadcastLeft(hint)) invalidBuildSideInHint(hint.leftHint.get, "left")
        if (hintToBroadcastRight(hint)) invalidBuildSideInHint(hint.rightHint.get, "right")
      } else {
        // check shuffle hash join
        if (hintToShuffleHashJoinLeft(hint)) invalidBuildSideInHint(hint.leftHint.get, "left")
        if (hintToShuffleHashJoinRight(hint)) invalidBuildSideInHint(hint.rightHint.get, "right")
      }
    }
  }
}

case class RewriteBroadcastHashJoinSelection(spark: SparkSession) extends Strategy {
  private val conf = SQLConf.get

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    import spark.sessionState.planner.JoinSelection._
    plan match {
      case j @ ExtractEquiJoinKeys(
            joinType,
            leftKeys,
            rightKeys,
            nonEquiCond,
            _,
            left,
            right,
            hint) =>
        def createBroadcastHashJoin(onlyLookingAtHint: Boolean) = {
          val buildSide =
            getBroadcastBuildSide(left, right, joinType, hint, onlyLookingAtHint, conf)
          Helper.checkHintBuildSide(onlyLookingAtHint, buildSide, joinType, hint, true)
          buildSide.map {
            buildSide =>
              Seq(
                org.apache.spark.sql.execution.joins.BroadcastHashJoinExec(
                  leftKeys,
                  rightKeys,
                  joinType,
                  buildSide,
                  nonEquiCond,
                  planLater(left),
                  planLater(right)))
          }
        }

        def createShuffleHashJoin(onlyLookingAtHint: Boolean) = {
          val buildSide =
            getShuffleHashJoinBuildSide(left, right, joinType, hint, onlyLookingAtHint, conf)
          Helper.checkHintBuildSide(onlyLookingAtHint, buildSide, joinType, hint, false)
          buildSide.map {
            buildSide =>
              Seq(
                org.apache.spark.sql.execution.joins.ShuffledHashJoinExec(
                  leftKeys,
                  rightKeys,
                  joinType,
                  buildSide,
                  nonEquiCond,
                  planLater(left),
                  planLater(right)))
          }
        }

        def createSortMergeJoin() = {
          if (RowOrdering.isOrderable(leftKeys)) {
            Some(
              Seq(
                org.apache.spark.sql.execution.joins.SortMergeJoinExec(
                  leftKeys,
                  rightKeys,
                  joinType,
                  nonEquiCond,
                  planLater(left),
                  planLater(right))))
          } else {
            None
          }
        }

        def createCartesianProduct() = {
          if (joinType.isInstanceOf[InnerLike] && !hintToNotBroadcastAndReplicate(hint)) {
            // `CartesianProductExec` can't implicitly evaluate equal join condition, here we should
            // pass the original condition which includes both equal and non-equal conditions.
            Some(
              Seq(
                org.apache.spark.sql.execution.joins
                  .CartesianProductExec(planLater(left), planLater(right), j.condition)))
          } else {
            None
          }
        }

        def createJoinWithoutHint() = {
          createBroadcastHashJoin(false)
            .orElse(createShuffleHashJoin(false))
            .orElse(createSortMergeJoin())
            .orElse(createCartesianProduct())
            .getOrElse {
              // This join could be very slow or OOM
              // Build the smaller side unless the join requires a particular build side
              // (e.g. NO_BROADCAST_AND_REPLICATION hint)
              val requiredBuildSide = getBroadcastNestedLoopJoinBuildSide(hint)
              val buildSide = requiredBuildSide.getOrElse(getSmallerSide(left, right))
              Seq(
                org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec(
                  planLater(left),
                  planLater(right),
                  buildSide,
                  joinType,
                  j.condition))
            }
        }

        if (hint.isEmpty) {
          createJoinWithoutHint()
        } else {
          createBroadcastHashJoin(true)
            .orElse {
              if (hintToSortMergeJoin(hint)) createSortMergeJoin() else None
            }
            .orElse(createShuffleHashJoin(true))
            .orElse {
              if (hintToShuffleReplicateNL(hint)) createCartesianProduct() else None
            }
            .getOrElse(createJoinWithoutHint())
        }
      case _ => Nil
    }
  }
}
