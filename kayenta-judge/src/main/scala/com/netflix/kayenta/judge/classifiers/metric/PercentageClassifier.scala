/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.kayenta.judge.classifiers.metric

import com.netflix.kayenta.judge.Metric
import com.netflix.kayenta.judge.preprocessing.Transforms
import com.netflix.kayenta.judge.stats.EffectSizes
import com.netflix.kayenta.mannwhitney.{MannWhitney, MannWhitneyParams}
import org.apache.commons.math3.stat.StatUtils
import com.typesafe.scalalogging.StrictLogging

// case class ComparisonResult(classification: MetricClassificationLabel, reason: Option[String], deviation: Double)

class PercentageClassifier(upperThreshold: Double=0.20,
                            lowerThreshold: Double=0.20) extends BaseMetricClassifier {

  /**
    * Compare the experiment to the control using the Percentage Test
    */
  private def compare(control: Metric,
                      experiment: Metric,
                      direction: MetricDirection): ComparisonResult = {

    // Calculate Control & Experiment Average
    var controlTotal : Double = 0.0d
    for (i <- 0 until control.values.length) {
      controlTotal += control.values(i)
    }
    val controlAverage = controlTotal/control.values.length 

    var experimentTotal : Double = 0.0d
    for (i <- 0 until experiment.values.length) {
      experimentTotal += experiment.values(i)
    }
    val experimentAverage = experimentTotal/experiment.values.length 

    // logger.info("Control Sum: " + control.values.sum)

    // Calculate percetage difference
    val perChange = ((experimentAverage - controlAverage)/controlAverage)

    //Check if the experiment is high in comparison to the control
    val isHigh = {
      (direction == MetricDirection.Increase || direction == MetricDirection.Either) &&
        (perChange > upperThreshold)
    }

    //Check if the experiment is low in comparison to the control
    val isLow = {
      (direction == MetricDirection.Decrease || direction == MetricDirection.Either) &&
        (perChange < (0.0d-lowerThreshold))
    }

    if(isHigh){
      val reason = s"${experiment.name} was classified as $High"
      ComparisonResult(High, Some(reason), perChange)

    }else if(isLow){
      val reason = s"${experiment.name} was classified as $Low"
      ComparisonResult(Low, Some(reason), perChange)

    } else {
      ComparisonResult(Pass, None, perChange)
    }
  }

  override def classify(control: Metric,
                        experiment: Metric,
                        direction: MetricDirection,
                        nanStrategy: NaNStrategy,
                        isCriticalMetric: Boolean): MetricClassification = {

    //Check if there is no-data for the experiment or control
    if (experiment.values.isEmpty || control.values.isEmpty) {
      if (nanStrategy == NaNStrategy.Remove) {
        return MetricClassification(Nodata, None, 1.0, isCriticalMetric)
      } else {
        return MetricClassification(Pass, None, 1.0, critical = false)
      }
    }

    //Check if the experiment and control data are equal
    if (experiment.values.sorted.sameElements(control.values.sorted)) {
      val reason = s"The ${experiment.label} and ${control.label} data are identical"
      return MetricClassification(Pass, Some(reason), 1.0, critical = false)
    }

    //Check the number of unique observations
    if (experiment.values.union(control.values).distinct.length == 1) {
      return MetricClassification(Pass, None, 1.0, critical = false)
    }

    //Compare the experiment to the control using the Percentage Test, checking the magnitude of the effect
    val comparison = compare(control, experiment, direction)

    //Check if the metric was marked as critical, and if the metric was classified as a failure (High, Low)
    if(isCriticalMetric && comparison.classification == High){
      val reason = s"The metric ${experiment.name} was classified as $High (Critical)"
      MetricClassification(High, Some(reason), 1.0, critical = true)

    }else if(isCriticalMetric && comparison.classification == Low){
      val reason = s"The metric ${experiment.name} was classified as $Low (Critical)"
      MetricClassification(Low, Some(reason), 1.0, critical = true)

    }else if(isCriticalMetric && (comparison.classification == Nodata)){
      MetricClassification(comparison.classification, comparison.reason, 1.0, critical = true)

    }else{
      MetricClassification(comparison.classification, comparison.reason, comparison.deviation, critical = false)
    }

  }
}
