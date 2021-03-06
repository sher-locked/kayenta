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

package com.netflix.kayenta.judge

import java.util

import com.netflix.kayenta.canary.results._
import com.netflix.kayenta.canary.{CanaryClassifierThresholdsConfig, CanaryConfig, CanaryJudge}
import com.netflix.kayenta.judge.classifiers.metric._
import com.netflix.kayenta.judge.classifiers.score.{ScoreClassification, ThresholdScoreClassifier}
import com.netflix.kayenta.judge.detectors.IQRDetector
import com.netflix.kayenta.judge.preprocessing.Transforms
import com.netflix.kayenta.judge.scorers.{ScoreResult, WeightedSumScorer}
import com.netflix.kayenta.judge.stats.DescriptiveStatistics
import com.netflix.kayenta.judge.scorers.ScoringHelper
import com.netflix.kayenta.judge.utils.MapUtils
import com.netflix.kayenta.mannwhitney.MannWhitneyException
import com.netflix.kayenta.metrics.MetricSetPair
import com.typesafe.scalalogging.StrictLogging
import org.springframework.stereotype.Component
import scala.collection.JavaConverters._

import com.netflix.kayenta.judge.classifiers.score
import com.netflix.kayenta.judge.scorers.ScoreResult
import com.netflix.kayenta.canary.results.CanaryAnalysisResult
import com.netflix.kayenta.judge.classifiers.metric.{High, Low, Pass}

import com.fasterxml.jackson.annotation.JsonInclude;
import javax.validation.constraints.NotNull;
import java.util.List;
import scala.collection.mutable._
import java.io._
// import org.saddle._
import sys.process._
import scala.io.Source
import collection.mutable._

@Component
class OpsMxACAJudge extends CanaryJudge with StrictLogging {

  private final val judgeName = "OpsMxACAJudge-v1.0"

  override def isVisible: Boolean = true
  override def getName: String = judgeName

  override def judge(canaryConfig: CanaryConfig,
                     scoreThresholds: CanaryClassifierThresholdsConfig,
                     metricSetPairList: util.List[MetricSetPair]): CanaryJudgeResult = {

    // 1. Save Metrics onto disk
    val metricsList = metricSetPairList.asScala.toList.map { metricPair =>
      val metricName = metricPair.getName
      val experimentValues = metricPair.getValues.get("experiment").asScala.map(_.toDouble).toArray
      val controlValues = metricPair.getValues.get("control").asScala.map(_.toDouble).toArray
      var baseDir = "/home/ubuntu/ScoringAndPCA/"
      var baselineDir = "baseline/"
      var canaryDir = "canary/"
      
      var writer = new PrintWriter(new File(baseDir + baselineDir + metricName + ".csv"))
      for (x <- controlValues) {
        writer.write(x + "\n")
      }
      writer.close()
      
      writer = new PrintWriter(new File(baseDir + canaryDir + metricName + ".csv"))
      for (x <- experimentValues) {
        writer.write(x + "\n")
      }
      writer.close()

      logger.info("OpsMx: Saved baseline & canary data onto disk.")
    }

    // 2. Normalize baseline and canary data

    val exitCode = "/home/ubuntu/miniconda3/bin/python3 /home/opsmxuser/python/normalizer.py /home/opsmxuser/python/test.csv".!
    logger.info("### exitCode ###: " + exitCode)

    var baselineNormDir = "baseline_normalized/"
    var canaryNormDir = "canary_normalized/"
    // TODO: Point to actual directories
    // val exitCode = "/home/ubuntu/miniconda3/bin/python3 /home/opsmxuser/python/normalizer.py /home/opsmxuser/python/test.csv".!

    // 3. Call [Mannwhitney (Wilcoxson) + Quantile Distribution]
    // TODO: Call actual code

    // 4. Read Weights from flat-file
    val groupWeights = Option(canaryConfig.getClassifier.getGroupWeights) match {
      case Some(groups) => groups.asScala.mapValues(_.toDouble).toMap
      case None => Map[String, Double]()
    }

    val bufferedSource = scala.io.Source.fromFile("/home/ubuntu/ScoringAndPCA/GroupWeights.txt")
    val longtermWeights = scala.collection.mutable.Map[String, Double]()
    for (line <- bufferedSource.getLines) {
      val cols = line.split(",").map(_.trim)
      longtermWeights += cols(1) -> cols(2).toDouble
    }

    for ((key, value) <- groupWeights) {
      logger.info("OpsMx:: groupWeights:: Key: " + key + ", Value: " + value)
    }

    for ((key, value) <- longtermWeights) {
      logger.info("OpsMx:: longtermWeights:: Key: " + key + ", Value: " + value)
    }


    //Metric Classification
    val metricResults = metricSetPairList.asScala.toList.map { metricPair =>
      classifyMetric(canaryConfig, metricPair)
    }

    val scoringHelper = new ScoringHelper(judgeName)
    scoringHelper.score(canaryConfig, scoreThresholds, metricResults)
  }

  /**
    * Metric Transformations
    */
  def transformMetric(metric: Metric, nanStrategy: NaNStrategy): Metric = {
    val detector = new IQRDetector(factor = 3.0, reduceSensitivity = true)
    val transform = if (nanStrategy == NaNStrategy.Remove) {
      Function.chain[Metric](Seq(Transforms.removeNaNs(_), Transforms.removeOutliers(_, detector)))
    } else {
      Function.chain[Metric](Seq(Transforms.replaceNaNs(_), Transforms.removeOutliers(_, detector)))
    }
    transform(metric)
  }

  /**
    * Metric Classification
    */
  def classifyMetric(canaryConfig: CanaryConfig, metric: MetricSetPair): CanaryAnalysisResult ={

    val metricConfig = canaryConfig.getMetrics.asScala.find(m => m.getName == metric.getName) match {
      case Some(config) => config
      case None => throw new IllegalArgumentException(s"Could not find metric config for ${metric.getName}")
    }

    val experimentValues = metric.getValues.get("experiment").asScala.map(_.toDouble).toArray
    val controlValues = metric.getValues.get("control").asScala.map(_.toDouble).toArray

    val experiment = Metric(metric.getName, experimentValues, label="Canary")
    val control = Metric(metric.getName, controlValues, label="Baseline")

    val directionalityString = MapUtils.getAsStringWithDefault("either", metricConfig.getAnalysisConfigurations, "canary", "direction")
    val directionality = MetricDirection.parse(directionalityString)

    val nanStrategyString = MapUtils.getAsStringWithDefault("none", metricConfig.getAnalysisConfigurations, "canary", "nanStrategy")
    val nanStrategy = NaNStrategy.parse(nanStrategyString)

    val isCriticalMetric = MapUtils.getAsBooleanWithDefault(false, metricConfig.getAnalysisConfigurations, "canary", "critical")

    //Effect Size Parameters
    val allowedIncrease = MapUtils.getAsDoubleWithDefault(1.0, metricConfig.getAnalysisConfigurations, "canary", "effectSize", "allowedIncrease")
    val allowedDecrease = MapUtils.getAsDoubleWithDefault(1.0, metricConfig.getAnalysisConfigurations, "canary", "effectSize", "allowedDecrease")
    val effectSizeThresholds = (allowedDecrease, allowedIncrease)

    //Critical Effect Size Parameters
    val criticalIncrease = MapUtils.getAsDoubleWithDefault(1.0, metricConfig.getAnalysisConfigurations, "canary", "effectSize", "criticalIncrease")
    val criticalDecrease = MapUtils.getAsDoubleWithDefault(1.0, metricConfig.getAnalysisConfigurations, "canary", "effectSize", "criticalDecrease")
    val criticalThresholds = (criticalDecrease, criticalIncrease)

    //=============================================
    // Metric Transformation (Remove NaN values, etc.)
    // ============================================
    val transformedExperiment = transformMetric(experiment, nanStrategy)
    val transformedControl = transformMetric(control, nanStrategy)

    //=============================================
    // Calculate metric statistics
    // ============================================
    val experimentStats = DescriptiveStatistics.summary(transformedExperiment)
    val controlStats = DescriptiveStatistics.summary(transformedControl)

    //=============================================
    // Metric Classification
    // ============================================
    val mannWhitney = new MannWhitneyClassifier(tolerance = 0.25, confLevel = 0.98, effectSizeThresholds, criticalThresholds)

    val resultBuilder = CanaryAnalysisResult.builder()
      .name(metric.getName)
      .id(metric.getId)
      .tags(metric.getTags)
      .groups(metricConfig.getGroups)
      .experimentMetadata(Map("stats" -> experimentStats.toMap.asJava.asInstanceOf[Object]).asJava)
      .controlMetadata(Map("stats" -> controlStats.toMap.asJava.asInstanceOf[Object]).asJava)

    try {
      val metricClassification = mannWhitney.classify(transformedControl, transformedExperiment, directionality, nanStrategy, isCriticalMetric)
      resultBuilder
        .classification(metricClassification.classification.toString)
        .classificationReason(metricClassification.reason.orNull)
        .critical(metricClassification.critical)
        .resultMetadata(Map("ratio" -> metricClassification.deviation.asInstanceOf[Object]).asJava)
        .build()

    } catch {
      case e: RuntimeException =>
        logger.error("Metric Classification Failed", e)
        resultBuilder
          .classification(Error.toString)
          .classificationReason("Metric Classification Failed")
          .build()
    }
  }

}
