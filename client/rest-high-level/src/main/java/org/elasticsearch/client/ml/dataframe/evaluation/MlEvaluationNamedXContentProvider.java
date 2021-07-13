/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.evaluation;

import org.elasticsearch.client.ml.dataframe.evaluation.classification.AccuracyMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.AucRocMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.Classification;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.MulticlassConfusionMatrixMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.PrecisionMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.RecallMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.common.AucRocResult;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.ConfusionMatrixMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.OutlierDetection;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.HuberMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.MeanSquaredErrorMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.MeanSquaredLogarithmicErrorMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.RSquaredMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.Regression;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;

import java.util.Arrays;
import java.util.List;

public class MlEvaluationNamedXContentProvider implements NamedXContentProvider {

    /**
     * Constructs the name under which a metric (or metric result) is registered.
     * The name is prefixed with evaluation name so that registered names are unique.
     *
     * @param evaluationName name of the evaluation
     * @param metricName name of the metric
     * @return name appropriate for registering a metric (or metric result) in {@link NamedXContentRegistry}
     */
    public static String registeredMetricName(String evaluationName, String metricName) {
        return evaluationName + "." +  metricName;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return Arrays.asList(
            // Evaluations
            new NamedXContentRegistry.Entry(
                Evaluation.class, new ParseField(OutlierDetection.NAME), OutlierDetection::fromXContent),
            new NamedXContentRegistry.Entry(Evaluation.class, new ParseField(Classification.NAME), Classification::fromXContent),
            new NamedXContentRegistry.Entry(Evaluation.class, new ParseField(Regression.NAME), Regression::fromXContent),
            // Evaluation metrics
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(
                    registeredMetricName(
                        OutlierDetection.NAME, org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.AucRocMetric.NAME)),
                org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.AucRocMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(
                    registeredMetricName(
                        OutlierDetection.NAME, org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.PrecisionMetric.NAME)),
                org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.PrecisionMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(
                    registeredMetricName(
                        OutlierDetection.NAME, org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.RecallMetric.NAME)),
                org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.RecallMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(OutlierDetection.NAME, ConfusionMatrixMetric.NAME)),
                ConfusionMatrixMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Classification.NAME, AucRocMetric.NAME)),
                AucRocMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Classification.NAME, AccuracyMetric.NAME)),
                AccuracyMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Classification.NAME, PrecisionMetric.NAME)),
                PrecisionMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Classification.NAME, RecallMetric.NAME)),
                RecallMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Classification.NAME, MulticlassConfusionMatrixMetric.NAME)),
                MulticlassConfusionMatrixMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Regression.NAME, MeanSquaredErrorMetric.NAME)),
                MeanSquaredErrorMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Regression.NAME, MeanSquaredLogarithmicErrorMetric.NAME)),
                MeanSquaredLogarithmicErrorMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Regression.NAME, HuberMetric.NAME)),
                HuberMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Regression.NAME, RSquaredMetric.NAME)),
                RSquaredMetric::fromXContent),
            // Evaluation metrics results
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(registeredMetricName(
                    OutlierDetection.NAME, org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.AucRocMetric.NAME)),
                AucRocResult::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(
                    registeredMetricName(
                        OutlierDetection.NAME, org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.PrecisionMetric.NAME)),
                org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.PrecisionMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(
                    registeredMetricName(
                        OutlierDetection.NAME, org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.RecallMetric.NAME)),
                org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.RecallMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(registeredMetricName(OutlierDetection.NAME, ConfusionMatrixMetric.NAME)),
                ConfusionMatrixMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(registeredMetricName(Classification.NAME, AucRocMetric.NAME)),
                AucRocResult::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(registeredMetricName(Classification.NAME, AccuracyMetric.NAME)),
                AccuracyMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(registeredMetricName(Classification.NAME, PrecisionMetric.NAME)),
                PrecisionMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(registeredMetricName(Classification.NAME, RecallMetric.NAME)),
                RecallMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(registeredMetricName(Classification.NAME, MulticlassConfusionMatrixMetric.NAME)),
                MulticlassConfusionMatrixMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(registeredMetricName(Regression.NAME, MeanSquaredErrorMetric.NAME)),
                MeanSquaredErrorMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(registeredMetricName(Regression.NAME, MeanSquaredLogarithmicErrorMetric.NAME)),
                MeanSquaredLogarithmicErrorMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(registeredMetricName(Regression.NAME, HuberMetric.NAME)),
                HuberMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(registeredMetricName(Regression.NAME, RSquaredMetric.NAME)),
                RSquaredMetric.Result::fromXContent)
        );
    }
}
