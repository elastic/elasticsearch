/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Accuracy;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.AucRoc;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Precision;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Recall;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.common.AbstractAucRoc;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.ConfusionMatrix;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.OutlierDetection;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.ScoreByThresholdResult;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression.Huber;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression.MeanSquaredError;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression.MeanSquaredLogarithmicError;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression.RSquared;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression.Regression;

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
    public static String registeredMetricName(ParseField evaluationName, ParseField metricName) {
        return registeredMetricName(evaluationName.getPreferredName(), metricName.getPreferredName());
    }

    /**
     * Constructs the name under which a metric (or metric result) is registered.
     * The name is prefixed with evaluation name so that registered names are unique.
     *
     * @param evaluationName name of the evaluation
     * @param metricName name of the metric
     * @return name appropriate for registering a metric (or metric result) in {@link NamedXContentRegistry}
     */
    public static String registeredMetricName(String evaluationName, String metricName) {
        return evaluationName + "." + metricName;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return Arrays.asList(
            // Evaluations
            new NamedXContentRegistry.Entry(Evaluation.class, OutlierDetection.NAME, OutlierDetection::fromXContent),
            new NamedXContentRegistry.Entry(Evaluation.class, Classification.NAME, Classification::fromXContent),
            new NamedXContentRegistry.Entry(Evaluation.class, Regression.NAME, Regression::fromXContent),

            // Outlier detection metrics
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(
                    registeredMetricName(
                        OutlierDetection.NAME,
                        org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.AucRoc.NAME
                    )
                ),
                org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.AucRoc::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(
                    registeredMetricName(
                        OutlierDetection.NAME,
                        org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.Precision.NAME
                    )
                ),
                org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.Precision::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(
                    registeredMetricName(
                        OutlierDetection.NAME,
                        org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.Recall.NAME
                    )
                ),
                org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.Recall::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(OutlierDetection.NAME, ConfusionMatrix.NAME)),
                ConfusionMatrix::fromXContent
            ),

            // Classification metrics
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Classification.NAME, AucRoc.NAME)),
                AucRoc::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Classification.NAME, MulticlassConfusionMatrix.NAME)),
                MulticlassConfusionMatrix::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Classification.NAME, Accuracy.NAME)),
                Accuracy::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Classification.NAME, Precision.NAME)),
                Precision::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Classification.NAME, Recall.NAME)),
                Recall::fromXContent
            ),

            // Regression metrics
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Regression.NAME, MeanSquaredError.NAME)),
                MeanSquaredError::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Regression.NAME, MeanSquaredLogarithmicError.NAME)),
                MeanSquaredLogarithmicError::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Regression.NAME, Huber.NAME)),
                Huber::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Regression.NAME, RSquared.NAME)),
                RSquared::fromXContent
            )
        );
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
            // Evaluations
            new NamedWriteableRegistry.Entry(Evaluation.class, OutlierDetection.NAME.getPreferredName(), OutlierDetection::new),
            new NamedWriteableRegistry.Entry(Evaluation.class, Classification.NAME.getPreferredName(), Classification::new),
            new NamedWriteableRegistry.Entry(Evaluation.class, Regression.NAME.getPreferredName(), Regression::new),

            // Evaluation metrics
            new NamedWriteableRegistry.Entry(
                EvaluationMetric.class,
                registeredMetricName(
                    OutlierDetection.NAME,
                    org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.AucRoc.NAME
                ),
                org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.AucRoc::new
            ),
            new NamedWriteableRegistry.Entry(
                EvaluationMetric.class,
                registeredMetricName(
                    OutlierDetection.NAME,
                    org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.Precision.NAME
                ),
                org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.Precision::new
            ),
            new NamedWriteableRegistry.Entry(
                EvaluationMetric.class,
                registeredMetricName(
                    OutlierDetection.NAME,
                    org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.Recall.NAME
                ),
                org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.Recall::new
            ),
            new NamedWriteableRegistry.Entry(
                EvaluationMetric.class,
                registeredMetricName(OutlierDetection.NAME, ConfusionMatrix.NAME),
                ConfusionMatrix::new
            ),
            new NamedWriteableRegistry.Entry(EvaluationMetric.class, registeredMetricName(Classification.NAME, AucRoc.NAME), AucRoc::new),
            new NamedWriteableRegistry.Entry(
                EvaluationMetric.class,
                registeredMetricName(Classification.NAME, MulticlassConfusionMatrix.NAME),
                MulticlassConfusionMatrix::new
            ),
            new NamedWriteableRegistry.Entry(
                EvaluationMetric.class,
                registeredMetricName(Classification.NAME, Accuracy.NAME),
                Accuracy::new
            ),
            new NamedWriteableRegistry.Entry(
                EvaluationMetric.class,
                registeredMetricName(Classification.NAME, Precision.NAME),
                Precision::new
            ),
            new NamedWriteableRegistry.Entry(EvaluationMetric.class, registeredMetricName(Classification.NAME, Recall.NAME), Recall::new),
            new NamedWriteableRegistry.Entry(
                EvaluationMetric.class,
                registeredMetricName(Regression.NAME, MeanSquaredError.NAME),
                MeanSquaredError::new
            ),
            new NamedWriteableRegistry.Entry(
                EvaluationMetric.class,
                registeredMetricName(Regression.NAME, MeanSquaredLogarithmicError.NAME),
                MeanSquaredLogarithmicError::new
            ),
            new NamedWriteableRegistry.Entry(EvaluationMetric.class, registeredMetricName(Regression.NAME, Huber.NAME), Huber::new),
            new NamedWriteableRegistry.Entry(EvaluationMetric.class, registeredMetricName(Regression.NAME, RSquared.NAME), RSquared::new),

            // Evaluation metrics results
            new NamedWriteableRegistry.Entry(
                EvaluationMetricResult.class,
                registeredMetricName(OutlierDetection.NAME, ScoreByThresholdResult.NAME),
                ScoreByThresholdResult::new
            ),
            new NamedWriteableRegistry.Entry(
                EvaluationMetricResult.class,
                registeredMetricName(OutlierDetection.NAME, ConfusionMatrix.NAME),
                ConfusionMatrix.Result::new
            ),
            new NamedWriteableRegistry.Entry(EvaluationMetricResult.class, AbstractAucRoc.Result.NAME, AbstractAucRoc.Result::new),
            new NamedWriteableRegistry.Entry(
                EvaluationMetricResult.class,
                registeredMetricName(Classification.NAME, MulticlassConfusionMatrix.NAME),
                MulticlassConfusionMatrix.Result::new
            ),
            new NamedWriteableRegistry.Entry(
                EvaluationMetricResult.class,
                registeredMetricName(Classification.NAME, Accuracy.NAME),
                Accuracy.Result::new
            ),
            new NamedWriteableRegistry.Entry(
                EvaluationMetricResult.class,
                registeredMetricName(Classification.NAME, Precision.NAME),
                Precision.Result::new
            ),
            new NamedWriteableRegistry.Entry(
                EvaluationMetricResult.class,
                registeredMetricName(Classification.NAME, Recall.NAME),
                Recall.Result::new
            ),
            new NamedWriteableRegistry.Entry(
                EvaluationMetricResult.class,
                registeredMetricName(Regression.NAME, MeanSquaredError.NAME),
                MeanSquaredError.Result::new
            ),
            new NamedWriteableRegistry.Entry(
                EvaluationMetricResult.class,
                registeredMetricName(Regression.NAME, MeanSquaredLogarithmicError.NAME),
                MeanSquaredLogarithmicError.Result::new
            ),
            new NamedWriteableRegistry.Entry(
                EvaluationMetricResult.class,
                registeredMetricName(Regression.NAME, Huber.NAME),
                Huber.Result::new
            ),
            new NamedWriteableRegistry.Entry(
                EvaluationMetricResult.class,
                registeredMetricName(Regression.NAME, RSquared.NAME),
                RSquared.Result::new
            )
        );
    }
}
