/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Accuracy;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression.MeanSquaredError;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression.RSquared;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression.Regression;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification.AucRoc;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification.BinarySoftClassification;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification.ConfusionMatrix;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification.Precision;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification.Recall;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification.ScoreByThresholdResult;

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
        return evaluationName + "." +  metricName;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return Arrays.asList(
            // Evaluations
            new NamedXContentRegistry.Entry(Evaluation.class, BinarySoftClassification.NAME, BinarySoftClassification::fromXContent),
            new NamedXContentRegistry.Entry(Evaluation.class, Classification.NAME, Classification::fromXContent),
            new NamedXContentRegistry.Entry(Evaluation.class, Regression.NAME, Regression::fromXContent),

            // Soft classification metrics
            new NamedXContentRegistry.Entry(EvaluationMetric.class,
                new ParseField(registeredMetricName(BinarySoftClassification.NAME, AucRoc.NAME)),
                AucRoc::fromXContent),
            new NamedXContentRegistry.Entry(EvaluationMetric.class,
                new ParseField(registeredMetricName(BinarySoftClassification.NAME, Precision.NAME)),
                Precision::fromXContent),
            new NamedXContentRegistry.Entry(EvaluationMetric.class,
                new ParseField(registeredMetricName(BinarySoftClassification.NAME, Recall.NAME)),
                Recall::fromXContent),
            new NamedXContentRegistry.Entry(EvaluationMetric.class,
                new ParseField(registeredMetricName(BinarySoftClassification.NAME, ConfusionMatrix.NAME)),
                ConfusionMatrix::fromXContent),

            // Classification metrics
            new NamedXContentRegistry.Entry(EvaluationMetric.class,
                new ParseField(registeredMetricName(Classification.NAME, MulticlassConfusionMatrix.NAME)),
                MulticlassConfusionMatrix::fromXContent),
            new NamedXContentRegistry.Entry(EvaluationMetric.class,
                new ParseField(registeredMetricName(Classification.NAME, Accuracy.NAME)),
                Accuracy::fromXContent),
            new NamedXContentRegistry.Entry(EvaluationMetric.class,
                new ParseField(
                    registeredMetricName(
                        Classification.NAME, org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Precision.NAME)),
                org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Precision::fromXContent),
            new NamedXContentRegistry.Entry(EvaluationMetric.class,
                new ParseField(
                    registeredMetricName(
                        Classification.NAME, org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Recall.NAME)),
                org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Recall::fromXContent),

            // Regression metrics
            new NamedXContentRegistry.Entry(EvaluationMetric.class,
                new ParseField(registeredMetricName(Regression.NAME, MeanSquaredError.NAME)),
                MeanSquaredError::fromXContent),
            new NamedXContentRegistry.Entry(EvaluationMetric.class,
                new ParseField(registeredMetricName(Regression.NAME, RSquared.NAME)),
                RSquared::fromXContent)
        );
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
            // Evaluations
            new NamedWriteableRegistry.Entry(Evaluation.class,
                BinarySoftClassification.NAME.getPreferredName(),
                BinarySoftClassification::new),
            new NamedWriteableRegistry.Entry(Evaluation.class,
                Classification.NAME.getPreferredName(),
                Classification::new),
            new NamedWriteableRegistry.Entry(Evaluation.class,
                Regression.NAME.getPreferredName(),
                Regression::new),

            // Evaluation metrics
            new NamedWriteableRegistry.Entry(EvaluationMetric.class,
                registeredMetricName(BinarySoftClassification.NAME, AucRoc.NAME),
                AucRoc::new),
            new NamedWriteableRegistry.Entry(EvaluationMetric.class,
                registeredMetricName(BinarySoftClassification.NAME, Precision.NAME),
                Precision::new),
            new NamedWriteableRegistry.Entry(EvaluationMetric.class,
                registeredMetricName(BinarySoftClassification.NAME, Recall.NAME),
                Recall::new),
            new NamedWriteableRegistry.Entry(EvaluationMetric.class,
                registeredMetricName(BinarySoftClassification.NAME, ConfusionMatrix.NAME),
                ConfusionMatrix::new),
            new NamedWriteableRegistry.Entry(EvaluationMetric.class,
                registeredMetricName(Classification.NAME, MulticlassConfusionMatrix.NAME),
                MulticlassConfusionMatrix::new),
            new NamedWriteableRegistry.Entry(EvaluationMetric.class,
                registeredMetricName(Classification.NAME, Accuracy.NAME),
                Accuracy::new),
            new NamedWriteableRegistry.Entry(EvaluationMetric.class,
                registeredMetricName(
                    Classification.NAME, org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Precision.NAME),
                org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Precision::new),
            new NamedWriteableRegistry.Entry(EvaluationMetric.class,
                registeredMetricName(
                    Classification.NAME, org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Recall.NAME),
                org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Recall::new),
            new NamedWriteableRegistry.Entry(EvaluationMetric.class,
                registeredMetricName(Regression.NAME, MeanSquaredError.NAME),
                MeanSquaredError::new),
            new NamedWriteableRegistry.Entry(EvaluationMetric.class,
                registeredMetricName(Regression.NAME, RSquared.NAME),
                RSquared::new),

            // Evaluation metrics results
            new NamedWriteableRegistry.Entry(EvaluationMetricResult.class,
                registeredMetricName(BinarySoftClassification.NAME, AucRoc.NAME),
                AucRoc.Result::new),
            new NamedWriteableRegistry.Entry(EvaluationMetricResult.class,
                registeredMetricName(BinarySoftClassification.NAME, ScoreByThresholdResult.NAME),
                ScoreByThresholdResult::new),
            new NamedWriteableRegistry.Entry(EvaluationMetricResult.class,
                registeredMetricName(BinarySoftClassification.NAME, ConfusionMatrix.NAME),
                ConfusionMatrix.Result::new),
            new NamedWriteableRegistry.Entry(EvaluationMetricResult.class,
                registeredMetricName(Classification.NAME, MulticlassConfusionMatrix.NAME),
                MulticlassConfusionMatrix.Result::new),
            new NamedWriteableRegistry.Entry(EvaluationMetricResult.class,
                registeredMetricName(Classification.NAME, Accuracy.NAME),
                Accuracy.Result::new),
            new NamedWriteableRegistry.Entry(EvaluationMetricResult.class,
                registeredMetricName(
                    Classification.NAME, org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Precision.NAME),
                org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Precision.Result::new),
            new NamedWriteableRegistry.Entry(EvaluationMetricResult.class,
                registeredMetricName(
                    Classification.NAME, org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Recall.NAME),
                org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Recall.Result::new),
            new NamedWriteableRegistry.Entry(EvaluationMetricResult.class,
                registeredMetricName(Regression.NAME, MeanSquaredError.NAME),
                MeanSquaredError.Result::new),
            new NamedWriteableRegistry.Entry(EvaluationMetricResult.class,
                registeredMetricName(Regression.NAME, RSquared.NAME),
                RSquared.Result::new)
        );
    }
}
