/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.dataframe.evaluation;

import org.elasticsearch.client.ml.dataframe.evaluation.classification.AccuracyMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.Classification;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.MulticlassConfusionMatrixMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.MeanSquaredErrorMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.RSquaredMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.Regression;
import org.elasticsearch.client.ml.dataframe.evaluation.softclassification.AucRocMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.softclassification.BinarySoftClassification;
import org.elasticsearch.client.ml.dataframe.evaluation.softclassification.ConfusionMatrixMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.softclassification.PrecisionMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.softclassification.RecallMetric;
import org.elasticsearch.common.ParseField;
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
                Evaluation.class, new ParseField(BinarySoftClassification.NAME), BinarySoftClassification::fromXContent),
            new NamedXContentRegistry.Entry(Evaluation.class, new ParseField(Classification.NAME), Classification::fromXContent),
            new NamedXContentRegistry.Entry(Evaluation.class, new ParseField(Regression.NAME), Regression::fromXContent),
            // Evaluation metrics
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(BinarySoftClassification.NAME, AucRocMetric.NAME)),
                AucRocMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(BinarySoftClassification.NAME, PrecisionMetric.NAME)),
                PrecisionMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(BinarySoftClassification.NAME, RecallMetric.NAME)),
                RecallMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(BinarySoftClassification.NAME, ConfusionMatrixMetric.NAME)),
                ConfusionMatrixMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(Classification.NAME, AccuracyMetric.NAME)),
                AccuracyMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(
                    Classification.NAME, org.elasticsearch.client.ml.dataframe.evaluation.classification.PrecisionMetric.NAME)),
                org.elasticsearch.client.ml.dataframe.evaluation.classification.PrecisionMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class,
                new ParseField(registeredMetricName(
                    Classification.NAME, org.elasticsearch.client.ml.dataframe.evaluation.classification.RecallMetric.NAME)),
                org.elasticsearch.client.ml.dataframe.evaluation.classification.RecallMetric::fromXContent),
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
                new ParseField(registeredMetricName(Regression.NAME, RSquaredMetric.NAME)),
                RSquaredMetric::fromXContent),
            // Evaluation metrics results
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(registeredMetricName(BinarySoftClassification.NAME, AucRocMetric.NAME)),
                AucRocMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(registeredMetricName(BinarySoftClassification.NAME, PrecisionMetric.NAME)),
                PrecisionMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(registeredMetricName(BinarySoftClassification.NAME, RecallMetric.NAME)),
                RecallMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(registeredMetricName(BinarySoftClassification.NAME, ConfusionMatrixMetric.NAME)),
                ConfusionMatrixMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(registeredMetricName(Classification.NAME, AccuracyMetric.NAME)),
                AccuracyMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(registeredMetricName(
                    Classification.NAME, org.elasticsearch.client.ml.dataframe.evaluation.classification.PrecisionMetric.NAME)),
                org.elasticsearch.client.ml.dataframe.evaluation.classification.PrecisionMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class,
                new ParseField(registeredMetricName(
                    Classification.NAME, org.elasticsearch.client.ml.dataframe.evaluation.classification.RecallMetric.NAME)),
                org.elasticsearch.client.ml.dataframe.evaluation.classification.RecallMetric.Result::fromXContent),
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
                new ParseField(registeredMetricName(Regression.NAME, RSquaredMetric.NAME)),
                RSquaredMetric.Result::fromXContent)
        );
    }
}
