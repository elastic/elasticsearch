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

import org.elasticsearch.client.ml.dataframe.evaluation.regression.MeanSquaredErrorMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.RSquaredMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.Regression;
import org.elasticsearch.client.ml.dataframe.evaluation.softclassification.BinarySoftClassification;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.client.ml.dataframe.evaluation.softclassification.AucRocMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.softclassification.ConfusionMatrixMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.softclassification.PrecisionMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.softclassification.RecallMetric;

import java.util.Arrays;
import java.util.List;

public class MlEvaluationNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return Arrays.asList(
            // Evaluations
            new NamedXContentRegistry.Entry(
                Evaluation.class, new ParseField(BinarySoftClassification.NAME), BinarySoftClassification::fromXContent),
            new NamedXContentRegistry.Entry(Evaluation.class, new ParseField(Regression.NAME), Regression::fromXContent),
            // Evaluation metrics
            new NamedXContentRegistry.Entry(EvaluationMetric.class, new ParseField(AucRocMetric.NAME), AucRocMetric::fromXContent),
            new NamedXContentRegistry.Entry(EvaluationMetric.class, new ParseField(PrecisionMetric.NAME), PrecisionMetric::fromXContent),
            new NamedXContentRegistry.Entry(EvaluationMetric.class, new ParseField(RecallMetric.NAME), RecallMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class, new ParseField(ConfusionMatrixMetric.NAME), ConfusionMatrixMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class, new ParseField(MeanSquaredErrorMetric.NAME), MeanSquaredErrorMetric::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.class, new ParseField(RSquaredMetric.NAME), RSquaredMetric::fromXContent),
            // Evaluation metrics results
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class, new ParseField(AucRocMetric.NAME), AucRocMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class, new ParseField(PrecisionMetric.NAME), PrecisionMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class, new ParseField(RecallMetric.NAME), RecallMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class, new ParseField(RSquaredMetric.NAME), RSquaredMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class, new ParseField(MeanSquaredErrorMetric.NAME), MeanSquaredErrorMetric.Result::fromXContent),
            new NamedXContentRegistry.Entry(
                EvaluationMetric.Result.class, new ParseField(ConfusionMatrixMetric.NAME), ConfusionMatrixMetric.Result::fromXContent));
    }
}
