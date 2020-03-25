/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.AccuracyMetricResultTests;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.Classification;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.MulticlassConfusionMatrixMetricResultTests;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.MeanSquaredErrorMetricResultTests;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.RSquaredMetricResultTests;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.Regression;
import org.elasticsearch.client.ml.dataframe.evaluation.softclassification.AucRocMetricResultTests;
import org.elasticsearch.client.ml.dataframe.evaluation.softclassification.BinarySoftClassification;
import org.elasticsearch.client.ml.dataframe.evaluation.softclassification.ConfusionMatrixMetricResultTests;
import org.elasticsearch.client.ml.dataframe.evaluation.softclassification.PrecisionMetricResultTests;
import org.elasticsearch.client.ml.dataframe.evaluation.softclassification.RecallMetricResultTests;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

public class EvaluateDataFrameResponseTests extends AbstractXContentTestCase<EvaluateDataFrameResponse> {

    public static EvaluateDataFrameResponse randomResponse() {
        String evaluationName = randomFrom(BinarySoftClassification.NAME, Classification.NAME, Regression.NAME);
        List<EvaluationMetric.Result> metrics;
        switch (evaluationName) {
            case BinarySoftClassification.NAME:
                metrics = randomSubsetOf(
                    Arrays.asList(
                        AucRocMetricResultTests.randomResult(),
                        PrecisionMetricResultTests.randomResult(),
                        RecallMetricResultTests.randomResult(),
                        ConfusionMatrixMetricResultTests.randomResult()));
                break;
            case Regression.NAME:
                metrics = randomSubsetOf(
                    Arrays.asList(
                        MeanSquaredErrorMetricResultTests.randomResult(),
                        RSquaredMetricResultTests.randomResult()));
                break;
            case Classification.NAME:
                metrics = randomSubsetOf(
                    Arrays.asList(
                        AccuracyMetricResultTests.randomResult(),
                        org.elasticsearch.client.ml.dataframe.evaluation.classification.PrecisionMetricResultTests.randomResult(),
                        org.elasticsearch.client.ml.dataframe.evaluation.classification.RecallMetricResultTests.randomResult(),
                        MulticlassConfusionMatrixMetricResultTests.randomResult()));
                break;
            default:
                throw new AssertionError("Please add missing \"case\" variant to the \"switch\" statement");
        }
        return new EvaluateDataFrameResponse(evaluationName, metrics);
    }

    @Override
    protected EvaluateDataFrameResponse createTestInstance() {
        return randomResponse();
    }

    @Override
    protected EvaluateDataFrameResponse doParseInstance(XContentParser parser) throws IOException {
        return EvaluateDataFrameResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow unknown fields in the metrics map (i.e. alongside named metrics like "precision" or "recall")
        return field -> field.isEmpty() || field.contains(".");
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlEvaluationNamedXContentProvider().getNamedXContentParsers());
    }
}
