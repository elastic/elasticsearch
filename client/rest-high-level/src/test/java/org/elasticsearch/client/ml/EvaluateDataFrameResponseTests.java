/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.client.ml.dataframe.evaluation.common.AucRocResultTests;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.OutlierDetection;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.ConfusionMatrixMetricResultTests;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.PrecisionMetricResultTests;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.RecallMetricResultTests;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

public class EvaluateDataFrameResponseTests extends AbstractXContentTestCase<EvaluateDataFrameResponse> {

    public static EvaluateDataFrameResponse randomResponse() {
        String evaluationName = randomFrom(OutlierDetection.NAME, Classification.NAME, Regression.NAME);
        List<EvaluationMetric.Result> metrics;
        switch (evaluationName) {
            case OutlierDetection.NAME:
                metrics = randomSubsetOf(
                    Arrays.asList(
                        AucRocResultTests.randomResult(),
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
                        AucRocResultTests.randomResult(),
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
