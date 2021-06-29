/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.xpack.core.ml.action.EvaluateDataFrameAction;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.AucRoc;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.ConfusionMatrix;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.OutlierDetection;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.Precision;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection.Recall;
import org.junit.After;
import org.junit.Before;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.ml.integration.ClassificationEvaluationIT.ANIMALS_DATA_INDEX;
import static org.elasticsearch.xpack.ml.integration.ClassificationEvaluationIT.IS_PREDATOR_BOOLEAN_FIELD;
import static org.elasticsearch.xpack.ml.integration.ClassificationEvaluationIT.IS_PREDATOR_PREDICTION_PROBABILITY_FIELD;
import static org.elasticsearch.xpack.ml.integration.ClassificationEvaluationIT.createAnimalsIndex;
import static org.elasticsearch.xpack.ml.integration.ClassificationEvaluationIT.indexAnimalsData;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class OutlierDetectionEvaluationIT extends MlNativeDataFrameAnalyticsIntegTestCase {

    @Before
    public void setup() {
        createAnimalsIndex(ANIMALS_DATA_INDEX);
        indexAnimalsData(ANIMALS_DATA_INDEX);
    }

    @After
    public void cleanup() {
        cleanUp();
    }

    public void testEvaluate_DefaultMetrics() {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(
                ANIMALS_DATA_INDEX, new OutlierDetection(IS_PREDATOR_BOOLEAN_FIELD, IS_PREDATOR_PREDICTION_PROBABILITY_FIELD, null));

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(OutlierDetection.NAME.getPreferredName()));
        assertThat(
            evaluateDataFrameResponse.getMetrics().stream().map(EvaluationMetricResult::getMetricName).collect(toList()),
            containsInAnyOrder(
                AucRoc.NAME.getPreferredName(),
                Precision.NAME.getPreferredName(),
                Recall.NAME.getPreferredName(),
                ConfusionMatrix.NAME.getPreferredName()));
    }

    public void testEvaluate_AllMetrics() {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(
                ANIMALS_DATA_INDEX,
                new OutlierDetection(
                    IS_PREDATOR_BOOLEAN_FIELD,
                    IS_PREDATOR_PREDICTION_PROBABILITY_FIELD,
                    List.of(
                        new AucRoc(false),
                        new Precision(List.of(0.5)),
                        new Recall(List.of(0.5)),
                        new ConfusionMatrix(List.of(0.5)))));

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(OutlierDetection.NAME.getPreferredName()));
        assertThat(
            evaluateDataFrameResponse.getMetrics().stream().map(EvaluationMetricResult::getMetricName).collect(toList()),
            containsInAnyOrder(
                AucRoc.NAME.getPreferredName(),
                Precision.NAME.getPreferredName(),
                Recall.NAME.getPreferredName(),
                ConfusionMatrix.NAME.getPreferredName()));
    }

    private AucRoc.Result evaluateAucRoc(String actualField, String predictedField, boolean includeCurve) {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(
                ANIMALS_DATA_INDEX,
                new OutlierDetection(actualField, predictedField, List.of(new AucRoc(includeCurve))));

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(OutlierDetection.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics(), hasSize(1));

        AucRoc.Result aucrocResult = (AucRoc.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(aucrocResult.getMetricName(), equalTo(AucRoc.NAME.getPreferredName()));
        return aucrocResult;
    }

    public void testEvaluate_AucRoc_DoNotIncludeCurve() {
        AucRoc.Result aucrocResult = evaluateAucRoc(IS_PREDATOR_BOOLEAN_FIELD, IS_PREDATOR_PREDICTION_PROBABILITY_FIELD, false);
        assertThat(aucrocResult.getValue(), is(closeTo(0.98, 0.001)));
        assertThat(aucrocResult.getCurve(), hasSize(0));
    }

    public void testEvaluate_AucRoc_IncludeCurve() {
        AucRoc.Result aucrocResult = evaluateAucRoc(IS_PREDATOR_BOOLEAN_FIELD, IS_PREDATOR_PREDICTION_PROBABILITY_FIELD, true);
        assertThat(aucrocResult.getValue(), is(closeTo(0.98, 0.001)));
        assertThat(aucrocResult.getCurve(), hasSize(greaterThan(0)));
    }

    @Override
    boolean supportsInference() {
        return false;
    }
}
