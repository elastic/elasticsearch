/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.xpack.core.ml.action.EvaluateDataFrameAction;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix.ActualClass;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix.PredictedClass;
import org.junit.After;
import org.junit.Before;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ClassificationEvaluationIT extends MlNativeDataFrameAnalyticsIntegTestCase {

    private static final String ANIMALS_DATA_INDEX = "test-evaluate-animals-index";

    private static final String ACTUAL_CLASS_FIELD = "actual_class_field";
    private static final String PREDICTED_CLASS_FIELD = "predicted_class_field";

    @Before
    public void setup() {
        indexAnimalsData(ANIMALS_DATA_INDEX);
    }

    @After
    public void cleanup() {
        cleanUp();
    }

    public void testEvaluate_MulticlassClassification_DefaultMetrics() {
        EvaluateDataFrameAction.Request evaluateDataFrameRequest =
            new EvaluateDataFrameAction.Request()
                .setIndices(List.of(ANIMALS_DATA_INDEX))
                .setEvaluation(new Classification(ACTUAL_CLASS_FIELD, PREDICTED_CLASS_FIELD, null));

        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            client().execute(EvaluateDataFrameAction.INSTANCE, evaluateDataFrameRequest).actionGet();

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics().size(), equalTo(1));
        MulticlassConfusionMatrix.Result confusionMatrixResult =
            (MulticlassConfusionMatrix.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(confusionMatrixResult.getMetricName(), equalTo(MulticlassConfusionMatrix.NAME.getPreferredName()));
        assertThat(
            confusionMatrixResult.getConfusionMatrix(),
            equalTo(List.of(
                new ActualClass("ant",
                    15,
                    List.of(
                        new PredictedClass("ant", 1L),
                        new PredictedClass("cat", 4L),
                        new PredictedClass("dog", 3L),
                        new PredictedClass("fox", 2L),
                        new PredictedClass("mouse", 5L)),
                    0),
                new ActualClass("cat",
                    15,
                    List.of(
                        new PredictedClass("ant", 3L),
                        new PredictedClass("cat", 1L),
                        new PredictedClass("dog", 5L),
                        new PredictedClass("fox", 4L),
                        new PredictedClass("mouse", 2L)),
                    0),
                new ActualClass("dog",
                    15,
                    List.of(
                        new PredictedClass("ant", 4L),
                        new PredictedClass("cat", 2L),
                        new PredictedClass("dog", 1L),
                        new PredictedClass("fox", 5L),
                        new PredictedClass("mouse", 3L)),
                    0),
                new ActualClass("fox",
                    15,
                    List.of(
                        new PredictedClass("ant", 5L),
                        new PredictedClass("cat", 3L),
                        new PredictedClass("dog", 2L),
                        new PredictedClass("fox", 1L),
                        new PredictedClass("mouse", 4L)),
                    0),
                new ActualClass("mouse",
                    15,
                    List.of(
                        new PredictedClass("ant", 2L),
                        new PredictedClass("cat", 5L),
                        new PredictedClass("dog", 4L),
                        new PredictedClass("fox", 3L),
                        new PredictedClass("mouse", 1L)),
                    0))));
        assertThat(confusionMatrixResult.getOtherActualClassCount(), equalTo(0L));
    }

    public void testEvaluate_MulticlassClassification_ConfusionMatrixMetricWithDefaultSize() {
        EvaluateDataFrameAction.Request evaluateDataFrameRequest =
            new EvaluateDataFrameAction.Request()
                .setIndices(List.of(ANIMALS_DATA_INDEX))
                .setEvaluation(new Classification(ACTUAL_CLASS_FIELD, PREDICTED_CLASS_FIELD, List.of(new MulticlassConfusionMatrix())));

        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            client().execute(EvaluateDataFrameAction.INSTANCE, evaluateDataFrameRequest).actionGet();

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics().size(), equalTo(1));
        MulticlassConfusionMatrix.Result confusionMatrixResult =
            (MulticlassConfusionMatrix.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(confusionMatrixResult.getMetricName(), equalTo(MulticlassConfusionMatrix.NAME.getPreferredName()));
        assertThat(
            confusionMatrixResult.getConfusionMatrix(),
            equalTo(List.of(
                new ActualClass("ant",
                    15,
                    List.of(
                        new PredictedClass("ant", 1L),
                        new PredictedClass("cat", 4L),
                        new PredictedClass("dog", 3L),
                        new PredictedClass("fox", 2L),
                        new PredictedClass("mouse", 5L)),
                    0),
                new ActualClass("cat",
                    15,
                    List.of(
                        new PredictedClass("ant", 3L),
                        new PredictedClass("cat", 1L),
                        new PredictedClass("dog", 5L),
                        new PredictedClass("fox", 4L),
                        new PredictedClass("mouse", 2L)),
                    0),
                new ActualClass("dog",
                    15,
                    List.of(
                        new PredictedClass("ant", 4L),
                        new PredictedClass("cat", 2L),
                        new PredictedClass("dog", 1L),
                        new PredictedClass("fox", 5L),
                        new PredictedClass("mouse", 3L)),
                    0),
                new ActualClass("fox",
                    15,
                    List.of(
                        new PredictedClass("ant", 5L),
                        new PredictedClass("cat", 3L),
                        new PredictedClass("dog", 2L),
                        new PredictedClass("fox", 1L),
                        new PredictedClass("mouse", 4L)),
                    0),
                new ActualClass("mouse",
                    15,
                    List.of(
                        new PredictedClass("ant", 2L),
                        new PredictedClass("cat", 5L),
                        new PredictedClass("dog", 4L),
                        new PredictedClass("fox", 3L),
                        new PredictedClass("mouse", 1L)),
                    0))));
        assertThat(confusionMatrixResult.getOtherActualClassCount(), equalTo(0L));
    }

    public void testEvaluate_MulticlassClassification_ConfusionMatrixMetricWithUserProvidedSize() {
        EvaluateDataFrameAction.Request evaluateDataFrameRequest =
            new EvaluateDataFrameAction.Request()
                .setIndices(List.of(ANIMALS_DATA_INDEX))
                .setEvaluation(new Classification(ACTUAL_CLASS_FIELD, PREDICTED_CLASS_FIELD, List.of(new MulticlassConfusionMatrix(3))));

        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            client().execute(EvaluateDataFrameAction.INSTANCE, evaluateDataFrameRequest).actionGet();

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics().size(), equalTo(1));
        MulticlassConfusionMatrix.Result confusionMatrixResult =
            (MulticlassConfusionMatrix.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(confusionMatrixResult.getMetricName(), equalTo(MulticlassConfusionMatrix.NAME.getPreferredName()));
        assertThat(
            confusionMatrixResult.getConfusionMatrix(),
            equalTo(List.of(
                new ActualClass("ant",
                    15,
                    List.of(new PredictedClass("ant", 1L), new PredictedClass("cat", 4L), new PredictedClass("dog", 3L)),
                    7),
                new ActualClass("cat",
                    15,
                    List.of(new PredictedClass("ant", 3L), new PredictedClass("cat", 1L), new PredictedClass("dog", 5L)),
                    6),
                new ActualClass("dog",
                    15,
                    List.of(new PredictedClass("ant", 4L), new PredictedClass("cat", 2L), new PredictedClass("dog", 1L)),
                    8))));
        assertThat(confusionMatrixResult.getOtherActualClassCount(), equalTo(2L));
    }

    private static void indexAnimalsData(String indexName) {
        client().admin().indices().prepareCreate(indexName)
            .addMapping("_doc", ACTUAL_CLASS_FIELD, "type=keyword", PREDICTED_CLASS_FIELD, "type=keyword")
            .get();

        List<String> classNames = List.of("dog", "cat", "mouse", "ant", "fox");
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < classNames.size(); i++) {
            for (int j = 0; j < classNames.size(); j++) {
                for (int k = 0; k < j + 1; k++) {
                    bulkRequestBuilder.add(
                        new IndexRequest(indexName)
                            .source(
                                ACTUAL_CLASS_FIELD, classNames.get(i),
                                PREDICTED_CLASS_FIELD, classNames.get((i + j) % classNames.size())));
                }
            }
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }
    }
}
