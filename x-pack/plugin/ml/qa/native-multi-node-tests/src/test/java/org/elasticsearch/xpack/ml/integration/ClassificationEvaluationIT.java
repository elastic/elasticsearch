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
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                .setIndices(Arrays.asList(ANIMALS_DATA_INDEX))
                .setEvaluation(new Classification(ACTUAL_CLASS_FIELD, PREDICTED_CLASS_FIELD, null));

        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            client().execute(EvaluateDataFrameAction.INSTANCE, evaluateDataFrameRequest).actionGet();

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics().size(), equalTo(1));
        MulticlassConfusionMatrix.Result confusionMatrixResult =
            (MulticlassConfusionMatrix.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(confusionMatrixResult.getMetricName(), equalTo(MulticlassConfusionMatrix.NAME.getPreferredName()));
        Map<String, Map<String, Long>> expectedConfusionMatrix = new HashMap<>();
        expectedConfusionMatrix.put("ant", new HashMap<>());
        expectedConfusionMatrix.get("ant").put("ant", 1L);
        expectedConfusionMatrix.get("ant").put("cat", 4L);
        expectedConfusionMatrix.get("ant").put("dog", 3L);
        expectedConfusionMatrix.get("ant").put("fox", 2L);
        expectedConfusionMatrix.get("ant").put("mouse", 5L);
        expectedConfusionMatrix.put("cat", new HashMap<>());
        expectedConfusionMatrix.get("cat").put("ant", 3L);
        expectedConfusionMatrix.get("cat").put("cat", 1L);
        expectedConfusionMatrix.get("cat").put("dog", 5L);
        expectedConfusionMatrix.get("cat").put("fox", 4L);
        expectedConfusionMatrix.get("cat").put("mouse", 2L);
        expectedConfusionMatrix.put("dog", new HashMap<>());
        expectedConfusionMatrix.get("dog").put("ant", 4L);
        expectedConfusionMatrix.get("dog").put("cat", 2L);
        expectedConfusionMatrix.get("dog").put("dog", 1L);
        expectedConfusionMatrix.get("dog").put("fox", 5L);
        expectedConfusionMatrix.get("dog").put("mouse", 3L);
        expectedConfusionMatrix.put("fox", new HashMap<>());
        expectedConfusionMatrix.get("fox").put("ant", 5L);
        expectedConfusionMatrix.get("fox").put("cat", 3L);
        expectedConfusionMatrix.get("fox").put("dog", 2L);
        expectedConfusionMatrix.get("fox").put("fox", 1L);
        expectedConfusionMatrix.get("fox").put("mouse", 4L);
        expectedConfusionMatrix.put("mouse", new HashMap<>());
        expectedConfusionMatrix.get("mouse").put("ant", 2L);
        expectedConfusionMatrix.get("mouse").put("cat", 5L);
        expectedConfusionMatrix.get("mouse").put("dog", 4L);
        expectedConfusionMatrix.get("mouse").put("fox", 3L);
        expectedConfusionMatrix.get("mouse").put("mouse", 1L);
        assertThat(confusionMatrixResult.getConfusionMatrix(), equalTo(expectedConfusionMatrix));
        assertThat(confusionMatrixResult.getOtherClassesCount(), equalTo(0L));
    }

    public void testEvaluate_MulticlassClassification_ConfusionMatrixMetricWithDefaultSize() {
        EvaluateDataFrameAction.Request evaluateDataFrameRequest =
            new EvaluateDataFrameAction.Request()
                .setIndices(Arrays.asList(ANIMALS_DATA_INDEX))
                .setEvaluation(
                    new Classification(ACTUAL_CLASS_FIELD, PREDICTED_CLASS_FIELD, Arrays.asList(new MulticlassConfusionMatrix())));

        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            client().execute(EvaluateDataFrameAction.INSTANCE, evaluateDataFrameRequest).actionGet();

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics().size(), equalTo(1));
        MulticlassConfusionMatrix.Result confusionMatrixResult =
            (MulticlassConfusionMatrix.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(confusionMatrixResult.getMetricName(), equalTo(MulticlassConfusionMatrix.NAME.getPreferredName()));
        Map<String, Map<String, Long>> expectedConfusionMatrix = new HashMap<>();
        expectedConfusionMatrix.put("ant", new HashMap<>());
        expectedConfusionMatrix.get("ant").put("ant", 1L);
        expectedConfusionMatrix.get("ant").put("cat", 4L);
        expectedConfusionMatrix.get("ant").put("dog", 3L);
        expectedConfusionMatrix.get("ant").put("fox", 2L);
        expectedConfusionMatrix.get("ant").put("mouse", 5L);
        expectedConfusionMatrix.put("cat", new HashMap<>());
        expectedConfusionMatrix.get("cat").put("ant", 3L);
        expectedConfusionMatrix.get("cat").put("cat", 1L);
        expectedConfusionMatrix.get("cat").put("dog", 5L);
        expectedConfusionMatrix.get("cat").put("fox", 4L);
        expectedConfusionMatrix.get("cat").put("mouse", 2L);
        expectedConfusionMatrix.put("dog", new HashMap<>());
        expectedConfusionMatrix.get("dog").put("ant", 4L);
        expectedConfusionMatrix.get("dog").put("cat", 2L);
        expectedConfusionMatrix.get("dog").put("dog", 1L);
        expectedConfusionMatrix.get("dog").put("fox", 5L);
        expectedConfusionMatrix.get("dog").put("mouse", 3L);
        expectedConfusionMatrix.put("fox", new HashMap<>());
        expectedConfusionMatrix.get("fox").put("ant", 5L);
        expectedConfusionMatrix.get("fox").put("cat", 3L);
        expectedConfusionMatrix.get("fox").put("dog", 2L);
        expectedConfusionMatrix.get("fox").put("fox", 1L);
        expectedConfusionMatrix.get("fox").put("mouse", 4L);
        expectedConfusionMatrix.put("mouse", new HashMap<>());
        expectedConfusionMatrix.get("mouse").put("ant", 2L);
        expectedConfusionMatrix.get("mouse").put("cat", 5L);
        expectedConfusionMatrix.get("mouse").put("dog", 4L);
        expectedConfusionMatrix.get("mouse").put("fox", 3L);
        expectedConfusionMatrix.get("mouse").put("mouse", 1L);
        assertThat(confusionMatrixResult.getConfusionMatrix(), equalTo(expectedConfusionMatrix));
        assertThat(confusionMatrixResult.getOtherClassesCount(), equalTo(0L));
    }

    public void testEvaluate_MulticlassClassification_ConfusionMatrixMetricWithUserProvidedSize() {
        EvaluateDataFrameAction.Request evaluateDataFrameRequest =
            new EvaluateDataFrameAction.Request()
                .setIndices(Arrays.asList(ANIMALS_DATA_INDEX))
                .setEvaluation(
                    new Classification(ACTUAL_CLASS_FIELD, PREDICTED_CLASS_FIELD, Arrays.asList(new MulticlassConfusionMatrix(3))));

        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            client().execute(EvaluateDataFrameAction.INSTANCE, evaluateDataFrameRequest).actionGet();

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics().size(), equalTo(1));
        MulticlassConfusionMatrix.Result confusionMatrixResult =
            (MulticlassConfusionMatrix.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(confusionMatrixResult.getMetricName(), equalTo(MulticlassConfusionMatrix.NAME.getPreferredName()));
        Map<String, Map<String, Long>> expectedConfusionMatrix = new HashMap<>();
        expectedConfusionMatrix.put("ant", new HashMap<>());
        expectedConfusionMatrix.get("ant").put("ant", 1L);
        expectedConfusionMatrix.get("ant").put("cat", 4L);
        expectedConfusionMatrix.get("ant").put("dog", 3L);
        expectedConfusionMatrix.get("ant").put("_other_", 7L);
        expectedConfusionMatrix.put("cat", new HashMap<>());
        expectedConfusionMatrix.get("cat").put("ant", 3L);
        expectedConfusionMatrix.get("cat").put("cat", 1L);
        expectedConfusionMatrix.get("cat").put("dog", 5L);
        expectedConfusionMatrix.get("cat").put("_other_", 6L);
        expectedConfusionMatrix.put("dog", new HashMap<>());
        expectedConfusionMatrix.get("dog").put("ant", 4L);
        expectedConfusionMatrix.get("dog").put("cat", 2L);
        expectedConfusionMatrix.get("dog").put("dog", 1L);
        expectedConfusionMatrix.get("dog").put("_other_", 8L);
        assertThat(confusionMatrixResult.getConfusionMatrix(), equalTo(expectedConfusionMatrix));
        assertThat(confusionMatrixResult.getOtherClassesCount(), equalTo(2L));
    }

    private static void indexAnimalsData(String indexName) {
        client().admin().indices().prepareCreate(indexName)
            .addMapping("_doc", ACTUAL_CLASS_FIELD, "type=keyword", PREDICTED_CLASS_FIELD, "type=keyword")
            .get();

        List<String> classNames = Arrays.asList("dog", "cat", "mouse", "ant", "fox");
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
