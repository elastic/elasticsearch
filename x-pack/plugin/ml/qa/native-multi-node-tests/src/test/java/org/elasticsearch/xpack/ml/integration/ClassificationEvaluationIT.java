/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.xpack.core.ml.action.EvaluateDataFrameAction;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Accuracy;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Precision;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Recall;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix;
import org.junit.After;
import org.junit.Before;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ClassificationEvaluationIT extends MlNativeDataFrameAnalyticsIntegTestCase {

    private static final String ANIMALS_DATA_INDEX = "test-evaluate-animals-index";

    private static final String ANIMAL_NAME_FIELD = "animal_name";
    private static final String ANIMAL_NAME_PREDICTION_FIELD = "animal_name_prediction";
    private static final String NO_LEGS_FIELD = "no_legs";
    private static final String NO_LEGS_PREDICTION_FIELD = "no_legs_prediction";
    private static final String IS_PREDATOR_FIELD = "predator";
    private static final String IS_PREDATOR_PREDICTION_FIELD = "predator_prediction";

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
            evaluateDataFrame(ANIMALS_DATA_INDEX, new Classification(ANIMAL_NAME_FIELD, ANIMAL_NAME_PREDICTION_FIELD, null));

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(
            evaluateDataFrameResponse.getMetrics().stream().map(EvaluationMetricResult::getMetricName).collect(toList()),
            contains(MulticlassConfusionMatrix.NAME.getPreferredName()));
    }

    public void testEvaluate_AllMetrics() {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(
                ANIMALS_DATA_INDEX,
                new Classification(
                    ANIMAL_NAME_FIELD,
                    ANIMAL_NAME_PREDICTION_FIELD,
                    List.of(new Accuracy(), new MulticlassConfusionMatrix(), new Precision(), new Recall())));

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(
            evaluateDataFrameResponse.getMetrics().stream().map(EvaluationMetricResult::getMetricName).collect(toList()),
            contains(
                Accuracy.NAME.getPreferredName(),
                MulticlassConfusionMatrix.NAME.getPreferredName(),
                Precision.NAME.getPreferredName(),
                Recall.NAME.getPreferredName()));
    }

    public void testEvaluate_Accuracy_KeywordField() {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(
                ANIMALS_DATA_INDEX, new Classification(ANIMAL_NAME_FIELD, ANIMAL_NAME_PREDICTION_FIELD, List.of(new Accuracy())));

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics(), hasSize(1));

        Accuracy.Result accuracyResult = (Accuracy.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(accuracyResult.getMetricName(), equalTo(Accuracy.NAME.getPreferredName()));
        assertThat(
            accuracyResult.getClasses(),
            equalTo(
                List.of(
                    new Accuracy.PerClassResult("ant", 47.0 / 75),
                    new Accuracy.PerClassResult("cat", 47.0 / 75),
                    new Accuracy.PerClassResult("dog", 47.0 / 75),
                    new Accuracy.PerClassResult("fox", 47.0 / 75),
                    new Accuracy.PerClassResult("mouse", 47.0 / 75))));
        assertThat(accuracyResult.getOverallAccuracy(), equalTo(5.0 / 75));
    }

    public void testEvaluate_Accuracy_IntegerField() {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(
                ANIMALS_DATA_INDEX, new Classification(NO_LEGS_FIELD, NO_LEGS_PREDICTION_FIELD, List.of(new Accuracy())));

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics(), hasSize(1));

        Accuracy.Result accuracyResult = (Accuracy.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(accuracyResult.getMetricName(), equalTo(Accuracy.NAME.getPreferredName()));
        assertThat(
            accuracyResult.getClasses(),
            equalTo(
                List.of(
                    new Accuracy.PerClassResult("1", 57.0 / 75),
                    new Accuracy.PerClassResult("2", 54.0 / 75),
                    new Accuracy.PerClassResult("3", 51.0 / 75),
                    new Accuracy.PerClassResult("4", 48.0 / 75),
                    new Accuracy.PerClassResult("5", 45.0 / 75))));
        assertThat(accuracyResult.getOverallAccuracy(), equalTo(15.0 / 75));
    }

    public void testEvaluate_Accuracy_BooleanField() {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(
                ANIMALS_DATA_INDEX, new Classification(IS_PREDATOR_FIELD, IS_PREDATOR_PREDICTION_FIELD, List.of(new Accuracy())));

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics(), hasSize(1));

        Accuracy.Result accuracyResult = (Accuracy.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(accuracyResult.getMetricName(), equalTo(Accuracy.NAME.getPreferredName()));
        assertThat(
            accuracyResult.getClasses(),
            equalTo(
                List.of(
                    new Accuracy.PerClassResult("false", 18.0 / 30),
                    new Accuracy.PerClassResult("true", 27.0 / 45))));
        assertThat(accuracyResult.getOverallAccuracy(), equalTo(45.0 / 75));
    }

    public void testEvaluate_Precision() {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(
                ANIMALS_DATA_INDEX, new Classification(ANIMAL_NAME_FIELD, ANIMAL_NAME_PREDICTION_FIELD, List.of(new Precision())));

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics(), hasSize(1));

        Precision.Result precisionResult = (Precision.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(precisionResult.getMetricName(), equalTo(Precision.NAME.getPreferredName()));
        assertThat(
            precisionResult.getClasses(),
            equalTo(
                List.of(
                    new Precision.PerClassResult("ant", 1.0 / 15),
                    new Precision.PerClassResult("cat", 1.0 / 15),
                    new Precision.PerClassResult("dog", 1.0 / 15),
                    new Precision.PerClassResult("fox", 1.0 / 15),
                    new Precision.PerClassResult("mouse", 1.0 / 15))));
        assertThat(precisionResult.getAvgPrecision(), equalTo(5.0 / 75));
    }

    public void testEvaluate_Precision_CardinalityTooHigh() {
        indexDistinctAnimals(ANIMALS_DATA_INDEX, 1001);
        ElasticsearchStatusException e =
            expectThrows(
                ElasticsearchStatusException.class,
                () -> evaluateDataFrame(
                    ANIMALS_DATA_INDEX, new Classification(ANIMAL_NAME_FIELD, ANIMAL_NAME_PREDICTION_FIELD, List.of(new Precision()))));
        assertThat(e.getMessage(), containsString("Cardinality of field [animal_name] is too high"));
    }

    public void testEvaluate_Recall() {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(
                ANIMALS_DATA_INDEX, new Classification(ANIMAL_NAME_FIELD, ANIMAL_NAME_PREDICTION_FIELD, List.of(new Recall())));

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics(), hasSize(1));

        Recall.Result recallResult = (Recall.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(recallResult.getMetricName(), equalTo(Recall.NAME.getPreferredName()));
        assertThat(
            recallResult.getClasses(),
            equalTo(
                List.of(
                    new Recall.PerClassResult("ant", 1.0 / 15),
                    new Recall.PerClassResult("cat", 1.0 / 15),
                    new Recall.PerClassResult("dog", 1.0 / 15),
                    new Recall.PerClassResult("fox", 1.0 / 15),
                    new Recall.PerClassResult("mouse", 1.0 / 15))));
        assertThat(recallResult.getAvgRecall(), equalTo(5.0 / 75));
    }

    public void testEvaluate_Recall_CardinalityTooHigh() {
        indexDistinctAnimals(ANIMALS_DATA_INDEX, 1001);
        ElasticsearchStatusException e =
            expectThrows(
                ElasticsearchStatusException.class,
                () -> evaluateDataFrame(
                    ANIMALS_DATA_INDEX, new Classification(ANIMAL_NAME_FIELD, ANIMAL_NAME_PREDICTION_FIELD, List.of(new Recall()))));
        assertThat(e.getMessage(), containsString("Cardinality of field [animal_name] is too high"));
    }

    public void testEvaluate_ConfusionMatrixMetricWithDefaultSize() {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(
                ANIMALS_DATA_INDEX,
                new Classification(ANIMAL_NAME_FIELD, ANIMAL_NAME_PREDICTION_FIELD, List.of(new MulticlassConfusionMatrix())));

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics(), hasSize(1));

        MulticlassConfusionMatrix.Result confusionMatrixResult =
            (MulticlassConfusionMatrix.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(confusionMatrixResult.getMetricName(), equalTo(MulticlassConfusionMatrix.NAME.getPreferredName()));
        assertThat(
            confusionMatrixResult.getConfusionMatrix(),
            equalTo(List.of(
                new MulticlassConfusionMatrix.ActualClass("ant",
                    15,
                    List.of(
                        new MulticlassConfusionMatrix.PredictedClass("ant", 1L),
                        new MulticlassConfusionMatrix.PredictedClass("cat", 4L),
                        new MulticlassConfusionMatrix.PredictedClass("dog", 3L),
                        new MulticlassConfusionMatrix.PredictedClass("fox", 2L),
                        new MulticlassConfusionMatrix.PredictedClass("mouse", 5L)),
                    0),
                new MulticlassConfusionMatrix.ActualClass("cat",
                    15,
                    List.of(
                        new MulticlassConfusionMatrix.PredictedClass("ant", 3L),
                        new MulticlassConfusionMatrix.PredictedClass("cat", 1L),
                        new MulticlassConfusionMatrix.PredictedClass("dog", 5L),
                        new MulticlassConfusionMatrix.PredictedClass("fox", 4L),
                        new MulticlassConfusionMatrix.PredictedClass("mouse", 2L)),
                    0),
                new MulticlassConfusionMatrix.ActualClass("dog",
                    15,
                    List.of(
                        new MulticlassConfusionMatrix.PredictedClass("ant", 4L),
                        new MulticlassConfusionMatrix.PredictedClass("cat", 2L),
                        new MulticlassConfusionMatrix.PredictedClass("dog", 1L),
                        new MulticlassConfusionMatrix.PredictedClass("fox", 5L),
                        new MulticlassConfusionMatrix.PredictedClass("mouse", 3L)),
                    0),
                new MulticlassConfusionMatrix.ActualClass("fox",
                    15,
                    List.of(
                        new MulticlassConfusionMatrix.PredictedClass("ant", 5L),
                        new MulticlassConfusionMatrix.PredictedClass("cat", 3L),
                        new MulticlassConfusionMatrix.PredictedClass("dog", 2L),
                        new MulticlassConfusionMatrix.PredictedClass("fox", 1L),
                        new MulticlassConfusionMatrix.PredictedClass("mouse", 4L)),
                    0),
                new MulticlassConfusionMatrix.ActualClass("mouse",
                    15,
                    List.of(
                        new MulticlassConfusionMatrix.PredictedClass("ant", 2L),
                        new MulticlassConfusionMatrix.PredictedClass("cat", 5L),
                        new MulticlassConfusionMatrix.PredictedClass("dog", 4L),
                        new MulticlassConfusionMatrix.PredictedClass("fox", 3L),
                        new MulticlassConfusionMatrix.PredictedClass("mouse", 1L)),
                    0))));
        assertThat(confusionMatrixResult.getOtherActualClassCount(), equalTo(0L));
    }

    public void testEvaluate_ConfusionMatrixMetricWithUserProvidedSize() {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(
                ANIMALS_DATA_INDEX,
                new Classification(ANIMAL_NAME_FIELD, ANIMAL_NAME_PREDICTION_FIELD, List.of(new MulticlassConfusionMatrix(3, null))));

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics(), hasSize(1));
        MulticlassConfusionMatrix.Result confusionMatrixResult =
            (MulticlassConfusionMatrix.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(confusionMatrixResult.getMetricName(), equalTo(MulticlassConfusionMatrix.NAME.getPreferredName()));
        assertThat(
            confusionMatrixResult.getConfusionMatrix(),
            equalTo(List.of(
                new MulticlassConfusionMatrix.ActualClass("ant",
                    15,
                    List.of(
                        new MulticlassConfusionMatrix.PredictedClass("ant", 1L),
                        new MulticlassConfusionMatrix.PredictedClass("cat", 4L),
                        new MulticlassConfusionMatrix.PredictedClass("dog", 3L)),
                    7),
                new MulticlassConfusionMatrix.ActualClass("cat",
                    15,
                    List.of(
                        new MulticlassConfusionMatrix.PredictedClass("ant", 3L),
                        new MulticlassConfusionMatrix.PredictedClass("cat", 1L),
                        new MulticlassConfusionMatrix.PredictedClass("dog", 5L)),
                    6),
                new MulticlassConfusionMatrix.ActualClass("dog",
                    15,
                    List.of(
                        new MulticlassConfusionMatrix.PredictedClass("ant", 4L),
                        new MulticlassConfusionMatrix.PredictedClass("cat", 2L),
                        new MulticlassConfusionMatrix.PredictedClass("dog", 1L)),
                    8))));
        assertThat(confusionMatrixResult.getOtherActualClassCount(), equalTo(2L));
    }

    private static void createAnimalsIndex(String indexName) {
        client().admin().indices().prepareCreate(indexName)
            .setMapping(
                ANIMAL_NAME_FIELD, "type=keyword",
                ANIMAL_NAME_PREDICTION_FIELD, "type=keyword",
                NO_LEGS_FIELD, "type=integer",
                NO_LEGS_PREDICTION_FIELD, "type=integer",
                IS_PREDATOR_FIELD, "type=boolean",
                IS_PREDATOR_PREDICTION_FIELD, "type=boolean")
            .get();
    }

    private static void indexAnimalsData(String indexName) {
        List<String> animalNames = List.of("dog", "cat", "mouse", "ant", "fox");
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < animalNames.size(); i++) {
            for (int j = 0; j < animalNames.size(); j++) {
                for (int k = 0; k < j + 1; k++) {
                    bulkRequestBuilder.add(
                        new IndexRequest(indexName)
                            .source(
                                ANIMAL_NAME_FIELD, animalNames.get(i),
                                ANIMAL_NAME_PREDICTION_FIELD, animalNames.get((i + j) % animalNames.size()),
                                NO_LEGS_FIELD, i + 1,
                                NO_LEGS_PREDICTION_FIELD, j + 1,
                                IS_PREDATOR_FIELD, i % 2 == 0,
                                IS_PREDATOR_PREDICTION_FIELD, (i + j) % 2 == 0));
                }
            }
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }
    }

    private static void indexDistinctAnimals(String indexName, int distinctAnimalCount) {
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < distinctAnimalCount; i++) {
            bulkRequestBuilder.add(
                new IndexRequest(indexName).source(ANIMAL_NAME_FIELD, "animal_" + i, ANIMAL_NAME_PREDICTION_FIELD, randomAlphaOfLength(5)));
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }
    }
}
