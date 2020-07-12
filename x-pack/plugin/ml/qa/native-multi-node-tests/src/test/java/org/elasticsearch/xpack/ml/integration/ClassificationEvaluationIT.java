/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService.TooManyBucketsException;
import org.elasticsearch.xpack.core.ml.action.EvaluateDataFrameAction;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Accuracy;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Precision;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Recall;
import org.junit.After;
import org.junit.Before;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notANumber;

public class ClassificationEvaluationIT extends MlNativeDataFrameAnalyticsIntegTestCase {

    private static final String ANIMALS_DATA_INDEX = "test-evaluate-animals-index";

    private static final String ANIMAL_NAME_KEYWORD_FIELD = "animal_name_keyword";
    private static final String ANIMAL_NAME_PREDICTION_KEYWORD_FIELD = "animal_name_keyword_prediction";
    private static final String NO_LEGS_KEYWORD_FIELD = "no_legs_keyword";
    private static final String NO_LEGS_INTEGER_FIELD = "no_legs_integer";
    private static final String NO_LEGS_PREDICTION_INTEGER_FIELD = "no_legs_integer_prediction";
    private static final String IS_PREDATOR_KEYWORD_FIELD = "predator_keyword";
    private static final String IS_PREDATOR_BOOLEAN_FIELD = "predator_boolean";
    private static final String IS_PREDATOR_PREDICTION_BOOLEAN_FIELD = "predator_boolean_prediction";

    @Before
    public void setup() {
        createAnimalsIndex(ANIMALS_DATA_INDEX);
        indexAnimalsData(ANIMALS_DATA_INDEX);
    }

    @After
    public void cleanup() {
        cleanUp();
        client().admin().cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull("search.max_buckets"))
            .get();
    }

    public void testEvaluate_DefaultMetrics() {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(
                ANIMALS_DATA_INDEX, new Classification(ANIMAL_NAME_KEYWORD_FIELD, ANIMAL_NAME_PREDICTION_KEYWORD_FIELD, null));

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
                    ANIMAL_NAME_KEYWORD_FIELD,
                    ANIMAL_NAME_PREDICTION_KEYWORD_FIELD,
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

    public void testEvaluate_AllMetrics_KeywordField_CaseSensitivity() {
        String indexName = "some-index";
        String actualField = "fieldA";
        String predictedField = "fieldB";
        client().admin().indices().prepareCreate(indexName)
            .setMapping(
                actualField, "type=keyword",
                predictedField, "type=keyword")
            .get();
        client().prepareIndex(indexName)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .setSource(
                actualField, "crocodile",
                predictedField, "cRoCoDiLe")
            .get();

        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(
                indexName,
                new Classification(
                    actualField,
                    predictedField,
                    List.of(new Accuracy(), new MulticlassConfusionMatrix(), new Precision(), new Recall())));

        Accuracy.Result accuracyResult = (Accuracy.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(accuracyResult.getClasses(), contains(new Accuracy.PerClassResult("crocodile", 0.0)));
        assertThat(accuracyResult.getOverallAccuracy(), equalTo(0.0));

        MulticlassConfusionMatrix.Result confusionMatrixResult =
            (MulticlassConfusionMatrix.Result) evaluateDataFrameResponse.getMetrics().get(1);
        assertThat(
            confusionMatrixResult.getConfusionMatrix(),
            equalTo(List.of(
                new MulticlassConfusionMatrix.ActualClass(
                    "crocodile", 1, List.of(new MulticlassConfusionMatrix.PredictedClass("crocodile", 0L)), 1))));

        Precision.Result precisionResult = (Precision.Result) evaluateDataFrameResponse.getMetrics().get(2);
        assertThat(precisionResult.getClasses(), empty());
        assertThat(precisionResult.getAvgPrecision(), is(notANumber()));

        Recall.Result recallResult = (Recall.Result) evaluateDataFrameResponse.getMetrics().get(3);
        assertThat(recallResult.getClasses(), contains(new Recall.PerClassResult("crocodile", 0.0)));
        assertThat(recallResult.getAvgRecall(), equalTo(0.0));
    }

    private Accuracy.Result evaluateAccuracy(String actualField, String predictedField) {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(ANIMALS_DATA_INDEX, new Classification(actualField, predictedField, List.of(new Accuracy())));

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics(), hasSize(1));

        Accuracy.Result accuracyResult = (Accuracy.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(accuracyResult.getMetricName(), equalTo(Accuracy.NAME.getPreferredName()));
        return accuracyResult;
    }

    public void testEvaluate_Accuracy_KeywordField() {
        List<Accuracy.PerClassResult> expectedPerClassResults =
            List.of(
                new Accuracy.PerClassResult("ant", 47.0 / 75),
                new Accuracy.PerClassResult("cat", 47.0 / 75),
                new Accuracy.PerClassResult("dog", 47.0 / 75),
                new Accuracy.PerClassResult("fox", 47.0 / 75),
                new Accuracy.PerClassResult("mouse", 47.0 / 75));
        double expectedOverallAccuracy = 5.0 / 75;

        Accuracy.Result accuracyResult = evaluateAccuracy(ANIMAL_NAME_KEYWORD_FIELD, ANIMAL_NAME_PREDICTION_KEYWORD_FIELD);
        assertThat(accuracyResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(accuracyResult.getOverallAccuracy(), equalTo(expectedOverallAccuracy));

        // Actual and predicted fields are swapped but this does not alter the result (accuracy is symmetric)
        accuracyResult = evaluateAccuracy(ANIMAL_NAME_PREDICTION_KEYWORD_FIELD, ANIMAL_NAME_KEYWORD_FIELD);
        assertThat(accuracyResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(accuracyResult.getOverallAccuracy(), equalTo(expectedOverallAccuracy));
    }

    public void testEvaluate_Accuracy_IntegerField() {
        List<Accuracy.PerClassResult> expectedPerClassResults =
            List.of(
                new Accuracy.PerClassResult("1", 57.0 / 75),
                new Accuracy.PerClassResult("2", 54.0 / 75),
                new Accuracy.PerClassResult("3", 51.0 / 75),
                new Accuracy.PerClassResult("4", 48.0 / 75),
                new Accuracy.PerClassResult("5", 45.0 / 75));
        double expectedOverallAccuracy = 15.0 / 75;

        Accuracy.Result accuracyResult = evaluateAccuracy(NO_LEGS_INTEGER_FIELD, NO_LEGS_PREDICTION_INTEGER_FIELD);
        assertThat(accuracyResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(accuracyResult.getOverallAccuracy(), equalTo(expectedOverallAccuracy));

        // Actual and predicted fields are swapped but this does not alter the result (accuracy is symmetric)
        accuracyResult = evaluateAccuracy(NO_LEGS_PREDICTION_INTEGER_FIELD, NO_LEGS_INTEGER_FIELD);
        assertThat(accuracyResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(accuracyResult.getOverallAccuracy(), equalTo(expectedOverallAccuracy));

        // Actual and predicted fields are of different types but the values are matched correctly
        accuracyResult = evaluateAccuracy(NO_LEGS_KEYWORD_FIELD, NO_LEGS_PREDICTION_INTEGER_FIELD);
        assertThat(accuracyResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(accuracyResult.getOverallAccuracy(), equalTo(expectedOverallAccuracy));

        // Actual and predicted fields are of different types but the values are matched correctly
        accuracyResult = evaluateAccuracy(NO_LEGS_PREDICTION_INTEGER_FIELD, NO_LEGS_KEYWORD_FIELD);
        assertThat(accuracyResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(accuracyResult.getOverallAccuracy(), equalTo(expectedOverallAccuracy));
    }

    public void testEvaluate_Accuracy_BooleanField() {
        List<Accuracy.PerClassResult> expectedPerClassResults =
            List.of(
                new Accuracy.PerClassResult("false", 18.0 / 30),
                new Accuracy.PerClassResult("true", 27.0 / 45));
        double expectedOverallAccuracy = 45.0 / 75;

        Accuracy.Result accuracyResult = evaluateAccuracy(IS_PREDATOR_BOOLEAN_FIELD, IS_PREDATOR_PREDICTION_BOOLEAN_FIELD);
        assertThat(accuracyResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(accuracyResult.getOverallAccuracy(), equalTo(expectedOverallAccuracy));

        // Actual and predicted fields are swapped but this does not alter the result (accuracy is symmetric)
        accuracyResult = evaluateAccuracy(IS_PREDATOR_PREDICTION_BOOLEAN_FIELD, IS_PREDATOR_BOOLEAN_FIELD);
        assertThat(accuracyResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(accuracyResult.getOverallAccuracy(), equalTo(expectedOverallAccuracy));

        // Actual and predicted fields are of different types but the values are matched correctly
        accuracyResult = evaluateAccuracy(IS_PREDATOR_KEYWORD_FIELD, IS_PREDATOR_PREDICTION_BOOLEAN_FIELD);
        assertThat(accuracyResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(accuracyResult.getOverallAccuracy(), equalTo(expectedOverallAccuracy));

        // Actual and predicted fields are of different types but the values are matched correctly
        accuracyResult = evaluateAccuracy(IS_PREDATOR_PREDICTION_BOOLEAN_FIELD, IS_PREDATOR_KEYWORD_FIELD);
        assertThat(accuracyResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(accuracyResult.getOverallAccuracy(), equalTo(expectedOverallAccuracy));
    }

    public void testEvaluate_Accuracy_FieldTypeMismatch() {
        {
            // When actual and predicted fields have different types, the sets of classes are disjoint
            List<Accuracy.PerClassResult> expectedPerClassResults =
                List.of(
                    new Accuracy.PerClassResult("1", 0.8),
                    new Accuracy.PerClassResult("2", 0.8),
                    new Accuracy.PerClassResult("3", 0.8),
                    new Accuracy.PerClassResult("4", 0.8),
                    new Accuracy.PerClassResult("5", 0.8));
            double expectedOverallAccuracy = 0.0;

            Accuracy.Result accuracyResult = evaluateAccuracy(NO_LEGS_INTEGER_FIELD, IS_PREDATOR_BOOLEAN_FIELD);
            assertThat(accuracyResult.getClasses(), equalTo(expectedPerClassResults));
            assertThat(accuracyResult.getOverallAccuracy(), equalTo(expectedOverallAccuracy));
        }
        {
            // When actual and predicted fields have different types, the sets of classes are disjoint
            List<Accuracy.PerClassResult> expectedPerClassResults =
                List.of(
                    new Accuracy.PerClassResult("false", 0.6),
                    new Accuracy.PerClassResult("true", 0.4));
            double expectedOverallAccuracy = 0.0;

            Accuracy.Result accuracyResult = evaluateAccuracy(IS_PREDATOR_BOOLEAN_FIELD, NO_LEGS_INTEGER_FIELD);
            assertThat(accuracyResult.getClasses(), equalTo(expectedPerClassResults));
            assertThat(accuracyResult.getOverallAccuracy(), equalTo(expectedOverallAccuracy));
        }
    }

    private Precision.Result evaluatePrecision(String actualField, String predictedField) {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(ANIMALS_DATA_INDEX, new Classification(actualField, predictedField, List.of(new Precision())));

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics(), hasSize(1));

        Precision.Result precisionResult = (Precision.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(precisionResult.getMetricName(), equalTo(Precision.NAME.getPreferredName()));
        return precisionResult;
    }

    public void testEvaluate_Precision_KeywordField() {
        List<Precision.PerClassResult> expectedPerClassResults =
            List.of(
                new Precision.PerClassResult("ant", 1.0 / 15),
                new Precision.PerClassResult("cat", 1.0 / 15),
                new Precision.PerClassResult("dog", 1.0 / 15),
                new Precision.PerClassResult("fox", 1.0 / 15),
                new Precision.PerClassResult("mouse", 1.0 / 15));
        double expectedAvgPrecision = 5.0 / 75;

        Precision.Result precisionResult = evaluatePrecision(ANIMAL_NAME_KEYWORD_FIELD, ANIMAL_NAME_PREDICTION_KEYWORD_FIELD);
        assertThat(precisionResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(precisionResult.getAvgPrecision(), equalTo(expectedAvgPrecision));

        evaluatePrecision(ANIMAL_NAME_PREDICTION_KEYWORD_FIELD, ANIMAL_NAME_KEYWORD_FIELD);
    }

    public void testEvaluate_Precision_IntegerField() {
        List<Precision.PerClassResult> expectedPerClassResults =
            List.of(
                new Precision.PerClassResult("1", 0.2),
                new Precision.PerClassResult("2", 0.2),
                new Precision.PerClassResult("3", 0.2),
                new Precision.PerClassResult("4", 0.2),
                new Precision.PerClassResult("5", 0.2));
        double expectedAvgPrecision = 0.2;

        Precision.Result precisionResult = evaluatePrecision(NO_LEGS_INTEGER_FIELD, NO_LEGS_PREDICTION_INTEGER_FIELD);
        assertThat(precisionResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(precisionResult.getAvgPrecision(), equalTo(expectedAvgPrecision));

        // Actual and predicted fields are of different types but the values are matched correctly
        precisionResult = evaluatePrecision(NO_LEGS_KEYWORD_FIELD, NO_LEGS_PREDICTION_INTEGER_FIELD);
        assertThat(precisionResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(precisionResult.getAvgPrecision(), equalTo(expectedAvgPrecision));

        evaluatePrecision(NO_LEGS_PREDICTION_INTEGER_FIELD, NO_LEGS_INTEGER_FIELD);

        evaluatePrecision(NO_LEGS_PREDICTION_INTEGER_FIELD, NO_LEGS_KEYWORD_FIELD);
    }

    public void testEvaluate_Precision_BooleanField() {
        List<Precision.PerClassResult> expectedPerClassResults =
            List.of(
                new Precision.PerClassResult("false", 0.5),
                new Precision.PerClassResult("true", 9.0 / 13));
        double expectedAvgPrecision = 31.0 / 52;

        Precision.Result precisionResult = evaluatePrecision(IS_PREDATOR_BOOLEAN_FIELD, IS_PREDATOR_PREDICTION_BOOLEAN_FIELD);
        assertThat(precisionResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(precisionResult.getAvgPrecision(), equalTo(expectedAvgPrecision));

        // Actual and predicted fields are of different types but the values are matched correctly
        precisionResult = evaluatePrecision(IS_PREDATOR_KEYWORD_FIELD, IS_PREDATOR_PREDICTION_BOOLEAN_FIELD);
        assertThat(precisionResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(precisionResult.getAvgPrecision(), equalTo(expectedAvgPrecision));

        evaluatePrecision(IS_PREDATOR_PREDICTION_BOOLEAN_FIELD, IS_PREDATOR_BOOLEAN_FIELD);

        evaluatePrecision(IS_PREDATOR_PREDICTION_BOOLEAN_FIELD, IS_PREDATOR_KEYWORD_FIELD);
    }

    public void testEvaluate_Precision_FieldTypeMismatch() {
        {
            Precision.Result precisionResult = evaluatePrecision(NO_LEGS_INTEGER_FIELD, IS_PREDATOR_BOOLEAN_FIELD);
            // When actual and predicted fields have different types, the sets of classes are disjoint, hence empty results here
            assertThat(precisionResult.getClasses(), empty());
            assertThat(precisionResult.getAvgPrecision(), is(notANumber()));
        }
        {
            Precision.Result precisionResult = evaluatePrecision(IS_PREDATOR_BOOLEAN_FIELD, NO_LEGS_INTEGER_FIELD);
            // When actual and predicted fields have different types, the sets of classes are disjoint, hence empty results here
            assertThat(precisionResult.getClasses(), empty());
            assertThat(precisionResult.getAvgPrecision(), is(notANumber()));
        }
    }

    public void testEvaluate_Precision_CardinalityTooHigh() {
        indexDistinctAnimals(ANIMALS_DATA_INDEX, 1001);
        ElasticsearchStatusException e =
            expectThrows(
                ElasticsearchStatusException.class,
                () -> evaluateDataFrame(
                    ANIMALS_DATA_INDEX,
                    new Classification(ANIMAL_NAME_KEYWORD_FIELD, ANIMAL_NAME_PREDICTION_KEYWORD_FIELD, List.of(new Precision()))));
        assertThat(e.getMessage(), containsString("Cardinality of field [animal_name_keyword] is too high"));
    }

    private Recall.Result evaluateRecall(String actualField, String predictedField) {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(ANIMALS_DATA_INDEX, new Classification(actualField, predictedField, List.of(new Recall())));

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics(), hasSize(1));

        Recall.Result recallResult = (Recall.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(recallResult.getMetricName(), equalTo(Recall.NAME.getPreferredName()));
        return recallResult;
    }

    public void testEvaluate_Recall_KeywordField() {
        List<Recall.PerClassResult> expectedPerClassResults =
            List.of(
                new Recall.PerClassResult("ant", 1.0 / 15),
                new Recall.PerClassResult("cat", 1.0 / 15),
                new Recall.PerClassResult("dog", 1.0 / 15),
                new Recall.PerClassResult("fox", 1.0 / 15),
                new Recall.PerClassResult("mouse", 1.0 / 15));
        double expectedAvgRecall = 5.0 / 75;

        Recall.Result recallResult = evaluateRecall(ANIMAL_NAME_KEYWORD_FIELD, ANIMAL_NAME_PREDICTION_KEYWORD_FIELD);
        assertThat(recallResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(recallResult.getAvgRecall(), equalTo(expectedAvgRecall));

        evaluateRecall(ANIMAL_NAME_PREDICTION_KEYWORD_FIELD, ANIMAL_NAME_KEYWORD_FIELD);
    }

    public void testEvaluate_Recall_IntegerField() {
        List<Recall.PerClassResult> expectedPerClassResults =
            List.of(
                new Recall.PerClassResult("1", 1.0 / 15),
                new Recall.PerClassResult("2", 2.0 / 15),
                new Recall.PerClassResult("3", 3.0 / 15),
                new Recall.PerClassResult("4", 4.0 / 15),
                new Recall.PerClassResult("5", 5.0 / 15));
        double expectedAvgRecall = 3.0 / 15;

        Recall.Result recallResult = evaluateRecall(NO_LEGS_INTEGER_FIELD, NO_LEGS_PREDICTION_INTEGER_FIELD);
        assertThat(recallResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(recallResult.getAvgRecall(), equalTo(expectedAvgRecall));

        // Actual and predicted fields are of different types but the values are matched correctly
        recallResult = evaluateRecall(NO_LEGS_KEYWORD_FIELD, NO_LEGS_PREDICTION_INTEGER_FIELD);
        assertThat(recallResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(recallResult.getAvgRecall(), equalTo(expectedAvgRecall));

        evaluateRecall(NO_LEGS_PREDICTION_INTEGER_FIELD, NO_LEGS_INTEGER_FIELD);

        evaluateRecall(NO_LEGS_PREDICTION_INTEGER_FIELD, NO_LEGS_KEYWORD_FIELD);
    }

    public void testEvaluate_Recall_BooleanField() {
        List<Recall.PerClassResult> expectedPerClassResults =
            List.of(
                new Recall.PerClassResult("true", 0.6),
                new Recall.PerClassResult("false", 0.6));
        double expectedAvgRecall = 0.6;

        Recall.Result recallResult = evaluateRecall(IS_PREDATOR_BOOLEAN_FIELD, IS_PREDATOR_PREDICTION_BOOLEAN_FIELD);
        assertThat(recallResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(recallResult.getAvgRecall(), equalTo(expectedAvgRecall));

        // Actual and predicted fields are of different types but the values are matched correctly
        recallResult = evaluateRecall(IS_PREDATOR_KEYWORD_FIELD, IS_PREDATOR_PREDICTION_BOOLEAN_FIELD);
        assertThat(recallResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(recallResult.getAvgRecall(), equalTo(expectedAvgRecall));

        evaluateRecall(IS_PREDATOR_PREDICTION_BOOLEAN_FIELD, IS_PREDATOR_BOOLEAN_FIELD);

        evaluateRecall(IS_PREDATOR_PREDICTION_BOOLEAN_FIELD, IS_PREDATOR_KEYWORD_FIELD);
    }

    public void testEvaluate_Recall_FieldTypeMismatch() {
        {
            // When actual and predicted fields have different types, the sets of classes are disjoint, hence 0.0 results here
            List<Recall.PerClassResult> expectedPerClassResults =
                List.of(
                    new Recall.PerClassResult("1", 0.0),
                    new Recall.PerClassResult("2", 0.0),
                    new Recall.PerClassResult("3", 0.0),
                    new Recall.PerClassResult("4", 0.0),
                    new Recall.PerClassResult("5", 0.0));
            double expectedAvgRecall = 0.0;

            Recall.Result recallResult = evaluateRecall(NO_LEGS_INTEGER_FIELD, IS_PREDATOR_BOOLEAN_FIELD);
            assertThat(recallResult.getClasses(), equalTo(expectedPerClassResults));
            assertThat(recallResult.getAvgRecall(), equalTo(expectedAvgRecall));
        }
        {
            // When actual and predicted fields have different types, the sets of classes are disjoint, hence 0.0 results here
            List<Recall.PerClassResult> expectedPerClassResults =
                List.of(
                    new Recall.PerClassResult("true", 0.0),
                    new Recall.PerClassResult("false", 0.0));
            double expectedAvgRecall = 0.0;

            Recall.Result recallResult = evaluateRecall(IS_PREDATOR_BOOLEAN_FIELD, NO_LEGS_INTEGER_FIELD);
            assertThat(recallResult.getClasses(), equalTo(expectedPerClassResults));
            assertThat(recallResult.getAvgRecall(), equalTo(expectedAvgRecall));
        }
    }

    public void testEvaluate_Recall_CardinalityTooHigh() {
        indexDistinctAnimals(ANIMALS_DATA_INDEX, 1001);
        ElasticsearchStatusException e =
            expectThrows(
                ElasticsearchStatusException.class,
                () -> evaluateDataFrame(
                    ANIMALS_DATA_INDEX,
                    new Classification(ANIMAL_NAME_KEYWORD_FIELD, ANIMAL_NAME_PREDICTION_KEYWORD_FIELD, List.of(new Recall()))));
        assertThat(e.getMessage(), containsString("Cardinality of field [animal_name_keyword] is too high"));
    }

    private void evaluateMulticlassConfusionMatrix() {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(
                ANIMALS_DATA_INDEX,
                new Classification(
                    ANIMAL_NAME_KEYWORD_FIELD, ANIMAL_NAME_PREDICTION_KEYWORD_FIELD, List.of(new MulticlassConfusionMatrix())));

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

    public void testEvaluate_ConfusionMatrixMetricWithDefaultSize() {
        evaluateMulticlassConfusionMatrix();

        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put("search.max_buckets", 20)).get();
        evaluateMulticlassConfusionMatrix();

        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put("search.max_buckets", 7)).get();
        evaluateMulticlassConfusionMatrix();

        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put("search.max_buckets", 6)).get();
        ElasticsearchException e = expectThrows(ElasticsearchException.class, this::evaluateMulticlassConfusionMatrix);

        assertThat(e.getCause(), is(instanceOf(TooManyBucketsException.class)));
        TooManyBucketsException tmbe = (TooManyBucketsException) e.getCause();
        assertThat(tmbe.getMaxBuckets(), equalTo(6));
    }

    public void testEvaluate_ConfusionMatrixMetricWithUserProvidedSize() {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(
                ANIMALS_DATA_INDEX,
                new Classification(
                    ANIMAL_NAME_KEYWORD_FIELD, ANIMAL_NAME_PREDICTION_KEYWORD_FIELD, List.of(new MulticlassConfusionMatrix(3, null))));

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
                ANIMAL_NAME_KEYWORD_FIELD, "type=keyword",
                ANIMAL_NAME_PREDICTION_KEYWORD_FIELD, "type=keyword",
                NO_LEGS_KEYWORD_FIELD, "type=keyword",
                NO_LEGS_INTEGER_FIELD, "type=integer",
                NO_LEGS_PREDICTION_INTEGER_FIELD, "type=integer",
                IS_PREDATOR_KEYWORD_FIELD, "type=keyword",
                IS_PREDATOR_BOOLEAN_FIELD, "type=boolean",
                IS_PREDATOR_PREDICTION_BOOLEAN_FIELD, "type=boolean")
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
                                ANIMAL_NAME_KEYWORD_FIELD, animalNames.get(i),
                                ANIMAL_NAME_PREDICTION_KEYWORD_FIELD, animalNames.get((i + j) % animalNames.size()),
                                NO_LEGS_KEYWORD_FIELD, String.valueOf(i + 1),
                                NO_LEGS_INTEGER_FIELD, i + 1,
                                NO_LEGS_PREDICTION_INTEGER_FIELD, j + 1,
                                IS_PREDATOR_KEYWORD_FIELD, String.valueOf(i % 2 == 0),
                                IS_PREDATOR_BOOLEAN_FIELD, i % 2 == 0,
                                IS_PREDATOR_PREDICTION_BOOLEAN_FIELD, (i + j) % 2 == 0));
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
                new IndexRequest(indexName)
                    .source(ANIMAL_NAME_KEYWORD_FIELD, "animal_" + i, ANIMAL_NAME_PREDICTION_KEYWORD_FIELD, randomAlphaOfLength(5)));
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }
    }

    @Override
    boolean supportsInference() {
        return true;
    }
}
