/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.AucRoc;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.PerClassSingleValue;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Precision;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Recall;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notANumber;

public class ClassificationEvaluationIT extends MlNativeDataFrameAnalyticsIntegTestCase {

    static final String ANIMALS_DATA_INDEX = "test-evaluate-animals-index";

    static final String ANIMAL_NAME_KEYWORD_FIELD = "animal_name_keyword";
    static final String ANIMAL_NAME_PREDICTION_KEYWORD_FIELD = "animal_name_keyword_prediction";
    static final String ANIMAL_NAME_PREDICTION_PROB_FIELD = "animal_name_prediction_prob";
    static final String NO_LEGS_KEYWORD_FIELD = "no_legs_keyword";
    static final String NO_LEGS_INTEGER_FIELD = "no_legs_integer";
    static final String NO_LEGS_PREDICTION_INTEGER_FIELD = "no_legs_integer_prediction";
    static final String IS_PREDATOR_KEYWORD_FIELD = "predator_keyword";
    static final String IS_PREDATOR_BOOLEAN_FIELD = "predator_boolean";
    static final String IS_PREDATOR_PREDICTION_BOOLEAN_FIELD = "predator_boolean_prediction";
    static final String IS_PREDATOR_PREDICTION_PROBABILITY_FIELD = "predator_prediction_probability";
    static final String ML_TOP_CLASSES_FIELD = "ml_results";

    @Before
    public void setup() {
        createAnimalsIndex(ANIMALS_DATA_INDEX);
        indexAnimalsData(ANIMALS_DATA_INDEX);
    }

    @After
    public void cleanup() {
        cleanUp();
        updateClusterSettings(Settings.builder().putNull("search.max_buckets"));
    }

    public void testEvaluate_DefaultMetrics() {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse = evaluateDataFrame(
            ANIMALS_DATA_INDEX,
            new Classification(ANIMAL_NAME_KEYWORD_FIELD, ANIMAL_NAME_PREDICTION_KEYWORD_FIELD, null, null)
        );

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(
            evaluateDataFrameResponse.getMetrics().stream().map(EvaluationMetricResult::getMetricName).collect(toList()),
            containsInAnyOrder(
                MulticlassConfusionMatrix.NAME.getPreferredName(),
                Accuracy.NAME.getPreferredName(),
                Precision.NAME.getPreferredName(),
                Recall.NAME.getPreferredName()
            )
        );
    }

    public void testEvaluate_AllMetrics() {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse = evaluateDataFrame(
            ANIMALS_DATA_INDEX,
            new Classification(
                ANIMAL_NAME_KEYWORD_FIELD,
                ANIMAL_NAME_PREDICTION_KEYWORD_FIELD,
                null,
                List.of(new Accuracy(), new MulticlassConfusionMatrix(), new Precision(), new Recall())
            )
        );

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(
            evaluateDataFrameResponse.getMetrics().stream().map(EvaluationMetricResult::getMetricName).collect(toList()),
            contains(
                Accuracy.NAME.getPreferredName(),
                MulticlassConfusionMatrix.NAME.getPreferredName(),
                Precision.NAME.getPreferredName(),
                Recall.NAME.getPreferredName()
            )
        );
    }

    public void testEvaluate_AllMetrics_KeywordField_CaseSensitivity() {
        String indexName = "some-index";
        String actualField = "fieldA";
        String predictedField = "fieldB";
        client().admin().indices().prepareCreate(indexName).setMapping(actualField, "type=keyword", predictedField, "type=keyword").get();
        client().prepareIndex(indexName)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .setSource(actualField, "crocodile", predictedField, "cRoCoDiLe")
            .get();

        EvaluateDataFrameAction.Response evaluateDataFrameResponse = evaluateDataFrame(
            indexName,
            new Classification(
                actualField,
                predictedField,
                null,
                List.of(new Accuracy(), new MulticlassConfusionMatrix(), new Precision(), new Recall())
            )
        );

        Accuracy.Result accuracyResult = (Accuracy.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(accuracyResult.getClasses(), contains(new PerClassSingleValue("crocodile", 0.0)));
        assertThat(accuracyResult.getOverallAccuracy(), equalTo(0.0));

        MulticlassConfusionMatrix.Result confusionMatrixResult = (MulticlassConfusionMatrix.Result) evaluateDataFrameResponse.getMetrics()
            .get(1);
        assertThat(
            confusionMatrixResult.getConfusionMatrix(),
            equalTo(
                List.of(
                    new MulticlassConfusionMatrix.ActualClass(
                        "crocodile",
                        1,
                        List.of(new MulticlassConfusionMatrix.PredictedClass("crocodile", 0L)),
                        1
                    )
                )
            )
        );

        Precision.Result precisionResult = (Precision.Result) evaluateDataFrameResponse.getMetrics().get(2);
        assertThat(precisionResult.getClasses(), empty());
        assertThat(precisionResult.getAvgPrecision(), is(notANumber()));

        Recall.Result recallResult = (Recall.Result) evaluateDataFrameResponse.getMetrics().get(3);
        assertThat(recallResult.getClasses(), contains(new PerClassSingleValue("crocodile", 0.0)));
        assertThat(recallResult.getAvgRecall(), equalTo(0.0));
    }

    private AucRoc.Result evaluateAucRoc(boolean includeCurve) {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse = evaluateDataFrame(
            ANIMALS_DATA_INDEX,
            new Classification(ANIMAL_NAME_KEYWORD_FIELD, null, ML_TOP_CLASSES_FIELD, List.of(new AucRoc(includeCurve, "cat")))
        );

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics(), hasSize(1));

        AucRoc.Result aucrocResult = (AucRoc.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(aucrocResult.getMetricName(), equalTo(AucRoc.NAME.getPreferredName()));
        return aucrocResult;
    }

    public void testEvaluate_AucRoc_DoNotIncludeCurve() {
        AucRoc.Result aucrocResult = evaluateAucRoc(false);
        assertThat(aucrocResult.getValue(), is(closeTo(0.5, 0.0001)));
        assertThat(aucrocResult.getCurve(), hasSize(0));
    }

    public void testEvaluate_AucRoc_IncludeCurve() {
        AucRoc.Result aucrocResult = evaluateAucRoc(true);
        assertThat(aucrocResult.getValue(), is(closeTo(0.5, 0.0001)));
        assertThat(aucrocResult.getCurve(), hasSize(greaterThan(0)));
    }

    private Accuracy.Result evaluateAccuracy(String actualField, String predictedField) {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse = evaluateDataFrame(
            ANIMALS_DATA_INDEX,
            new Classification(actualField, predictedField, null, List.of(new Accuracy()))
        );

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics(), hasSize(1));

        Accuracy.Result accuracyResult = (Accuracy.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(accuracyResult.getMetricName(), equalTo(Accuracy.NAME.getPreferredName()));
        return accuracyResult;
    }

    public void testEvaluate_Accuracy_KeywordField() {
        List<PerClassSingleValue> expectedPerClassResults = List.of(
            new PerClassSingleValue("ant", 47.0 / 75),
            new PerClassSingleValue("cat", 47.0 / 75),
            new PerClassSingleValue("dog", 47.0 / 75),
            new PerClassSingleValue("fox", 47.0 / 75),
            new PerClassSingleValue("mouse", 47.0 / 75)
        );
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
        List<PerClassSingleValue> expectedPerClassResults = List.of(
            new PerClassSingleValue("1", 57.0 / 75),
            new PerClassSingleValue("2", 54.0 / 75),
            new PerClassSingleValue("3", 51.0 / 75),
            new PerClassSingleValue("4", 48.0 / 75),
            new PerClassSingleValue("5", 45.0 / 75)
        );
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
        List<PerClassSingleValue> expectedPerClassResults = List.of(
            new PerClassSingleValue("false", 18.0 / 30),
            new PerClassSingleValue("true", 27.0 / 45)
        );
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
            List<PerClassSingleValue> expectedPerClassResults = List.of(
                new PerClassSingleValue("1", 0.8),
                new PerClassSingleValue("2", 0.8),
                new PerClassSingleValue("3", 0.8),
                new PerClassSingleValue("4", 0.8),
                new PerClassSingleValue("5", 0.8)
            );
            double expectedOverallAccuracy = 0.0;

            Accuracy.Result accuracyResult = evaluateAccuracy(NO_LEGS_INTEGER_FIELD, IS_PREDATOR_BOOLEAN_FIELD);
            assertThat(accuracyResult.getClasses(), equalTo(expectedPerClassResults));
            assertThat(accuracyResult.getOverallAccuracy(), equalTo(expectedOverallAccuracy));
        }
        {
            // When actual and predicted fields have different types, the sets of classes are disjoint
            List<PerClassSingleValue> expectedPerClassResults = List.of(
                new PerClassSingleValue("false", 0.6),
                new PerClassSingleValue("true", 0.4)
            );
            double expectedOverallAccuracy = 0.0;

            Accuracy.Result accuracyResult = evaluateAccuracy(IS_PREDATOR_BOOLEAN_FIELD, NO_LEGS_INTEGER_FIELD);
            assertThat(accuracyResult.getClasses(), equalTo(expectedPerClassResults));
            assertThat(accuracyResult.getOverallAccuracy(), equalTo(expectedOverallAccuracy));
        }
    }

    private Precision.Result evaluatePrecision(String actualField, String predictedField) {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse = evaluateDataFrame(
            ANIMALS_DATA_INDEX,
            new Classification(actualField, predictedField, null, List.of(new Precision()))
        );

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics(), hasSize(1));

        Precision.Result precisionResult = (Precision.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(precisionResult.getMetricName(), equalTo(Precision.NAME.getPreferredName()));
        return precisionResult;
    }

    public void testEvaluate_Precision_KeywordField() {
        List<PerClassSingleValue> expectedPerClassResults = List.of(
            new PerClassSingleValue("ant", 1.0 / 15),
            new PerClassSingleValue("cat", 1.0 / 15),
            new PerClassSingleValue("dog", 1.0 / 15),
            new PerClassSingleValue("fox", 1.0 / 15),
            new PerClassSingleValue("mouse", 1.0 / 15)
        );
        double expectedAvgPrecision = 5.0 / 75;

        Precision.Result precisionResult = evaluatePrecision(ANIMAL_NAME_KEYWORD_FIELD, ANIMAL_NAME_PREDICTION_KEYWORD_FIELD);
        assertThat(precisionResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(precisionResult.getAvgPrecision(), equalTo(expectedAvgPrecision));

        evaluatePrecision(ANIMAL_NAME_PREDICTION_KEYWORD_FIELD, ANIMAL_NAME_KEYWORD_FIELD);
    }

    public void testEvaluate_Precision_IntegerField() {
        List<PerClassSingleValue> expectedPerClassResults = List.of(
            new PerClassSingleValue("1", 0.2),
            new PerClassSingleValue("2", 0.2),
            new PerClassSingleValue("3", 0.2),
            new PerClassSingleValue("4", 0.2),
            new PerClassSingleValue("5", 0.2)
        );
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
        List<PerClassSingleValue> expectedPerClassResults = List.of(
            new PerClassSingleValue("false", 0.5),
            new PerClassSingleValue("true", 9.0 / 13)
        );
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
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> evaluateDataFrame(
                ANIMALS_DATA_INDEX,
                new Classification(ANIMAL_NAME_KEYWORD_FIELD, ANIMAL_NAME_PREDICTION_KEYWORD_FIELD, null, List.of(new Precision()))
            )
        );
        assertThat(e.getMessage(), containsString("Cardinality of field [animal_name_keyword] is too high"));
    }

    private Recall.Result evaluateRecall(String actualField, String predictedField) {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse = evaluateDataFrame(
            ANIMALS_DATA_INDEX,
            new Classification(actualField, predictedField, null, List.of(new Recall()))
        );

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics(), hasSize(1));

        Recall.Result recallResult = (Recall.Result) evaluateDataFrameResponse.getMetrics().get(0);
        assertThat(recallResult.getMetricName(), equalTo(Recall.NAME.getPreferredName()));
        return recallResult;
    }

    public void testEvaluate_Recall_KeywordField() {
        List<PerClassSingleValue> expectedPerClassResults = List.of(
            new PerClassSingleValue("ant", 1.0 / 15),
            new PerClassSingleValue("cat", 1.0 / 15),
            new PerClassSingleValue("dog", 1.0 / 15),
            new PerClassSingleValue("fox", 1.0 / 15),
            new PerClassSingleValue("mouse", 1.0 / 15)
        );
        double expectedAvgRecall = 5.0 / 75;

        Recall.Result recallResult = evaluateRecall(ANIMAL_NAME_KEYWORD_FIELD, ANIMAL_NAME_PREDICTION_KEYWORD_FIELD);
        assertThat(recallResult.getClasses(), equalTo(expectedPerClassResults));
        assertThat(recallResult.getAvgRecall(), equalTo(expectedAvgRecall));

        evaluateRecall(ANIMAL_NAME_PREDICTION_KEYWORD_FIELD, ANIMAL_NAME_KEYWORD_FIELD);
    }

    public void testEvaluate_Recall_IntegerField() {
        List<PerClassSingleValue> expectedPerClassResults = List.of(
            new PerClassSingleValue("1", 1.0 / 15),
            new PerClassSingleValue("2", 2.0 / 15),
            new PerClassSingleValue("3", 3.0 / 15),
            new PerClassSingleValue("4", 4.0 / 15),
            new PerClassSingleValue("5", 5.0 / 15)
        );
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
        List<PerClassSingleValue> expectedPerClassResults = List.of(
            new PerClassSingleValue("true", 0.6),
            new PerClassSingleValue("false", 0.6)
        );
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
            List<PerClassSingleValue> expectedPerClassResults = List.of(
                new PerClassSingleValue("1", 0.0),
                new PerClassSingleValue("2", 0.0),
                new PerClassSingleValue("3", 0.0),
                new PerClassSingleValue("4", 0.0),
                new PerClassSingleValue("5", 0.0)
            );
            double expectedAvgRecall = 0.0;

            Recall.Result recallResult = evaluateRecall(NO_LEGS_INTEGER_FIELD, IS_PREDATOR_BOOLEAN_FIELD);
            assertThat(recallResult.getClasses(), equalTo(expectedPerClassResults));
            assertThat(recallResult.getAvgRecall(), equalTo(expectedAvgRecall));
        }
        {
            // When actual and predicted fields have different types, the sets of classes are disjoint, hence 0.0 results here
            List<PerClassSingleValue> expectedPerClassResults = List.of(
                new PerClassSingleValue("true", 0.0),
                new PerClassSingleValue("false", 0.0)
            );
            double expectedAvgRecall = 0.0;

            Recall.Result recallResult = evaluateRecall(IS_PREDATOR_BOOLEAN_FIELD, NO_LEGS_INTEGER_FIELD);
            assertThat(recallResult.getClasses(), equalTo(expectedPerClassResults));
            assertThat(recallResult.getAvgRecall(), equalTo(expectedAvgRecall));
        }
    }

    public void testEvaluate_Recall_CardinalityTooHigh() {
        indexDistinctAnimals(ANIMALS_DATA_INDEX, 1001);
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> evaluateDataFrame(
                ANIMALS_DATA_INDEX,
                new Classification(ANIMAL_NAME_KEYWORD_FIELD, ANIMAL_NAME_PREDICTION_KEYWORD_FIELD, null, List.of(new Recall()))
            )
        );
        assertThat(e.getMessage(), containsString("Cardinality of field [animal_name_keyword] is too high"));
    }

    private void evaluateMulticlassConfusionMatrix() {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse = evaluateDataFrame(
            ANIMALS_DATA_INDEX,
            new Classification(
                ANIMAL_NAME_KEYWORD_FIELD,
                ANIMAL_NAME_PREDICTION_KEYWORD_FIELD,
                null,
                List.of(new MulticlassConfusionMatrix())
            )
        );

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics(), hasSize(1));

        MulticlassConfusionMatrix.Result confusionMatrixResult = (MulticlassConfusionMatrix.Result) evaluateDataFrameResponse.getMetrics()
            .get(0);
        assertThat(confusionMatrixResult.getMetricName(), equalTo(MulticlassConfusionMatrix.NAME.getPreferredName()));
        assertThat(
            confusionMatrixResult.getConfusionMatrix(),
            equalTo(
                List.of(
                    new MulticlassConfusionMatrix.ActualClass(
                        "ant",
                        15,
                        List.of(
                            new MulticlassConfusionMatrix.PredictedClass("ant", 1L),
                            new MulticlassConfusionMatrix.PredictedClass("cat", 4L),
                            new MulticlassConfusionMatrix.PredictedClass("dog", 3L),
                            new MulticlassConfusionMatrix.PredictedClass("fox", 2L),
                            new MulticlassConfusionMatrix.PredictedClass("mouse", 5L)
                        ),
                        0
                    ),
                    new MulticlassConfusionMatrix.ActualClass(
                        "cat",
                        15,
                        List.of(
                            new MulticlassConfusionMatrix.PredictedClass("ant", 3L),
                            new MulticlassConfusionMatrix.PredictedClass("cat", 1L),
                            new MulticlassConfusionMatrix.PredictedClass("dog", 5L),
                            new MulticlassConfusionMatrix.PredictedClass("fox", 4L),
                            new MulticlassConfusionMatrix.PredictedClass("mouse", 2L)
                        ),
                        0
                    ),
                    new MulticlassConfusionMatrix.ActualClass(
                        "dog",
                        15,
                        List.of(
                            new MulticlassConfusionMatrix.PredictedClass("ant", 4L),
                            new MulticlassConfusionMatrix.PredictedClass("cat", 2L),
                            new MulticlassConfusionMatrix.PredictedClass("dog", 1L),
                            new MulticlassConfusionMatrix.PredictedClass("fox", 5L),
                            new MulticlassConfusionMatrix.PredictedClass("mouse", 3L)
                        ),
                        0
                    ),
                    new MulticlassConfusionMatrix.ActualClass(
                        "fox",
                        15,
                        List.of(
                            new MulticlassConfusionMatrix.PredictedClass("ant", 5L),
                            new MulticlassConfusionMatrix.PredictedClass("cat", 3L),
                            new MulticlassConfusionMatrix.PredictedClass("dog", 2L),
                            new MulticlassConfusionMatrix.PredictedClass("fox", 1L),
                            new MulticlassConfusionMatrix.PredictedClass("mouse", 4L)
                        ),
                        0
                    ),
                    new MulticlassConfusionMatrix.ActualClass(
                        "mouse",
                        15,
                        List.of(
                            new MulticlassConfusionMatrix.PredictedClass("ant", 2L),
                            new MulticlassConfusionMatrix.PredictedClass("cat", 5L),
                            new MulticlassConfusionMatrix.PredictedClass("dog", 4L),
                            new MulticlassConfusionMatrix.PredictedClass("fox", 3L),
                            new MulticlassConfusionMatrix.PredictedClass("mouse", 1L)
                        ),
                        0
                    )
                )
            )
        );
        assertThat(confusionMatrixResult.getOtherActualClassCount(), equalTo(0L));
    }

    public void testEvaluate_ConfusionMatrixMetricWithDefaultSize() {
        evaluateMulticlassConfusionMatrix();

        updateClusterSettings(Settings.builder().put("search.max_buckets", 20));
        evaluateMulticlassConfusionMatrix();

        updateClusterSettings(Settings.builder().put("search.max_buckets", 7));
        evaluateMulticlassConfusionMatrix();

        updateClusterSettings(Settings.builder().put("search.max_buckets", 6));
        ElasticsearchException e = expectThrows(ElasticsearchException.class, this::evaluateMulticlassConfusionMatrix);

        assertThat(e.getCause(), is(instanceOf(TooManyBucketsException.class)));
        TooManyBucketsException tmbe = (TooManyBucketsException) e.getCause();
        assertThat(tmbe.getMaxBuckets(), equalTo(6));
    }

    public void testEvaluate_ConfusionMatrixMetricWithUserProvidedSize() {
        EvaluateDataFrameAction.Response evaluateDataFrameResponse = evaluateDataFrame(
            ANIMALS_DATA_INDEX,
            new Classification(
                ANIMAL_NAME_KEYWORD_FIELD,
                ANIMAL_NAME_PREDICTION_KEYWORD_FIELD,
                null,
                List.of(new MulticlassConfusionMatrix(3, null))
            )
        );

        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics(), hasSize(1));
        MulticlassConfusionMatrix.Result confusionMatrixResult = (MulticlassConfusionMatrix.Result) evaluateDataFrameResponse.getMetrics()
            .get(0);
        assertThat(confusionMatrixResult.getMetricName(), equalTo(MulticlassConfusionMatrix.NAME.getPreferredName()));
        assertThat(
            confusionMatrixResult.getConfusionMatrix(),
            equalTo(
                List.of(
                    new MulticlassConfusionMatrix.ActualClass(
                        "ant",
                        15,
                        List.of(
                            new MulticlassConfusionMatrix.PredictedClass("ant", 1L),
                            new MulticlassConfusionMatrix.PredictedClass("cat", 4L),
                            new MulticlassConfusionMatrix.PredictedClass("dog", 3L)
                        ),
                        7
                    ),
                    new MulticlassConfusionMatrix.ActualClass(
                        "cat",
                        15,
                        List.of(
                            new MulticlassConfusionMatrix.PredictedClass("ant", 3L),
                            new MulticlassConfusionMatrix.PredictedClass("cat", 1L),
                            new MulticlassConfusionMatrix.PredictedClass("dog", 5L)
                        ),
                        6
                    ),
                    new MulticlassConfusionMatrix.ActualClass(
                        "dog",
                        15,
                        List.of(
                            new MulticlassConfusionMatrix.PredictedClass("ant", 4L),
                            new MulticlassConfusionMatrix.PredictedClass("cat", 2L),
                            new MulticlassConfusionMatrix.PredictedClass("dog", 1L)
                        ),
                        8
                    )
                )
            )
        );
        assertThat(confusionMatrixResult.getOtherActualClassCount(), equalTo(2L));
    }

    static void createAnimalsIndex(String indexName) {
        indicesAdmin().prepareCreate(indexName)
            .setMapping(
                ANIMAL_NAME_KEYWORD_FIELD,
                "type=keyword",
                ANIMAL_NAME_PREDICTION_KEYWORD_FIELD,
                "type=keyword",
                NO_LEGS_KEYWORD_FIELD,
                "type=keyword",
                NO_LEGS_INTEGER_FIELD,
                "type=integer",
                NO_LEGS_PREDICTION_INTEGER_FIELD,
                "type=integer",
                IS_PREDATOR_KEYWORD_FIELD,
                "type=keyword",
                IS_PREDATOR_BOOLEAN_FIELD,
                "type=boolean",
                IS_PREDATOR_PREDICTION_BOOLEAN_FIELD,
                "type=boolean",
                IS_PREDATOR_PREDICTION_PROBABILITY_FIELD,
                "type=double",
                ML_TOP_CLASSES_FIELD,
                "type=nested"
            )
            .get();
    }

    static void indexAnimalsData(String indexName) {
        List<String> animalNames = List.of("dog", "cat", "mouse", "ant", "fox");
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < animalNames.size(); i++) {
            for (int j = 0; j < animalNames.size(); j++) {
                for (int k = 0; k < j + 1; k++) {
                    List<?> topClasses = IntStream.range(0, 5).mapToObj(ix -> new HashMap<String, Object>() {
                        {
                            put("class_name", animalNames.get(ix));
                            put("class_probability", 0.4 - 0.1 * ix);
                        }
                    }).collect(toList());
                    bulkRequestBuilder.add(
                        new IndexRequest(indexName).source(
                            ANIMAL_NAME_KEYWORD_FIELD,
                            animalNames.get(i),
                            ANIMAL_NAME_PREDICTION_KEYWORD_FIELD,
                            animalNames.get((i + j) % animalNames.size()),
                            ANIMAL_NAME_PREDICTION_PROB_FIELD,
                            animalNames.get((i + j) % animalNames.size()),
                            NO_LEGS_KEYWORD_FIELD,
                            String.valueOf(i + 1),
                            NO_LEGS_INTEGER_FIELD,
                            i + 1,
                            NO_LEGS_PREDICTION_INTEGER_FIELD,
                            j + 1,
                            IS_PREDATOR_KEYWORD_FIELD,
                            String.valueOf(i % 2 == 0),
                            IS_PREDATOR_BOOLEAN_FIELD,
                            i % 2 == 0,
                            IS_PREDATOR_PREDICTION_BOOLEAN_FIELD,
                            (i + j) % 2 == 0,
                            IS_PREDATOR_PREDICTION_PROBABILITY_FIELD,
                            i % 2 == 0 ? 1.0 - 0.1 * i : 0.1 * i,
                            ML_TOP_CLASSES_FIELD,
                            topClasses
                        )
                    );
                }
            }
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }
    }

    private static void indexDistinctAnimals(String indexName, int distinctAnimalCount) {
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < distinctAnimalCount; i++) {
            bulkRequestBuilder.add(
                new IndexRequest(indexName).source(
                    ANIMAL_NAME_KEYWORD_FIELD,
                    "animal_" + i,
                    ANIMAL_NAME_PREDICTION_KEYWORD_FIELD,
                    randomAlphaOfLength(5)
                )
            );
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
