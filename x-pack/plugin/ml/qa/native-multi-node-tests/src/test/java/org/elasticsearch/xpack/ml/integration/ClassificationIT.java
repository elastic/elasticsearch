/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import com.google.common.collect.Ordering;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.ml.action.EvaluateDataFrameAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.BoostedTreeParams;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.BoostedTreeParamsTests;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Accuracy;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

public class ClassificationIT extends MlNativeDataFrameAnalyticsIntegTestCase {

    private static final String BOOLEAN_FIELD = "boolean-field";
    private static final String NUMERICAL_FIELD = "numerical-field";
    private static final String DISCRETE_NUMERICAL_FIELD = "discrete-numerical-field";
    private static final String KEYWORD_FIELD = "keyword-field";
    private static final List<Boolean> BOOLEAN_FIELD_VALUES = Collections.unmodifiableList(Arrays.asList(false, true));
    private static final List<Double> NUMERICAL_FIELD_VALUES = Collections.unmodifiableList(Arrays.asList(1.0, 2.0));
    private static final List<Integer> DISCRETE_NUMERICAL_FIELD_VALUES = Collections.unmodifiableList(Arrays.asList(10, 20));
    private static final List<String> KEYWORD_FIELD_VALUES = Collections.unmodifiableList(Arrays.asList("cat", "dog"));

    private String jobId;
    private String sourceIndex;
    private String destIndex;

    @After
    public void cleanup() {
        cleanUp();
    }

    public void testSingleNumericFeatureAndMixedTrainingAndNonTrainingRows() throws Exception {
        initialize("classification_single_numeric_feature_and_mixed_data_set");
        indexData(sourceIndex, 300, 50, KEYWORD_FIELD);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Classification(KEYWORD_FIELD));
        registerAnalytics(config);
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgress(jobId, 0, 0, 0, 0);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        client().admin().indices().refresh(new RefreshRequest(destIndex));
        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            Map<String, Object> destDoc = getDestDoc(config, hit);
            Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(destDoc);

            assertThat(resultsObject.containsKey("keyword-field_prediction"), is(true));
            assertThat((String) resultsObject.get("keyword-field_prediction"), is(in(KEYWORD_FIELD_VALUES)));
            assertThat(resultsObject.containsKey("is_training"), is(true));
            assertThat(resultsObject.get("is_training"), is(destDoc.containsKey(KEYWORD_FIELD)));
            assertTopClasses(resultsObject, 2, KEYWORD_FIELD, KEYWORD_FIELD_VALUES);
        }

        assertProgress(jobId, 100, 100, 100, 100);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertThatAuditMessagesMatch(jobId,
            "Created analytics with analysis type [classification]",
            "Estimated memory usage for this analytics to be",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Finished analysis");
        assertEvaluation(KEYWORD_FIELD, KEYWORD_FIELD_VALUES, "ml.keyword-field_prediction.keyword");
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsHundred() throws Exception {
        initialize("classification_only_training_data_and_training_percent_is_100");
        indexData(sourceIndex, 300, 0, KEYWORD_FIELD);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Classification(KEYWORD_FIELD));
        registerAnalytics(config);
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgress(jobId, 0, 0, 0, 0);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        client().admin().indices().refresh(new RefreshRequest(destIndex));
        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(getDestDoc(config, hit));

            assertThat(resultsObject.containsKey("keyword-field_prediction"), is(true));
            assertThat((String) resultsObject.get("keyword-field_prediction"), is(in(KEYWORD_FIELD_VALUES)));
            assertThat(resultsObject.containsKey("is_training"), is(true));
            assertThat(resultsObject.get("is_training"), is(true));
            assertTopClasses(resultsObject, 2, KEYWORD_FIELD, KEYWORD_FIELD_VALUES);
        }

        assertProgress(jobId, 100, 100, 100, 100);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertThatAuditMessagesMatch(jobId,
            "Created analytics with analysis type [classification]",
            "Estimated memory usage for this analytics to be",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Finished analysis");
        assertEvaluation(KEYWORD_FIELD, KEYWORD_FIELD_VALUES, "ml.keyword-field_prediction.keyword");
    }

    public <T> void testWithOnlyTrainingRowsAndTrainingPercentIsFifty(
            String jobId, String dependentVariable, List<T> dependentVariableValues) throws Exception {
        initialize(jobId);
        String predictedClassField = dependentVariable + "_prediction";
        indexData(sourceIndex, 300, 0, dependentVariable);

        int numTopClasses = 2;
        DataFrameAnalyticsConfig config =
            buildAnalytics(
                jobId,
                sourceIndex,
                destIndex,
                null,
                new Classification(dependentVariable, BoostedTreeParamsTests.createRandom(), null, numTopClasses, 50.0, null));
        registerAnalytics(config);
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgress(jobId, 0, 0, 0, 0);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        int trainingRowsCount = 0;
        int nonTrainingRowsCount = 0;
        client().admin().indices().refresh(new RefreshRequest(destIndex));
        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(getDestDoc(config, hit));
            assertThat(resultsObject.containsKey(predictedClassField), is(true));
            @SuppressWarnings("unchecked")
            T predictedClassValue = (T) resultsObject.get(predictedClassField);
            assertThat(predictedClassValue, is(in(dependentVariableValues)));
            assertTopClasses(resultsObject, numTopClasses, dependentVariable, dependentVariableValues);

            assertThat(resultsObject.containsKey("is_training"), is(true));
            // Let's just assert there's both training and non-training results
            if ((boolean) resultsObject.get("is_training")) {
                trainingRowsCount++;
            } else {
                nonTrainingRowsCount++;
            }
        }
        assertThat(trainingRowsCount, greaterThan(0));
        assertThat(nonTrainingRowsCount, greaterThan(0));

        assertProgress(jobId, 100, 100, 100, 100);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertThatAuditMessagesMatch(jobId,
            "Created analytics with analysis type [classification]",
            "Estimated memory usage for this analytics to be",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Finished analysis");
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsFifty_DependentVariableIsKeyword() throws Exception {
        testWithOnlyTrainingRowsAndTrainingPercentIsFifty(
            "classification_training_percent_is_50_keyword", KEYWORD_FIELD, KEYWORD_FIELD_VALUES);
        assertEvaluation(KEYWORD_FIELD, KEYWORD_FIELD_VALUES, "ml.keyword-field_prediction.keyword");
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsFifty_DependentVariableIsInteger() throws Exception {
        testWithOnlyTrainingRowsAndTrainingPercentIsFifty(
            "classification_training_percent_is_50_integer", DISCRETE_NUMERICAL_FIELD, DISCRETE_NUMERICAL_FIELD_VALUES);
        assertEvaluation(DISCRETE_NUMERICAL_FIELD, DISCRETE_NUMERICAL_FIELD_VALUES, "ml.discrete-numerical-field_prediction");
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsFifty_DependentVariableIsDouble() throws Exception {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> testWithOnlyTrainingRowsAndTrainingPercentIsFifty(
                "classification_training_percent_is_50_double", NUMERICAL_FIELD, NUMERICAL_FIELD_VALUES));
        assertThat(e.getMessage(), startsWith("invalid types [double] for required field [numerical-field];"));
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsFifty_DependentVariableIsBoolean() throws Exception {
        testWithOnlyTrainingRowsAndTrainingPercentIsFifty(
            "classification_training_percent_is_50_boolean", BOOLEAN_FIELD, BOOLEAN_FIELD_VALUES);
        assertEvaluation(BOOLEAN_FIELD, BOOLEAN_FIELD_VALUES, "ml.boolean-field_prediction");
    }

    public void testDependentVariableCardinalityTooHighError() throws Exception {
        initialize("cardinality_too_high");
        indexData(sourceIndex, 6, 5, KEYWORD_FIELD);
        // Index one more document with a class different than the two already used.
        client().execute(IndexAction.INSTANCE, new IndexRequest(sourceIndex)
            .source(KEYWORD_FIELD, "fox")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
            .actionGet();

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Classification(KEYWORD_FIELD));
        registerAnalytics(config);
        putAnalytics(config);

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> startAnalytics(jobId));
        assertThat(e.status().getStatus(), equalTo(400));
        assertThat(e.getMessage(), equalTo("Field [keyword-field] must have at most [2] distinct values but there were at least [3]"));
    }

    public void testDependentVariableCardinalityTooHighButWithQueryMakesItWithinRange() throws Exception {
        initialize("cardinality_too_high_with_query");
        indexData(sourceIndex, 6, 5, KEYWORD_FIELD);
        // Index one more document with a class different than the two already used.
        client().execute(IndexAction.INSTANCE, new IndexRequest(sourceIndex)
            .source(KEYWORD_FIELD, "fox")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
            .actionGet();
        QueryBuilder query = QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery(KEYWORD_FIELD, KEYWORD_FIELD_VALUES));

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Classification(KEYWORD_FIELD), query);
        registerAnalytics(config);
        putAnalytics(config);

        // Should not throw
        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        assertProgress(jobId, 100, 100, 100, 100);
    }

    public void testTwoJobsWithSameRandomizeSeedUseSameTrainingSet() throws Exception {
        String sourceIndex = "classification_two_jobs_with_same_randomize_seed_source";
        String dependentVariable = KEYWORD_FIELD;

        // We use 100 rows as we can't set this too low. If too low it is possible
        // we only train with rows of one of the two classes which leads to a failure.
        indexData(sourceIndex, 100, 0, dependentVariable);

        String firstJobId = "classification_two_jobs_with_same_randomize_seed_1";
        String firstJobDestIndex = firstJobId + "_dest";

        BoostedTreeParams boostedTreeParams = new BoostedTreeParams(1.0, 1.0, 1.0, 1, 1.0);

        DataFrameAnalyticsConfig firstJob = buildAnalytics(firstJobId, sourceIndex, firstJobDestIndex, null,
            new Classification(dependentVariable, boostedTreeParams, null, 1, 50.0, null));
        registerAnalytics(firstJob);
        putAnalytics(firstJob);

        String secondJobId = "classification_two_jobs_with_same_randomize_seed_2";
        String secondJobDestIndex = secondJobId + "_dest";

        long randomizeSeed = ((Classification) firstJob.getAnalysis()).getRandomizeSeed();
        DataFrameAnalyticsConfig secondJob = buildAnalytics(secondJobId, sourceIndex, secondJobDestIndex, null,
            new Classification(dependentVariable, boostedTreeParams, null, 1, 50.0, randomizeSeed));

        registerAnalytics(secondJob);
        putAnalytics(secondJob);

        // Let's run both jobs in parallel and wait until they are finished
        startAnalytics(firstJobId);
        startAnalytics(secondJobId);
        waitUntilAnalyticsIsStopped(firstJobId);
        waitUntilAnalyticsIsStopped(secondJobId);

        // Now we compare they both used the same training rows
        Set<String> firstRunTrainingRowsIds = getTrainingRowsIds(firstJobDestIndex);
        Set<String> secondRunTrainingRowsIds = getTrainingRowsIds(secondJobDestIndex);

        assertThat(secondRunTrainingRowsIds, equalTo(firstRunTrainingRowsIds));
    }

    private void initialize(String jobId) {
        this.jobId = jobId;
        this.sourceIndex = jobId + "_source_index";
        this.destIndex = sourceIndex + "_results";
    }

    private static void indexData(String sourceIndex, int numTrainingRows, int numNonTrainingRows, String dependentVariable) {
        client().admin().indices().prepareCreate(sourceIndex)
            .addMapping("_doc",
                BOOLEAN_FIELD, "type=boolean",
                NUMERICAL_FIELD, "type=double",
                DISCRETE_NUMERICAL_FIELD, "type=integer",
                KEYWORD_FIELD, "type=keyword")
            .get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < numTrainingRows; i++) {
            List<Object> source = List.of(
                BOOLEAN_FIELD, BOOLEAN_FIELD_VALUES.get(i % BOOLEAN_FIELD_VALUES.size()),
                NUMERICAL_FIELD, NUMERICAL_FIELD_VALUES.get(i % NUMERICAL_FIELD_VALUES.size()),
                DISCRETE_NUMERICAL_FIELD, DISCRETE_NUMERICAL_FIELD_VALUES.get(i % DISCRETE_NUMERICAL_FIELD_VALUES.size()),
                KEYWORD_FIELD, KEYWORD_FIELD_VALUES.get(i % KEYWORD_FIELD_VALUES.size()));
            IndexRequest indexRequest = new IndexRequest(sourceIndex).source(source.toArray());
            bulkRequestBuilder.add(indexRequest);
        }
        for (int i = numTrainingRows; i < numTrainingRows + numNonTrainingRows; i++) {
            List<Object> source = new ArrayList<>();
            if (BOOLEAN_FIELD.equals(dependentVariable) == false) {
                source.addAll(List.of(BOOLEAN_FIELD, BOOLEAN_FIELD_VALUES.get(i % BOOLEAN_FIELD_VALUES.size())));
            }
            if (NUMERICAL_FIELD.equals(dependentVariable) == false) {
                source.addAll(List.of(NUMERICAL_FIELD, NUMERICAL_FIELD_VALUES.get(i % NUMERICAL_FIELD_VALUES.size())));
            }
            if (DISCRETE_NUMERICAL_FIELD.equals(dependentVariable) == false) {
                source.addAll(
                    List.of(DISCRETE_NUMERICAL_FIELD, DISCRETE_NUMERICAL_FIELD_VALUES.get(i % DISCRETE_NUMERICAL_FIELD_VALUES.size())));
            }
            if (KEYWORD_FIELD.equals(dependentVariable) == false) {
                source.addAll(List.of(KEYWORD_FIELD, KEYWORD_FIELD_VALUES.get(i % KEYWORD_FIELD_VALUES.size())));
            }
            IndexRequest indexRequest = new IndexRequest(sourceIndex).source(source.toArray());
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }
    }

    private static Map<String, Object> getDestDoc(DataFrameAnalyticsConfig config, SearchHit hit) {
        GetResponse destDocGetResponse = client().prepareGet().setIndex(config.getDest().getIndex()).setId(hit.getId()).get();
        assertThat(destDocGetResponse.isExists(), is(true));
        Map<String, Object> sourceDoc = hit.getSourceAsMap();
        Map<String, Object> destDoc = destDocGetResponse.getSource();
        for (String field : sourceDoc.keySet()) {
            assertThat(destDoc.containsKey(field), is(true));
            assertThat(destDoc.get(field), equalTo(sourceDoc.get(field)));
        }
        return destDoc;
    }

    private static Map<String, Object> getMlResultsObjectFromDestDoc(Map<String, Object> destDoc) {
        assertThat(destDoc.containsKey("ml"), is(true));
        @SuppressWarnings("unchecked")
        Map<String, Object> resultsObject = (Map<String, Object>) destDoc.get("ml");
        return resultsObject;
    }

    @SuppressWarnings("unchecked")
    private static <T> void assertTopClasses(
        Map<String, Object> resultsObject,
    int numTopClasses,
    String dependentVariable,
    List<T> dependentVariableValues) {
        assertThat(resultsObject.containsKey("top_classes"), is(true));
        List<Map<String, Object>> topClasses = (List<Map<String, Object>>) resultsObject.get("top_classes");
        assertThat(topClasses, hasSize(numTopClasses));
        List<T> classNames = new ArrayList<>(topClasses.size());
        List<Double> classProbabilities = new ArrayList<>(topClasses.size());
        for (Map<String, Object> topClass : topClasses) {
            assertThat(topClass, allOf(hasKey("class_name"), hasKey("class_probability")));
            classNames.add((T) topClass.get("class_name"));
            classProbabilities.add((Double) topClass.get("class_probability"));
        }
        // Assert that all the predicted class names come from the set of dependent variable values.
        classNames.forEach(className -> assertThat(className, is(in(dependentVariableValues))));
        // Assert that the first class listed in top classes is the same as the predicted class.
        assertThat(classNames.get(0), equalTo(resultsObject.get(dependentVariable + "_prediction")));
        // Assert that all the class probabilities lie within [0, 1] interval.
        classProbabilities.forEach(p -> assertThat(p, allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(1.0))));
        // Assert that the top classes are listed in the order of decreasing probabilities.
        assertThat(Ordering.natural().reverse().isOrdered(classProbabilities), is(true));
    }

    private <T> void assertEvaluation(String dependentVariable, List<T> dependentVariableValues, String predictedClassField) {
        List<String> dependentVariableValuesAsStrings = dependentVariableValues.stream().map(String::valueOf).collect(toList());
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(
                destIndex,
                new org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Classification(
                    dependentVariable, predictedClassField, Arrays.asList(new Accuracy(), new MulticlassConfusionMatrix())));
        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics().size(), equalTo(2));

        {   // Accuracy
            Accuracy.Result accuracyResult = (Accuracy.Result) evaluateDataFrameResponse.getMetrics().get(0);
            assertThat(accuracyResult.getMetricName(), equalTo(Accuracy.NAME.getPreferredName()));
            List<Accuracy.ActualClass> actualClasses = accuracyResult.getActualClasses();
            assertThat(
                actualClasses.stream().map(Accuracy.ActualClass::getActualClass).collect(toList()),
                equalTo(dependentVariableValuesAsStrings));
            actualClasses.forEach(
                actualClass -> assertThat(actualClass.getAccuracy(), allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(1.0))));
        }

        {   // MulticlassConfusionMatrix
            MulticlassConfusionMatrix.Result confusionMatrixResult =
                (MulticlassConfusionMatrix.Result) evaluateDataFrameResponse.getMetrics().get(1);
            assertThat(confusionMatrixResult.getMetricName(), equalTo(MulticlassConfusionMatrix.NAME.getPreferredName()));
            List<MulticlassConfusionMatrix.ActualClass> actualClasses = confusionMatrixResult.getConfusionMatrix();
            assertThat(
                actualClasses.stream().map(MulticlassConfusionMatrix.ActualClass::getActualClass).collect(toList()),
                equalTo(dependentVariableValuesAsStrings));
            for (MulticlassConfusionMatrix.ActualClass actualClass : actualClasses) {
                assertThat(actualClass.getOtherPredictedClassDocCount(), equalTo(0L));
                assertThat(
                    actualClass.getPredictedClasses().stream()
                        .map(MulticlassConfusionMatrix.PredictedClass::getPredictedClass)
                        .collect(toList()),
                    equalTo(dependentVariableValuesAsStrings));
            }
            assertThat(confusionMatrixResult.getOtherActualClassCount(), equalTo(0L));
        }
    }

    protected String stateDocId() {
        return jobId + "_classification_state#1";
    }
}
