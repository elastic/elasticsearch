/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.ml.action.EvaluateDataFrameAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.BoostedTreeParams;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Accuracy;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Precision;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Recall;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.core.ml.MlTasks.AWAITING_UPGRADE;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class ClassificationIT extends MlNativeDataFrameAnalyticsIntegTestCase {

    private static final String BOOLEAN_FIELD = "boolean-field";
    private static final String NUMERICAL_FIELD = "numerical-field";
    private static final String DISCRETE_NUMERICAL_FIELD = "discrete-numerical-field";
    private static final String TEXT_FIELD = "text-field";
    private static final String KEYWORD_FIELD = "keyword-field";
    private static final String NESTED_FIELD = "outer-field.inner-field";
    private static final String ALIAS_TO_KEYWORD_FIELD = "alias-to-keyword-field";
    private static final String ALIAS_TO_NESTED_FIELD = "alias-to-nested-field";
    private static final List<Boolean> BOOLEAN_FIELD_VALUES = List.of(false, true);
    private static final List<Double> NUMERICAL_FIELD_VALUES = List.of(1.0, 2.0);
    private static final List<Integer> DISCRETE_NUMERICAL_FIELD_VALUES = List.of(10, 20);
    private static final List<String> KEYWORD_FIELD_VALUES = List.of("cat", "dog");

    private String jobId;
    private String sourceIndex;
    private String destIndex;
    private boolean analysisUsesExistingDestIndex;

    @After
    public void cleanup() {
        cleanUp();
    }

    public void testSingleNumericFeatureAndMixedTrainingAndNonTrainingRows() throws Exception {
        initialize("classification_single_numeric_feature_and_mixed_data_set");
        String predictedClassField = KEYWORD_FIELD + "_prediction";
        indexData(sourceIndex, 300, 50, KEYWORD_FIELD);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null,
            new Classification(
                KEYWORD_FIELD,
                BoostedTreeParams.builder().setNumTopFeatureImportanceValues(1).build(),
                null,
                null,
                null,
                null,
                null));
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        client().admin().indices().refresh(new RefreshRequest(destIndex));
        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            Map<String, Object> destDoc = getDestDoc(config, hit);
            Map<String, Object> resultsObject = getFieldValue(destDoc, "ml");
            assertThat(getFieldValue(resultsObject, predictedClassField), is(in(KEYWORD_FIELD_VALUES)));
            assertThat(getFieldValue(resultsObject, "is_training"), is(destDoc.containsKey(KEYWORD_FIELD)));
            assertTopClasses(resultsObject, 2, KEYWORD_FIELD, KEYWORD_FIELD_VALUES);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> importanceArray = (List<Map<String, Object>>)resultsObject.get("feature_importance");
            assertThat(importanceArray, hasSize(greaterThan(0)));
        }

        assertProgressComplete(jobId);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "keyword");
        assertThatAuditMessagesMatch(jobId,
            "Created analytics with analysis type [classification]",
            "Estimated memory usage for this analytics to be",
            "Starting analytics on node",
            "Started analytics",
            expectedDestIndexAuditMessage(),
            "Started reindexing to destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis");
        assertEvaluation(KEYWORD_FIELD, KEYWORD_FIELD_VALUES, "ml." + predictedClassField);
    }

    public void testWithDatastreams() throws Exception {
        assumeTrue("should only run if data streams are enabled", ActionModule.DATASTREAMS_FEATURE_ENABLED);
        initialize("classification_with_datastreams", true);
        String predictedClassField = KEYWORD_FIELD + "_prediction";
        indexData(sourceIndex, 300, 50, KEYWORD_FIELD);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null,
            new Classification(
                KEYWORD_FIELD,
                BoostedTreeParams.builder().setNumTopFeatureImportanceValues(1).build(),
                null,
                null,
                null,
                null,
                null));
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        client().admin().indices().refresh(new RefreshRequest(destIndex));
        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            Map<String, Object> destDoc = getDestDoc(config, hit);
            Map<String, Object> resultsObject = getFieldValue(destDoc, "ml");
            assertThat(getFieldValue(resultsObject, predictedClassField), is(in(KEYWORD_FIELD_VALUES)));
            assertThat(getFieldValue(resultsObject, "is_training"), is(destDoc.containsKey(KEYWORD_FIELD)));
            assertTopClasses(resultsObject, 2, KEYWORD_FIELD, KEYWORD_FIELD_VALUES);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> importanceArray = (List<Map<String, Object>>)resultsObject.get("feature_importance");
            assertThat(importanceArray, hasSize(greaterThan(0)));
        }

        assertProgressComplete(jobId);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "keyword");
        assertThatAuditMessagesMatch(jobId,
            "Created analytics with analysis type [classification]",
            "Estimated memory usage for this analytics to be",
            "Starting analytics on node",
            "Started analytics",
            expectedDestIndexAuditMessage(),
            "Started reindexing to destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis");
        assertEvaluation(KEYWORD_FIELD, KEYWORD_FIELD_VALUES, "ml." + predictedClassField);
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsHundred() throws Exception {
        initialize("classification_only_training_data_and_training_percent_is_100");
        String predictedClassField = KEYWORD_FIELD + "_prediction";
        indexData(sourceIndex, 300, 0, KEYWORD_FIELD);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Classification(KEYWORD_FIELD));
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        client().admin().indices().refresh(new RefreshRequest(destIndex));
        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            Map<String, Object> destDoc = getDestDoc(config, hit);
            Map<String, Object> resultsObject = getFieldValue(destDoc, "ml");
            assertThat(getFieldValue(resultsObject, predictedClassField), is(in(KEYWORD_FIELD_VALUES)));
            assertThat(getFieldValue(resultsObject, "is_training"), is(true));
            assertTopClasses(resultsObject, 2, KEYWORD_FIELD, KEYWORD_FIELD_VALUES);
        }

        GetDataFrameAnalyticsStatsAction.Response.Stats stats = getAnalyticsStats(jobId);
        assertThat(stats.getDataCounts().getJobId(), equalTo(jobId));
        assertThat(stats.getDataCounts().getTrainingDocsCount(), equalTo(300L));
        assertThat(stats.getDataCounts().getTestDocsCount(), equalTo(0L));
        assertThat(stats.getDataCounts().getSkippedDocsCount(), equalTo(0L));

        assertProgressComplete(jobId);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "keyword");
        assertThatAuditMessagesMatch(jobId,
            "Created analytics with analysis type [classification]",
            "Estimated memory usage for this analytics to be",
            "Starting analytics on node",
            "Started analytics",
            expectedDestIndexAuditMessage(),
            "Started reindexing to destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis");
        assertEvaluation(KEYWORD_FIELD, KEYWORD_FIELD_VALUES, "ml." + predictedClassField);
    }

    public <T> void testWithOnlyTrainingRowsAndTrainingPercentIsFifty(String jobId,
                                                                      String dependentVariable,
                                                                      List<T> dependentVariableValues,
                                                                      String expectedMappingTypeForPredictedField) throws Exception {
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
                new Classification(dependentVariable, BoostedTreeParams.builder().build(), null, null, numTopClasses, 50.0, null));
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        int trainingRowsCount = 0;
        int nonTrainingRowsCount = 0;
        client().admin().indices().refresh(new RefreshRequest(destIndex));
        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            Map<String, Object> destDoc = getDestDoc(config, hit);
            Map<String, Object> resultsObject = getFieldValue(destDoc, "ml");
            assertThat(getFieldValue(resultsObject, predictedClassField), is(in(dependentVariableValues)));
            assertTopClasses(resultsObject, numTopClasses, dependentVariable, dependentVariableValues);

            // Let's just assert there's both training and non-training results
            //
            boolean isTraining = getFieldValue(resultsObject, "is_training");
            if (isTraining) {
                trainingRowsCount++;
            } else {
                nonTrainingRowsCount++;
            }
        }
        assertThat(trainingRowsCount, greaterThan(0));
        assertThat(nonTrainingRowsCount, greaterThan(0));

        GetDataFrameAnalyticsStatsAction.Response.Stats stats = getAnalyticsStats(jobId);
        assertThat(stats.getDataCounts().getJobId(), equalTo(jobId));
        assertThat(stats.getDataCounts().getTrainingDocsCount(), greaterThan(0L));
        assertThat(stats.getDataCounts().getTrainingDocsCount(), lessThan(300L));
        assertThat(stats.getDataCounts().getTestDocsCount(), greaterThan(0L));
        assertThat(stats.getDataCounts().getTestDocsCount(), lessThan(300L));
        assertThat(stats.getDataCounts().getSkippedDocsCount(), equalTo(0L));

        assertProgressComplete(jobId);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, expectedMappingTypeForPredictedField);
        assertThatAuditMessagesMatch(jobId,
            "Created analytics with analysis type [classification]",
            "Estimated memory usage for this analytics to be",
            "Starting analytics on node",
            "Started analytics",
            expectedDestIndexAuditMessage(),
            "Started reindexing to destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis");
        assertEvaluation(dependentVariable, dependentVariableValues, "ml." + predictedClassField);
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsFifty_DependentVariableIsKeyword() throws Exception {
        testWithOnlyTrainingRowsAndTrainingPercentIsFifty(
            "classification_training_percent_is_50_keyword", KEYWORD_FIELD, KEYWORD_FIELD_VALUES, "keyword");
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsFifty_DependentVariableIsInteger() throws Exception {
        testWithOnlyTrainingRowsAndTrainingPercentIsFifty(
            "classification_training_percent_is_50_integer", DISCRETE_NUMERICAL_FIELD, DISCRETE_NUMERICAL_FIELD_VALUES, "integer");
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsFifty_DependentVariableIsDouble() throws Exception {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> testWithOnlyTrainingRowsAndTrainingPercentIsFifty(
                "classification_training_percent_is_50_double", NUMERICAL_FIELD, NUMERICAL_FIELD_VALUES, null));
        assertThat(e.getMessage(), startsWith("invalid types [double] for required field [numerical-field];"));
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsFifty_DependentVariableIsText() throws Exception {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> testWithOnlyTrainingRowsAndTrainingPercentIsFifty(
                "classification_training_percent_is_50_text", TEXT_FIELD, KEYWORD_FIELD_VALUES, null));
        assertThat(e.getMessage(), startsWith("field [text-field] of type [text] is non-aggregatable"));
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsFifty_DependentVariableIsBoolean() throws Exception {
        testWithOnlyTrainingRowsAndTrainingPercentIsFifty(
            "classification_training_percent_is_50_boolean", BOOLEAN_FIELD, BOOLEAN_FIELD_VALUES, "boolean");
    }

    public void testStopAndRestart() throws Exception {
        initialize("classification_stop_and_restart");
        String predictedClassField = KEYWORD_FIELD + "_prediction";
        indexData(sourceIndex, 350, 0, KEYWORD_FIELD);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Classification(KEYWORD_FIELD));
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        NodeAcknowledgedResponse response = startAnalytics(jobId);
        assertThat(response.getNode(), not(emptyString()));

        // Wait until state is one of REINDEXING or ANALYZING, or until it is STOPPED.
        assertBusy(() -> {
            DataFrameAnalyticsState state = getAnalyticsStats(jobId).getState();
            assertThat(
                state,
                is(anyOf(
                    equalTo(DataFrameAnalyticsState.REINDEXING),
                    equalTo(DataFrameAnalyticsState.ANALYZING),
                    equalTo(DataFrameAnalyticsState.STOPPED))));
        });
        stopAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        // Now let's start it again
        try {
            response = startAnalytics(jobId);
            assertThat(response.getNode(), not(emptyString()));
        } catch (Exception e) {
            if (e.getMessage().equals("Cannot start because the job has already finished")) {
                // That means the job had managed to complete
            } else {
                throw e;
            }
        }

        waitUntilAnalyticsIsStopped(jobId, TimeValue.timeValueMinutes(1));

        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            Map<String, Object> destDoc = getDestDoc(config, hit);
            Map<String, Object> resultsObject = getFieldValue(destDoc, "ml");
            assertThat(getFieldValue(resultsObject, predictedClassField), is(in(KEYWORD_FIELD_VALUES)));
            assertThat(getFieldValue(resultsObject, "is_training"), is(true));
            assertTopClasses(resultsObject, 2, KEYWORD_FIELD, KEYWORD_FIELD_VALUES);
        }

        assertProgressComplete(jobId);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "keyword");
        assertEvaluation(KEYWORD_FIELD, KEYWORD_FIELD_VALUES, "ml." + predictedClassField);
    }

    public void testDependentVariableCardinalityTooHighError() throws Exception {
        initialize("cardinality_too_high");
        indexData(sourceIndex, 6, 5, KEYWORD_FIELD);

        // Index enough documents to have more classes than the allowed limit
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < Classification.MAX_DEPENDENT_VARIABLE_CARDINALITY - 1; i++) {
            IndexRequest indexRequest = new IndexRequest(sourceIndex).source(KEYWORD_FIELD, "fox-" + i);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Classification(KEYWORD_FIELD));
        putAnalytics(config);

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> startAnalytics(jobId));
        assertThat(e.status().getStatus(), equalTo(400));
        assertThat(e.getMessage(), equalTo("Field [keyword-field] must have at most [30] distinct values but there were at least [31]"));
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
        putAnalytics(config);

        // Should not throw
        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        assertProgressComplete(jobId);
    }

    public void testDependentVariableIsNested() throws Exception {
        initialize("dependent_variable_is_nested");
        String predictedClassField = NESTED_FIELD + "_prediction";
        indexData(sourceIndex, 100, 0, NESTED_FIELD);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Classification(NESTED_FIELD));
        putAnalytics(config);
        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        assertProgressComplete(jobId);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "keyword");
        assertEvaluation(NESTED_FIELD, KEYWORD_FIELD_VALUES, "ml." + predictedClassField);
    }

    public void testDependentVariableIsAliasToKeyword() throws Exception {
        initialize("dependent_variable_is_alias");
        String predictedClassField = ALIAS_TO_KEYWORD_FIELD + "_prediction";
        indexData(sourceIndex, 100, 0, KEYWORD_FIELD);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Classification(ALIAS_TO_KEYWORD_FIELD));
        putAnalytics(config);
        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        assertProgressComplete(jobId);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "keyword");
        assertEvaluation(ALIAS_TO_KEYWORD_FIELD, KEYWORD_FIELD_VALUES, "ml." + predictedClassField);
    }

    public void testDependentVariableIsAliasToNested() throws Exception {
        initialize("dependent_variable_is_alias_to_nested");
        String predictedClassField = ALIAS_TO_NESTED_FIELD + "_prediction";
        indexData(sourceIndex, 100, 0, NESTED_FIELD);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Classification(ALIAS_TO_NESTED_FIELD));
        putAnalytics(config);
        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        assertProgressComplete(jobId);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "keyword");
        assertEvaluation(ALIAS_TO_NESTED_FIELD, KEYWORD_FIELD_VALUES, "ml." + predictedClassField);
    }

    public void testTwoJobsWithSameRandomizeSeedUseSameTrainingSet() throws Exception {
        String sourceIndex = "classification_two_jobs_with_same_randomize_seed_source";
        String dependentVariable = KEYWORD_FIELD;

        createIndex(sourceIndex, false);
        // We use 100 rows as we can't set this too low. If too low it is possible
        // we only train with rows of one of the two classes which leads to a failure.
        indexData(sourceIndex, 100, 0, dependentVariable);

        String firstJobId = "classification_two_jobs_with_same_randomize_seed_1";
        String firstJobDestIndex = firstJobId + "_dest";

        BoostedTreeParams boostedTreeParams = BoostedTreeParams.builder()
            .setLambda(1.0)
            .setGamma(1.0)
            .setEta(1.0)
            .setFeatureBagFraction(1.0)
            .setMaxTrees(1)
            .build();

        DataFrameAnalyticsConfig firstJob = buildAnalytics(firstJobId, sourceIndex, firstJobDestIndex, null,
            new Classification(dependentVariable, boostedTreeParams, null, null, 1, 50.0, null));
        putAnalytics(firstJob);

        String secondJobId = "classification_two_jobs_with_same_randomize_seed_2";
        String secondJobDestIndex = secondJobId + "_dest";

        long randomizeSeed = ((Classification) firstJob.getAnalysis()).getRandomizeSeed();
        DataFrameAnalyticsConfig secondJob = buildAnalytics(secondJobId, sourceIndex, secondJobDestIndex, null,
            new Classification(dependentVariable, boostedTreeParams, null, null, 1, 50.0, randomizeSeed));

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

    public void testSetUpgradeMode_ExistingTaskGetsUnassigned() throws Exception {
        initialize("classification_set_upgrade_mode");
        indexData(sourceIndex, 300, 0, KEYWORD_FIELD);

        assertThat(upgradeMode(), is(false));

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Classification(KEYWORD_FIELD));
        putAnalytics(config);
        startAnalytics(jobId);
        assertThat(analyticsTaskList(), hasSize(1));
        assertThat(analyticsAssignedTaskList(), hasSize(1));

        setUpgradeModeTo(true);
        assertThat(analyticsTaskList(), hasSize(1));
        assertThat(analyticsAssignedTaskList(), is(empty()));

        assertBusy(() -> {
            try {
                GetDataFrameAnalyticsStatsAction.Response.Stats analyticsStats = getAnalyticsStats(jobId);
                assertThat(analyticsStats.getAssignmentExplanation(), is(equalTo(AWAITING_UPGRADE.getExplanation())));
                assertThat(analyticsStats.getNode(), is(nullValue()));
            } catch (ElasticsearchException e) {
                logger.error(new ParameterizedMessage("[{}] Encountered exception while fetching analytics stats", jobId), e);
                fail(e.getDetailedMessage());
            }
        });

        setUpgradeModeTo(false);
        assertThat(analyticsTaskList(), hasSize(1));
        assertBusy(() -> assertThat(analyticsAssignedTaskList(), hasSize(1)));

        assertBusy(() -> {
            try {
                GetDataFrameAnalyticsStatsAction.Response.Stats analyticsStats = getAnalyticsStats(jobId);
                assertThat(analyticsStats.getAssignmentExplanation(), is(not(equalTo(AWAITING_UPGRADE.getExplanation()))));
            } catch (ElasticsearchException e) {
                logger.error(new ParameterizedMessage("[{}] Encountered exception while fetching analytics stats", jobId), e);
                fail(e.getDetailedMessage());
            }
        });

        waitUntilAnalyticsIsStopped(jobId);
        assertProgressComplete(jobId);
    }

    public void testSetUpgradeMode_NewTaskDoesNotStart() throws Exception {
        initialize("classification_set_upgrade_mode_task_should_not_start");
        indexData(sourceIndex, 100, 0, KEYWORD_FIELD);

        assertThat(upgradeMode(), is(false));

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Classification(KEYWORD_FIELD));
        putAnalytics(config);

        setUpgradeModeTo(true);

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> startAnalytics(config.getId()));
        assertThat(e.status(), is(equalTo(RestStatus.TOO_MANY_REQUESTS)));
        assertThat(
            e.getMessage(),
            is(equalTo("Cannot perform cluster:admin/xpack/ml/data_frame/analytics/start action while upgrade mode is enabled")));

        assertThat(analyticsTaskList(), is(empty()));
        assertThat(analyticsAssignedTaskList(), is(empty()));
    }

    public void testDeleteExpiredData_RemovesUnusedState() throws Exception {
        initialize("classification_delete_expired_data");
        indexData(sourceIndex, 100, 0, KEYWORD_FIELD);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Classification(KEYWORD_FIELD));
        putAnalytics(config);
        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        assertProgressComplete(jobId);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);

        // Call _delete_expired_data API and check nothing was deleted
        assertThat(deleteExpiredData().isDeleted(), is(true));
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());

        // Delete the config straight from the config index
        DeleteResponse deleteResponse = client().prepareDelete(".ml-config", DataFrameAnalyticsConfig.documentId(jobId))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).execute().actionGet();
        assertThat(deleteResponse.status(), equalTo(RestStatus.OK));

        // Now calling the _delete_expired_data API should remove unused state
        assertThat(deleteExpiredData().isDeleted(), is(true));

        SearchResponse stateIndexSearchResponse = client().prepareSearch(".ml-state*").execute().actionGet();
        assertThat(stateIndexSearchResponse.getHits().getTotalHits().value, equalTo(0L));
    }

    private void initialize(String jobId) {
        initialize(jobId, false);
    }

    private void initialize(String jobId, boolean isDatastream) {
        this.jobId = jobId;
        this.sourceIndex = jobId + "_source_index";
        this.destIndex = sourceIndex + "_results";
        this.analysisUsesExistingDestIndex = randomBoolean();
        createIndex(sourceIndex, isDatastream);
        if (analysisUsesExistingDestIndex) {
            createIndex(destIndex, false);
        }
    }

    private static void createIndex(String index, boolean isDatastream) {
        String mapping = "{\n" +
            "      \"properties\": {\n" +
            "        \"time\": {\n" +
            "          \"type\": \"date\"\n" +
            "        }," +
            "        \""+ BOOLEAN_FIELD + "\": {\n" +
            "          \"type\": \"boolean\"\n" +
            "        }," +
            "        \""+ NUMERICAL_FIELD + "\": {\n" +
            "          \"type\": \"double\"\n" +
            "        }," +
            "        \""+ DISCRETE_NUMERICAL_FIELD + "\": {\n" +
            "          \"type\": \"integer\"\n" +
            "        }," +
            "        \""+ TEXT_FIELD + "\": {\n" +
            "          \"type\": \"text\"\n" +
            "        }," +
            "        \""+ KEYWORD_FIELD + "\": {\n" +
            "          \"type\": \"keyword\"\n" +
            "        }," +
            "        \""+ NESTED_FIELD + "\": {\n" +
            "          \"type\": \"keyword\"\n" +
            "        }," +
            "        \""+ ALIAS_TO_KEYWORD_FIELD + "\": {\n" +
            "          \"type\": \"alias\",\n" +
            "          \"path\": \"" + KEYWORD_FIELD + "\"\n" +
            "        }," +
            "        \""+ ALIAS_TO_NESTED_FIELD + "\": {\n" +
            "          \"type\": \"alias\",\n" +
            "          \"path\": \"" + NESTED_FIELD + "\"\n" +
            "        }" +
            "      }\n" +
            "    }";
        if (isDatastream) {
            try {
                createDataStreamAndTemplate(index, "time", mapping);
            } catch (IOException ex) {
                throw new ElasticsearchException(ex);
            }
        } else {
            client().admin().indices().prepareCreate(index)
                .setMapping(mapping)
                .get();
        }
    }

    private static void indexData(String sourceIndex, int numTrainingRows, int numNonTrainingRows, String dependentVariable) {
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < numTrainingRows; i++) {
            List<Object> source = List.of(
                BOOLEAN_FIELD, BOOLEAN_FIELD_VALUES.get(i % BOOLEAN_FIELD_VALUES.size()),
                NUMERICAL_FIELD, NUMERICAL_FIELD_VALUES.get(i % NUMERICAL_FIELD_VALUES.size()),
                DISCRETE_NUMERICAL_FIELD, DISCRETE_NUMERICAL_FIELD_VALUES.get(i % DISCRETE_NUMERICAL_FIELD_VALUES.size()),
                TEXT_FIELD, KEYWORD_FIELD_VALUES.get(i % KEYWORD_FIELD_VALUES.size()),
                KEYWORD_FIELD, KEYWORD_FIELD_VALUES.get(i % KEYWORD_FIELD_VALUES.size()),
                NESTED_FIELD, KEYWORD_FIELD_VALUES.get(i % KEYWORD_FIELD_VALUES.size()));
            IndexRequest indexRequest = new IndexRequest(sourceIndex).source(source.toArray()).opType(DocWriteRequest.OpType.CREATE);
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
            if (TEXT_FIELD.equals(dependentVariable) == false) {
                source.addAll(List.of(TEXT_FIELD, KEYWORD_FIELD_VALUES.get(i % KEYWORD_FIELD_VALUES.size())));
            }
            if (KEYWORD_FIELD.equals(dependentVariable) == false) {
                source.addAll(List.of(KEYWORD_FIELD, KEYWORD_FIELD_VALUES.get(i % KEYWORD_FIELD_VALUES.size())));
            }
            if (NESTED_FIELD.equals(dependentVariable) == false) {
                source.addAll(List.of(NESTED_FIELD, KEYWORD_FIELD_VALUES.get(i % KEYWORD_FIELD_VALUES.size())));
            }
            IndexRequest indexRequest = new IndexRequest(sourceIndex).source(source.toArray()).opType(DocWriteRequest.OpType.CREATE);
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
            assertThat(destDoc, hasKey(field));
            assertThat(destDoc.get(field), equalTo(sourceDoc.get(field)));
        }
        return destDoc;
    }

    private static <T> void assertTopClasses(Map<String, Object> resultsObject,
                                             int numTopClasses,
                                             String dependentVariable,
                                             List<T> dependentVariableValues) {
        List<Map<String, Object>> topClasses = getFieldValue(resultsObject, "top_classes");
        assertThat(topClasses, hasSize(numTopClasses));
        List<T> classNames = new ArrayList<>(topClasses.size());
        List<Double> classProbabilities = new ArrayList<>(topClasses.size());
        List<Double> classScores = new ArrayList<>(topClasses.size());
        for (Map<String, Object> topClass : topClasses) {
            classNames.add(getFieldValue(topClass, "class_name"));
            classProbabilities.add(getFieldValue(topClass, "class_probability"));
            classScores.add(getFieldValue(topClass, "class_score"));
        }
        // Assert that all the predicted class names come from the set of dependent variable values.
        classNames.forEach(className -> assertThat(className, is(in(dependentVariableValues))));
        // Assert that the first class listed in top classes is the same as the predicted class.
        assertThat(classNames.get(0), equalTo(resultsObject.get(dependentVariable + "_prediction")));
        // Assert that all the class probabilities lie within [0, 1] interval.
        classProbabilities.forEach(p -> assertThat(p, allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(1.0))));
        // Assert that the top classes are listed in the order of decreasing scores.
        double prevScore = classScores.get(0);
        for (int i = 1; i < classScores.size(); ++i) {
            double score = classScores.get(i);
            assertThat("class " + i, score, lessThanOrEqualTo(prevScore));
        }
    }

    private <T> void assertEvaluation(String dependentVariable, List<T> dependentVariableValues, String predictedClassField) {
        List<String> dependentVariableValuesAsStrings = dependentVariableValues.stream().map(String::valueOf).collect(toList());
        EvaluateDataFrameAction.Response evaluateDataFrameResponse =
            evaluateDataFrame(
                destIndex,
                new org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Classification(
                    dependentVariable,
                    predictedClassField,
                    Arrays.asList(new Accuracy(), new MulticlassConfusionMatrix(), new Precision(), new Recall())));
        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME.getPreferredName()));
        assertThat(evaluateDataFrameResponse.getMetrics().size(), equalTo(4));

        {   // Accuracy
            Accuracy.Result accuracyResult = (Accuracy.Result) evaluateDataFrameResponse.getMetrics().get(0);
            assertThat(accuracyResult.getMetricName(), equalTo(Accuracy.NAME.getPreferredName()));
            for (Accuracy.PerClassResult klass : accuracyResult.getClasses()) {
                assertThat(klass.getClassName(), is(in(dependentVariableValuesAsStrings)));
                assertThat(klass.getAccuracy(), allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(1.0)));
            }
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

        {   // Precision
            Precision.Result precisionResult = (Precision.Result) evaluateDataFrameResponse.getMetrics().get(2);
            assertThat(precisionResult.getMetricName(), equalTo(Precision.NAME.getPreferredName()));
            for (Precision.PerClassResult klass : precisionResult.getClasses()) {
                assertThat(klass.getClassName(), is(in(dependentVariableValuesAsStrings)));
                assertThat(klass.getPrecision(), allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(1.0)));
            }
        }

        {   // Recall
            Recall.Result recallResult = (Recall.Result) evaluateDataFrameResponse.getMetrics().get(3);
            assertThat(recallResult.getMetricName(), equalTo(Recall.NAME.getPreferredName()));
            for (Recall.PerClassResult klass : recallResult.getClasses()) {
                assertThat(klass.getClassName(), is(in(dependentVariableValuesAsStrings)));
                assertThat(klass.getRecall(), allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(1.0)));
            }
        }
    }

    private String stateDocId() {
        return jobId + "_classification_state#1";
    }

    private String expectedDestIndexAuditMessage() {
        return (analysisUsesExistingDestIndex ? "Using existing" : "Creating") + " destination index [" + destIndex + "]";
    }
}
