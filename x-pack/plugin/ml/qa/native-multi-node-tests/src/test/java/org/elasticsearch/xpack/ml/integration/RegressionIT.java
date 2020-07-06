/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.BoostedTreeParams;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.junit.After;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

public class RegressionIT extends MlNativeDataFrameAnalyticsIntegTestCase {

    private static final String NUMERICAL_FEATURE_FIELD = "feature";
    private static final String DISCRETE_NUMERICAL_FEATURE_FIELD = "discrete-feature";
    static final String DEPENDENT_VARIABLE_FIELD = "variable";
    private static final List<Double> NUMERICAL_FEATURE_VALUES = List.of(1.0, 2.0, 3.0);
    private static final List<Long> DISCRETE_NUMERICAL_FEATURE_VALUES = List.of(10L, 20L, 30L);
    private static final List<Double> DEPENDENT_VARIABLE_VALUES = List.of(10.0, 20.0, 30.0);

    private String jobId;
    private String sourceIndex;
    private String destIndex;

    @After
    public void cleanup() {
        cleanUp();
    }

    public void testSingleNumericFeatureAndMixedTrainingAndNonTrainingRows() throws Exception {
        initialize("regression_single_numeric_feature_and_mixed_data_set");
        String predictedClassField = DEPENDENT_VARIABLE_FIELD + "_prediction";
        indexData(sourceIndex, 300, 50);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null,
            new Regression(
                DEPENDENT_VARIABLE_FIELD,
                BoostedTreeParams.builder().setNumTopFeatureImportanceValues(1).build(),
                null,
                null,
                null,
                null,
                null)
        );
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            Map<String, Object> destDoc = getDestDoc(config, hit);
            Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(destDoc);

            // TODO reenable this assertion when the backend is stable
            // it seems for this case values can be as far off as 2.0

            // double featureValue = (double) destDoc.get(NUMERICAL_FEATURE_FIELD);
            // double predictionValue = (double) resultsObject.get(predictedClassField);
            // assertThat(predictionValue, closeTo(10 * featureValue, 2.0));

            assertThat(resultsObject.containsKey(predictedClassField), is(true));
            assertThat(resultsObject.containsKey("is_training"), is(true));
            assertThat(resultsObject.get("is_training"), is(destDoc.containsKey(DEPENDENT_VARIABLE_FIELD)));
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> importanceArray = (List<Map<String, Object>>)resultsObject.get("feature_importance");
            assertThat(importanceArray, hasSize(greaterThan(0)));
            assertThat(
                importanceArray.stream().filter(m -> NUMERICAL_FEATURE_FIELD.equals(m.get("feature_name"))
                    || DISCRETE_NUMERICAL_FEATURE_FIELD.equals(m.get("feature_name"))).findAny(),
                isPresent());
        }

        assertProgressComplete(jobId);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "double");
        assertThatAuditMessagesMatch(jobId,
            "Created analytics with analysis type [regression]",
            "Estimated memory usage for this analytics to be",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Started reindexing to destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis");
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsHundred() throws Exception {
        initialize("regression_only_training_data_and_training_percent_is_100");
        String predictedClassField = DEPENDENT_VARIABLE_FIELD + "_prediction";
        indexData(sourceIndex, 350, 0);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Regression(DEPENDENT_VARIABLE_FIELD));
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(getDestDoc(config, hit));

            assertThat(resultsObject.containsKey(predictedClassField), is(true));
            assertThat(resultsObject.containsKey("is_training"), is(true));
            assertThat(resultsObject.get("is_training"), is(true));
        }

        assertProgressComplete(jobId);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));

        GetDataFrameAnalyticsStatsAction.Response.Stats stats = getAnalyticsStats(jobId);
        assertThat(stats.getDataCounts().getJobId(), equalTo(jobId));
        assertThat(stats.getDataCounts().getTrainingDocsCount(), equalTo(350L));
        assertThat(stats.getDataCounts().getTestDocsCount(), equalTo(0L));
        assertThat(stats.getDataCounts().getSkippedDocsCount(), equalTo(0L));

        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "double");
        assertThatAuditMessagesMatch(jobId,
            "Created analytics with analysis type [regression]",
            "Estimated memory usage for this analytics to be",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Started reindexing to destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis");
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsFifty() throws Exception {
        initialize("regression_only_training_data_and_training_percent_is_50");
        String predictedClassField = DEPENDENT_VARIABLE_FIELD + "_prediction";
        indexData(sourceIndex, 350, 0);

        DataFrameAnalyticsConfig config =
            buildAnalytics(
                jobId,
                sourceIndex,
                destIndex,
                null,
                new Regression(DEPENDENT_VARIABLE_FIELD, BoostedTreeParams.builder().build(), null, 50.0, null, null, null));
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        int trainingRowsCount = 0;
        int nonTrainingRowsCount = 0;
        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(getDestDoc(config, hit));

            assertThat(resultsObject.containsKey(predictedClassField), is(true));
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

        GetDataFrameAnalyticsStatsAction.Response.Stats stats = getAnalyticsStats(jobId);
        assertThat(stats.getDataCounts().getJobId(), equalTo(jobId));
        assertThat(stats.getDataCounts().getTrainingDocsCount(), greaterThan(0L));
        assertThat(stats.getDataCounts().getTrainingDocsCount(), lessThan(350L));
        assertThat(stats.getDataCounts().getTestDocsCount(), greaterThan(0L));
        assertThat(stats.getDataCounts().getTestDocsCount(), lessThan(350L));
        assertThat(stats.getDataCounts().getSkippedDocsCount(), equalTo(0L));

        assertProgressComplete(jobId);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "double");
        assertThatAuditMessagesMatch(jobId,
            "Created analytics with analysis type [regression]",
            "Estimated memory usage for this analytics to be",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Started reindexing to destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis");
    }

    public void testStopAndRestart() throws Exception {
        initialize("regression_stop_and_restart");
        String predictedClassField = DEPENDENT_VARIABLE_FIELD + "_prediction";
        indexData(sourceIndex, 350, 0);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Regression(DEPENDENT_VARIABLE_FIELD));
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
            Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(getDestDoc(config, hit));

            assertThat(resultsObject.containsKey(predictedClassField), is(true));
            assertThat(resultsObject.containsKey("is_training"), is(true));
            assertThat(resultsObject.get("is_training"), is(true));
        }

        assertProgressComplete(jobId);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "double");
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/55807")
    public void testTwoJobsWithSameRandomizeSeedUseSameTrainingSet() throws Exception {
        String sourceIndex = "regression_two_jobs_with_same_randomize_seed_source";
        indexData(sourceIndex, 100, 0);

        String firstJobId = "regression_two_jobs_with_same_randomize_seed_1";
        String firstJobDestIndex = firstJobId + "_dest";

        BoostedTreeParams boostedTreeParams = BoostedTreeParams.builder()
            .setLambda(1.0)
            .setGamma(1.0)
            .setEta(1.0)
            .setFeatureBagFraction(1.0)
            .setMaxTrees(1)
            .build();

        DataFrameAnalyticsConfig firstJob = buildAnalytics(firstJobId, sourceIndex, firstJobDestIndex, null,
            new Regression(DEPENDENT_VARIABLE_FIELD, boostedTreeParams, null, 50.0, null, null, null));
        putAnalytics(firstJob);

        String secondJobId = "regression_two_jobs_with_same_randomize_seed_2";
        String secondJobDestIndex = secondJobId + "_dest";

        long randomizeSeed = ((Regression) firstJob.getAnalysis()).getRandomizeSeed();
        DataFrameAnalyticsConfig secondJob = buildAnalytics(secondJobId, sourceIndex, secondJobDestIndex, null,
            new Regression(DEPENDENT_VARIABLE_FIELD, boostedTreeParams, null, 50.0, randomizeSeed, null, null));

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

    public void testDeleteExpiredData_RemovesUnusedState() throws Exception {
        initialize("regression_delete_expired_data");
        String predictedClassField = DEPENDENT_VARIABLE_FIELD + "_prediction";
        indexData(sourceIndex, 100, 0);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Regression(DEPENDENT_VARIABLE_FIELD));
        putAnalytics(config);
        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        assertProgressComplete(jobId);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "double");

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

    public void testDependentVariableIsLong() throws Exception {
        initialize("regression_dependent_variable_is_long");
        String predictedClassField = DISCRETE_NUMERICAL_FEATURE_FIELD + "_prediction";
        indexData(sourceIndex, 100, 0);

        DataFrameAnalyticsConfig config =
            buildAnalytics(
                jobId,
                sourceIndex,
                destIndex,
                null,
                new Regression(DISCRETE_NUMERICAL_FEATURE_FIELD, BoostedTreeParams.builder().build(), null, null, null, null, null));
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);
        assertProgressComplete(jobId);

        assertMlResultsFieldMappings(destIndex, predictedClassField, "double");
    }

    public void testWithDatastream() throws Exception {
        assumeTrue("should only run if data streams are enabled", ActionModule.DATASTREAMS_FEATURE_ENABLED);
        initialize("regression_with_datastream");
        String predictedClassField = DEPENDENT_VARIABLE_FIELD + "_prediction";
        indexData(sourceIndex, 300, 50, true);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null,
            new Regression(
                DEPENDENT_VARIABLE_FIELD,
                BoostedTreeParams.builder().setNumTopFeatureImportanceValues(1).build(),
                null,
                null,
                null,
                null,
                null)
        );
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            Map<String, Object> destDoc = getDestDoc(config, hit);
            Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(destDoc);

            assertThat(resultsObject.containsKey(predictedClassField), is(true));
            assertThat(resultsObject.containsKey("is_training"), is(true));
            assertThat(resultsObject.get("is_training"), is(destDoc.containsKey(DEPENDENT_VARIABLE_FIELD)));
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> importanceArray = (List<Map<String, Object>>)resultsObject.get("feature_importance");
            assertThat(importanceArray, hasSize(greaterThan(0)));
            assertThat(
                importanceArray.stream().filter(m -> NUMERICAL_FEATURE_FIELD.equals(m.get("feature_name"))
                    || DISCRETE_NUMERICAL_FEATURE_FIELD.equals(m.get("feature_name"))).findAny(),
                isPresent());
        }

        assertProgressComplete(jobId);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "double");
        assertThatAuditMessagesMatch(jobId,
            "Created analytics with analysis type [regression]",
            "Estimated memory usage for this analytics to be",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Started reindexing to destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis");
    }

    private void initialize(String jobId) {
        this.jobId = jobId;
        this.sourceIndex = jobId + "_source_index";
        this.destIndex = sourceIndex + "_results";
    }

    static void indexData(String sourceIndex, int numTrainingRows, int numNonTrainingRows) {
        indexData(sourceIndex, numTrainingRows, numNonTrainingRows, false);
    }

    static void indexData(String sourceIndex, int numTrainingRows, int numNonTrainingRows, boolean dataStream) {
        String mapping = "{\n" +
            "      \"properties\": {\n" +
            "        \"time\": {\n" +
            "          \"type\": \"date\"\n" +
            "        }," +
            "        \""+ NUMERICAL_FEATURE_FIELD + "\": {\n" +
            "          \"type\": \"double\"\n" +
            "        }," +
            "        \"" + DISCRETE_NUMERICAL_FEATURE_FIELD + "\": {\n" +
            "          \"type\": \"long\"\n" +
            "        }," +
            "        \"" + DEPENDENT_VARIABLE_FIELD + "\": {\n" +
            "          \"type\": \"double\"\n" +
            "        }" +
            "      }\n" +
            "    }";
        if (dataStream) {
            try {
                createDataStreamAndTemplate(sourceIndex, "time", mapping);
            } catch (IOException ex) {
                throw new ElasticsearchException(ex);
            }
        } else {
            client().admin().indices().prepareCreate(sourceIndex)
                .setMapping(mapping)
                .get();
        }

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < numTrainingRows; i++) {
            List<Object> source = List.of(
                NUMERICAL_FEATURE_FIELD, NUMERICAL_FEATURE_VALUES.get(i % NUMERICAL_FEATURE_VALUES.size()),
                DISCRETE_NUMERICAL_FEATURE_FIELD, DISCRETE_NUMERICAL_FEATURE_VALUES.get(i % DISCRETE_NUMERICAL_FEATURE_VALUES.size()),
                DEPENDENT_VARIABLE_FIELD, DEPENDENT_VARIABLE_VALUES.get(i % DEPENDENT_VARIABLE_VALUES.size()),
                "time", Instant.now().toEpochMilli());
            IndexRequest indexRequest = new IndexRequest(sourceIndex).source(source.toArray()).opType(DocWriteRequest.OpType.CREATE);
            bulkRequestBuilder.add(indexRequest);
        }
        for (int i = numTrainingRows; i < numTrainingRows + numNonTrainingRows; i++) {
            List<Object> source = List.of(
                NUMERICAL_FEATURE_FIELD, NUMERICAL_FEATURE_VALUES.get(i % NUMERICAL_FEATURE_VALUES.size()),
                DISCRETE_NUMERICAL_FEATURE_FIELD, DISCRETE_NUMERICAL_FEATURE_VALUES.get(i % DISCRETE_NUMERICAL_FEATURE_VALUES.size()),
                "time", Instant.now().toEpochMilli());
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
            assertThat(destDoc.containsKey(field), is(true));
            assertThat(destDoc.get(field), equalTo(sourceDoc.get(field)));
        }
        return destDoc;
    }

    private static Map<String, Object> getMlResultsObjectFromDestDoc(Map<String, Object> destDoc) {
        return getFieldValue(destDoc, "ml");
    }

    protected String stateDocId() {
        return jobId + "_regression_state#1";
    }
}
