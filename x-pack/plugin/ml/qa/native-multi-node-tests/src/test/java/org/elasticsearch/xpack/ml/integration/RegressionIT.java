/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.BoostedTreeParams;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.BoostedTreeParamsTests;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.junit.After;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class RegressionIT extends MlNativeDataFrameAnalyticsIntegTestCase {

    private static final String NUMERICAL_FEATURE_FIELD = "feature";
    private static final String DEPENDENT_VARIABLE_FIELD = "variable";
    private static final List<Double> NUMERICAL_FEATURE_VALUES = Collections.unmodifiableList(Arrays.asList(1.0, 2.0, 3.0));
    private static final List<Double> DEPENDENT_VARIABLE_VALUES = Collections.unmodifiableList(Arrays.asList(10.0, 20.0, 30.0));

    private String jobId;
    private String sourceIndex;
    private String destIndex;

    @After
    public void cleanup() {
        cleanUp();
    }

    public void testSingleNumericFeatureAndMixedTrainingAndNonTrainingRows() throws Exception {
        initialize("regression_single_numeric_feature_and_mixed_data_set");
        indexData(sourceIndex, 300, 50);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Regression(DEPENDENT_VARIABLE_FIELD));
        registerAnalytics(config);
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgress(jobId, 0, 0, 0, 0);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            Map<String, Object> destDoc = getDestDoc(config, hit);
            Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(destDoc);

            // TODO reenable this assertion when the backend is stable
            // it seems for this case values can be as far off as 2.0

            // double featureValue = (double) destDoc.get(NUMERICAL_FEATURE_FIELD);
            // double predictionValue = (double) resultsObject.get("variable_prediction");
            // assertThat(predictionValue, closeTo(10 * featureValue, 2.0));

            assertThat(resultsObject.containsKey("variable_prediction"), is(true));
            assertThat(resultsObject.containsKey("is_training"), is(true));
            assertThat(resultsObject.get("is_training"), is(destDoc.containsKey(DEPENDENT_VARIABLE_FIELD)));
        }

        assertProgress(jobId, 100, 100, 100, 100);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertThatAuditMessagesMatch(jobId,
            "Created analytics with analysis type [regression]",
            "Estimated memory usage for this analytics to be",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Finished analysis");
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsHundred() throws Exception {
        initialize("regression_only_training_data_and_training_percent_is_100");
        indexData(sourceIndex, 350, 0);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Regression(DEPENDENT_VARIABLE_FIELD));
        registerAnalytics(config);
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgress(jobId, 0, 0, 0, 0);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(getDestDoc(config, hit));

            assertThat(resultsObject.containsKey("variable_prediction"), is(true));
            assertThat(resultsObject.containsKey("is_training"), is(true));
            assertThat(resultsObject.get("is_training"), is(true));
        }

        assertProgress(jobId, 100, 100, 100, 100);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
        assertThatAuditMessagesMatch(jobId,
            "Created analytics with analysis type [regression]",
            "Estimated memory usage for this analytics to be",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Finished analysis");
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsFifty() throws Exception {
        initialize("regression_only_training_data_and_training_percent_is_50");
        indexData(sourceIndex, 350, 0);

        DataFrameAnalyticsConfig config =
            buildAnalytics(
                jobId,
                sourceIndex,
                destIndex,
                null,
                new Regression(DEPENDENT_VARIABLE_FIELD, BoostedTreeParamsTests.createRandom(), null, 50.0, null));
        registerAnalytics(config);
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgress(jobId, 0, 0, 0, 0);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        int trainingRowsCount = 0;
        int nonTrainingRowsCount = 0;
        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(getDestDoc(config, hit));

            assertThat(resultsObject.containsKey("variable_prediction"), is(true));
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
            "Created analytics with analysis type [regression]",
            "Estimated memory usage for this analytics to be",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Finished analysis");
    }

    public void testStopAndRestart() throws Exception {
        initialize("regression_stop_and_restart");
        indexData(sourceIndex, 350, 0);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Regression(DEPENDENT_VARIABLE_FIELD));
        registerAnalytics(config);
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgress(jobId, 0, 0, 0, 0);

        startAnalytics(jobId);

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
            startAnalytics(jobId);
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

            assertThat(resultsObject.containsKey("variable_prediction"), is(true));
            assertThat(resultsObject.containsKey("is_training"), is(true));
            assertThat(resultsObject.get("is_training"), is(true));
        }

        assertProgress(jobId, 100, 100, 100, 100);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertModelStatePersisted(stateDocId());
        assertInferenceModelPersisted(jobId);
    }

    public void testTwoJobsWithSameRandomizeSeedUseSameTrainingSet() throws Exception {
        String sourceIndex = "regression_two_jobs_with_same_randomize_seed_source";
        indexData(sourceIndex, 10, 0);

        String firstJobId = "regression_two_jobs_with_same_randomize_seed_1";
        String firstJobDestIndex = firstJobId + "_dest";

        BoostedTreeParams boostedTreeParams = new BoostedTreeParams(1.0, 1.0, 1.0, 1, 1.0);

        DataFrameAnalyticsConfig firstJob = buildAnalytics(firstJobId, sourceIndex, firstJobDestIndex, null,
            new Regression(DEPENDENT_VARIABLE_FIELD, boostedTreeParams, null, 50.0, null));
        registerAnalytics(firstJob);
        putAnalytics(firstJob);

        String secondJobId = "regression_two_jobs_with_same_randomize_seed_2";
        String secondJobDestIndex = secondJobId + "_dest";

        long randomizeSeed = ((Regression) firstJob.getAnalysis()).getRandomizeSeed();
        DataFrameAnalyticsConfig secondJob = buildAnalytics(secondJobId, sourceIndex, secondJobDestIndex, null,
            new Regression(DEPENDENT_VARIABLE_FIELD, boostedTreeParams, null, 50.0, randomizeSeed));

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

    private static void indexData(String sourceIndex, int numTrainingRows, int numNonTrainingRows) {
        client().admin().indices().prepareCreate(sourceIndex)
            .addMapping("_doc", NUMERICAL_FEATURE_FIELD, "type=double", DEPENDENT_VARIABLE_FIELD, "type=double")
            .get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < numTrainingRows; i++) {
            List<Object> source = List.of(
                NUMERICAL_FEATURE_FIELD, NUMERICAL_FEATURE_VALUES.get(i % NUMERICAL_FEATURE_VALUES.size()),
                DEPENDENT_VARIABLE_FIELD, DEPENDENT_VARIABLE_VALUES.get(i % DEPENDENT_VARIABLE_VALUES.size()));
            IndexRequest indexRequest = new IndexRequest(sourceIndex).source(source.toArray());
            bulkRequestBuilder.add(indexRequest);
        }
        for (int i = numTrainingRows; i < numTrainingRows + numNonTrainingRows; i++) {
            List<Object> source = List.of(NUMERICAL_FEATURE_FIELD, NUMERICAL_FEATURE_VALUES.get(i % NUMERICAL_FEATURE_VALUES.size()));
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

    protected String stateDocId() {
        return jobId + "_regression_state#1";
    }
}
