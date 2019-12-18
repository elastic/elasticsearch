/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ml.action.DeleteDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.EvaluateDataFrameAction;
import org.elasticsearch.xpack.core.ml.action.ExplainDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.Evaluation;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.notifications.AuditorField;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;
import org.elasticsearch.xpack.ml.dataframe.StoredProgress;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Base class of ML integration tests that use a native data_frame_analytics process
 */
abstract class MlNativeDataFrameAnalyticsIntegTestCase extends MlNativeIntegTestCase {

    private List<DataFrameAnalyticsConfig> analytics = new ArrayList<>();

    @Override
    protected void cleanUpResources() {
        cleanUpAnalytics();
    }

    private void cleanUpAnalytics() {
        stopAnalyticsAndForceStopOnError();

        for (DataFrameAnalyticsConfig config : analytics) {
            try {
                assertThat(deleteAnalytics(config.getId()).isAcknowledged(), is(true));
                assertThat(searchStoredProgress(config.getId()).getHits().getTotalHits().value, equalTo(0L));
            } catch (Exception e) {
                // ignore
            }
        }
    }

    private void stopAnalyticsAndForceStopOnError() {
        try {
            assertThat(stopAnalytics("*").isStopped(), is(true));
        } catch (Exception e) {
            logger.error("Failed to stop data frame analytics jobs; trying force", e);
            try {
                assertThat(forceStopAnalytics("*").isStopped(), is(true));
            } catch (Exception e2) {
                logger.error("Force-stopping data frame analytics jobs failed", e2);
            }
            throw new RuntimeException("Had to resort to force-stopping jobs, something went wrong?", e);
        }
    }

    protected void registerAnalytics(DataFrameAnalyticsConfig config) {
        if (analytics.add(config) == false) {
            throw new IllegalArgumentException("analytics config [" + config.getId() + "] is already registered");
        }
    }

    protected PutDataFrameAnalyticsAction.Response putAnalytics(DataFrameAnalyticsConfig config) {
        PutDataFrameAnalyticsAction.Request request = new PutDataFrameAnalyticsAction.Request(config);
        return client().execute(PutDataFrameAnalyticsAction.INSTANCE, request).actionGet();
    }

    protected AcknowledgedResponse deleteAnalytics(String id) {
        DeleteDataFrameAnalyticsAction.Request request = new DeleteDataFrameAnalyticsAction.Request(id);
        return client().execute(DeleteDataFrameAnalyticsAction.INSTANCE, request).actionGet();
    }

    protected AcknowledgedResponse startAnalytics(String id) {
        StartDataFrameAnalyticsAction.Request request = new StartDataFrameAnalyticsAction.Request(id);
        return client().execute(StartDataFrameAnalyticsAction.INSTANCE, request).actionGet();
    }

    protected StopDataFrameAnalyticsAction.Response stopAnalytics(String id) {
        StopDataFrameAnalyticsAction.Request request = new StopDataFrameAnalyticsAction.Request(id);
        return client().execute(StopDataFrameAnalyticsAction.INSTANCE, request).actionGet();
    }

    protected StopDataFrameAnalyticsAction.Response forceStopAnalytics(String id) {
        StopDataFrameAnalyticsAction.Request request = new StopDataFrameAnalyticsAction.Request(id);
        request.setForce(true);
        return client().execute(StopDataFrameAnalyticsAction.INSTANCE, request).actionGet();
    }

    protected void waitUntilAnalyticsIsStopped(String id) throws Exception {
        waitUntilAnalyticsIsStopped(id, TimeValue.timeValueSeconds(30));
    }

    protected void waitUntilAnalyticsIsStopped(String id, TimeValue waitTime) throws Exception {
        assertBusy(() -> assertIsStopped(id), waitTime.getMillis(), TimeUnit.MILLISECONDS);
    }

    protected List<DataFrameAnalyticsConfig> getAnalytics(String id) {
        GetDataFrameAnalyticsAction.Request request = new GetDataFrameAnalyticsAction.Request(id);
        return client().execute(GetDataFrameAnalyticsAction.INSTANCE, request).actionGet().getResources().results();
    }

    protected GetDataFrameAnalyticsStatsAction.Response.Stats getAnalyticsStats(String id) {
        GetDataFrameAnalyticsStatsAction.Request request = new GetDataFrameAnalyticsStatsAction.Request(id);
        GetDataFrameAnalyticsStatsAction.Response response = client().execute(GetDataFrameAnalyticsStatsAction.INSTANCE, request)
            .actionGet();
        List<GetDataFrameAnalyticsStatsAction.Response.Stats> stats = response.getResponse().results();
        assertThat("Got: " + stats.toString(), stats.size(), equalTo(1));
        return stats.get(0);
    }

    protected ExplainDataFrameAnalyticsAction.Response explainDataFrame(DataFrameAnalyticsConfig config) {
        PutDataFrameAnalyticsAction.Request request = new PutDataFrameAnalyticsAction.Request(config);
        return client().execute(ExplainDataFrameAnalyticsAction.INSTANCE, request).actionGet();
    }

    protected EvaluateDataFrameAction.Response evaluateDataFrame(String index, Evaluation evaluation) {
        EvaluateDataFrameAction.Request request =
            new EvaluateDataFrameAction.Request()
                .setIndices(List.of(index))
                .setEvaluation(evaluation);
        return client().execute(EvaluateDataFrameAction.INSTANCE, request).actionGet();
    }

    protected static DataFrameAnalyticsConfig buildAnalytics(String id, String sourceIndex, String destIndex,
                                                             @Nullable String resultsField, DataFrameAnalysis analysis) throws Exception {
        return buildAnalytics(id, sourceIndex, destIndex, resultsField, analysis, QueryBuilders.matchAllQuery());
    }

    protected static DataFrameAnalyticsConfig buildAnalytics(String id, String sourceIndex, String destIndex,
                                                             @Nullable String resultsField, DataFrameAnalysis analysis,
                                                             QueryBuilder queryBuilder) throws Exception {
        return new DataFrameAnalyticsConfig.Builder()
            .setId(id)
            .setSource(new DataFrameAnalyticsSource(new String[] { sourceIndex }, QueryProvider.fromParsedQuery(queryBuilder), null))
            .setDest(new DataFrameAnalyticsDest(destIndex, resultsField))
            .setAnalysis(analysis)
            .build();
    }

    protected void assertIsStopped(String id) {
        GetDataFrameAnalyticsStatsAction.Response.Stats stats = getAnalyticsStats(id);
        assertThat(stats.getId(), equalTo(id));
        assertThat(stats.getFailureReason(), is(nullValue()));
        assertThat("Stats were: " + Strings.toString(stats), stats.getState(), equalTo(DataFrameAnalyticsState.STOPPED));
    }

    protected void assertProgress(String id, int reindexing, int loadingData, int analyzing, int writingResults) {
        GetDataFrameAnalyticsStatsAction.Response.Stats stats = getAnalyticsStats(id);
        assertThat(stats.getId(), equalTo(id));
        List<PhaseProgress> progress = stats.getProgress();
        assertThat(progress.size(), equalTo(4));
        assertThat(progress.get(0).getPhase(), equalTo("reindexing"));
        assertThat(progress.get(1).getPhase(), equalTo("loading_data"));
        assertThat(progress.get(2).getPhase(), equalTo("analyzing"));
        assertThat(progress.get(3).getPhase(), equalTo("writing_results"));
        assertThat(progress.get(0).getProgressPercent(), equalTo(reindexing));
        assertThat(progress.get(1).getProgressPercent(), equalTo(loadingData));
        assertThat(progress.get(2).getProgressPercent(), equalTo(analyzing));
        assertThat(progress.get(3).getProgressPercent(), equalTo(writingResults));
    }

    protected SearchResponse searchStoredProgress(String jobId) {
        String docId = StoredProgress.documentId(jobId);
        return client().prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern())
            .setQuery(QueryBuilders.idsQuery().addIds(docId))
            .get();
    }

    protected void assertInferenceModelPersisted(String jobId) {
        SearchResponse searchResponse = client().prepareSearch(InferenceIndexConstants.LATEST_INDEX_NAME)
            .setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery(TrainedModelConfig.TAGS.getPreferredName(), jobId)))
            .get();
        assertThat("Hits were: " + Strings.toString(searchResponse.getHits()), searchResponse.getHits().getHits(), arrayWithSize(1));
    }

    /**
     * Asserts whether the audit messages fetched from index match provided prefixes.
     * More specifically, in order to pass:
     *  1. the number of fetched messages must equal the number of provided prefixes
     * AND
     *  2. each fetched message must start with the corresponding prefix
     */
    protected static void assertThatAuditMessagesMatch(String configId, String... expectedAuditMessagePrefixes) throws Exception {
        // Make sure we wrote to the audit
        // Since calls to write the AbstractAuditor are sent and forgot (async) we could have returned from the start,
        // finished the job (as this is a very short analytics job), all without the audit being fully written.
        assertBusy(() -> assertTrue(indexExists(AuditorField.NOTIFICATIONS_INDEX)));
        @SuppressWarnings("unchecked")
        Matcher<String>[] itemMatchers = Arrays.stream(expectedAuditMessagePrefixes).map(Matchers::startsWith).toArray(Matcher[]::new);
        assertBusy(() -> {
            List<String> allAuditMessages = fetchAllAuditMessages(configId);
            assertThat(allAuditMessages, hasItems(itemMatchers));
            // TODO: Consider restoring this assertion when we are sure all the audit messages are available at this point.
            // assertThat("Messages: " + allAuditMessages, allAuditMessages, hasSize(expectedAuditMessagePrefixes.length));
        });
    }

    private static List<String> fetchAllAuditMessages(String dataFrameAnalyticsId) {
        RefreshRequest refreshRequest = new RefreshRequest(AuditorField.NOTIFICATIONS_INDEX);
        RefreshResponse refreshResponse = client().execute(RefreshAction.INSTANCE, refreshRequest).actionGet();
        assertThat(refreshResponse.getStatus().getStatus(), anyOf(equalTo(200), equalTo(201)));

        SearchRequest searchRequest = new SearchRequestBuilder(client(), SearchAction.INSTANCE)
            .setIndices(AuditorField.NOTIFICATIONS_INDEX)
            .addSort("timestamp", SortOrder.ASC)
            .setQuery(QueryBuilders.termQuery("job_id", dataFrameAnalyticsId))
            .request();
        SearchResponse searchResponse = client().execute(SearchAction.INSTANCE, searchRequest).actionGet();

        return Arrays.stream(searchResponse.getHits().getHits())
            .map(hit -> (String) hit.getSourceAsMap().get("message"))
            .collect(Collectors.toList());
    }

    protected static Set<String> getTrainingRowsIds(String index) {
        Set<String> trainingRowsIds = new HashSet<>();
        SearchResponse hits = client().prepareSearch(index).setSize(10000).get();
        for (SearchHit hit : hits.getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            assertThat(sourceAsMap.containsKey("ml"), is(true));
            @SuppressWarnings("unchecked")
            Map<String, Object> resultsObject = (Map<String, Object>) sourceAsMap.get("ml");

            assertThat(resultsObject.containsKey("is_training"), is(true));
            if (Boolean.TRUE.equals(resultsObject.get("is_training"))) {
                trainingRowsIds.add(hit.getId());
            }
        }
        assertThat(trainingRowsIds.isEmpty(), is(false));
        return trainingRowsIds;
    }

    protected static void assertModelStatePersisted(String stateDocId) {
        SearchResponse searchResponse = client().prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern())
            .setQuery(QueryBuilders.idsQuery().addIds(stateDocId))
            .get();
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
    }
}
