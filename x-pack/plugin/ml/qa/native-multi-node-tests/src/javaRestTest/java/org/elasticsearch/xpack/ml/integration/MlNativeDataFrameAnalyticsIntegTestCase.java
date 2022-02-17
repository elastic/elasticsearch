/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.EvaluateDataFrameAction;
import org.elasticsearch.xpack.core.ml.action.ExplainDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.action.PreviewDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigUpdate;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.Evaluation;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;
import org.elasticsearch.xpack.ml.dataframe.StoredProgress;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Base class of ML integration tests that use a native data_frame_analytics process
 */
abstract class MlNativeDataFrameAnalyticsIntegTestCase extends MlNativeIntegTestCase {

    protected PutDataFrameAnalyticsAction.Response putAnalytics(DataFrameAnalyticsConfig config) {
        PutDataFrameAnalyticsAction.Request request = new PutDataFrameAnalyticsAction.Request(config);
        return client().execute(PutDataFrameAnalyticsAction.INSTANCE, request).actionGet();
    }

    protected PutDataFrameAnalyticsAction.Response updateAnalytics(DataFrameAnalyticsConfigUpdate update) {
        UpdateDataFrameAnalyticsAction.Request request = new UpdateDataFrameAnalyticsAction.Request(update);
        return client().execute(UpdateDataFrameAnalyticsAction.INSTANCE, request).actionGet();
    }

    protected NodeAcknowledgedResponse startAnalytics(String id) {
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
        waitUntilAnalyticsIsStopped(id, TimeValue.timeValueSeconds(60));
    }

    protected void waitUntilAnalyticsIsStopped(String id, TimeValue waitTime) throws Exception {
        assertBusy(() -> assertIsStopped(id), waitTime.getMillis(), TimeUnit.MILLISECONDS);
    }

    protected void waitUntilAnalyticsIsFailed(String id) throws Exception {
        assertBusy(() -> assertIsFailed(id), TimeValue.timeValueSeconds(30).millis(), TimeUnit.MILLISECONDS);
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
        assertThat("Got: " + stats.toString(), stats, hasSize(1));
        return stats.get(0);
    }

    protected ExplainDataFrameAnalyticsAction.Response explainDataFrame(DataFrameAnalyticsConfig config) {
        PutDataFrameAnalyticsAction.Request request = new PutDataFrameAnalyticsAction.Request(config);
        return client().execute(ExplainDataFrameAnalyticsAction.INSTANCE, request).actionGet();
    }

    protected EvaluateDataFrameAction.Response evaluateDataFrame(String index, Evaluation evaluation) {
        EvaluateDataFrameAction.Request request = new EvaluateDataFrameAction.Request().setIndices(List.of(index))
            .setEvaluation(evaluation);
        return client().execute(EvaluateDataFrameAction.INSTANCE, request).actionGet();
    }

    protected PreviewDataFrameAnalyticsAction.Response previewDataFrame(String id) {
        List<DataFrameAnalyticsConfig> analytics = getAnalytics(id);
        assertThat(analytics, hasSize(1));
        return client().execute(PreviewDataFrameAnalyticsAction.INSTANCE, new PreviewDataFrameAnalyticsAction.Request(analytics.get(0)))
            .actionGet();
    }

    static DataFrameAnalyticsConfig buildAnalytics(
        String id,
        String sourceIndex,
        String destIndex,
        @Nullable String resultsField,
        DataFrameAnalysis analysis
    ) throws Exception {
        return buildAnalytics(id, sourceIndex, destIndex, resultsField, analysis, QueryBuilders.matchAllQuery());
    }

    protected static DataFrameAnalyticsConfig buildAnalytics(
        String id,
        String sourceIndex,
        String destIndex,
        @Nullable String resultsField,
        DataFrameAnalysis analysis,
        QueryBuilder queryBuilder
    ) throws Exception {
        return new DataFrameAnalyticsConfig.Builder().setId(id)
            .setSource(
                new DataFrameAnalyticsSource(
                    new String[] { sourceIndex },
                    QueryProvider.fromParsedQuery(queryBuilder),
                    null,
                    Collections.emptyMap()
                )
            )
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

    protected void assertIsFailed(String id) {
        GetDataFrameAnalyticsStatsAction.Response.Stats stats = getAnalyticsStats(id);
        assertThat("Stats were: " + Strings.toString(stats), stats.getState(), equalTo(DataFrameAnalyticsState.FAILED));
    }

    protected void assertProgressIsZero(String id) {
        List<PhaseProgress> progress = getProgress(id);
        assertThat(
            "progress is not all zero: " + progress,
            progress.stream().allMatch(phaseProgress -> phaseProgress.getProgressPercent() == 0),
            is(true)
        );
    }

    protected void assertProgressComplete(String id) {
        List<PhaseProgress> progress = getProgress(id);
        assertThat(
            "progress is complete: " + progress,
            progress.stream().allMatch(phaseProgress -> phaseProgress.getProgressPercent() == 100),
            is(true)
        );
    }

    abstract boolean supportsInference();

    protected List<PhaseProgress> getProgress(String id) {
        GetDataFrameAnalyticsStatsAction.Response.Stats stats = getAnalyticsStats(id);
        assertThat(stats.getId(), equalTo(id));
        List<PhaseProgress> progress = stats.getProgress();
        // We should have at least 4 phases: reindexing, loading_data, writing_results, plus at least one for the analysis
        assertThat(progress.size(), greaterThanOrEqualTo(4));
        assertThat(progress.get(0).getPhase(), equalTo("reindexing"));
        assertThat(progress.get(1).getPhase(), equalTo("loading_data"));
        if (supportsInference()) {
            assertThat(progress.get(progress.size() - 2).getPhase(), equalTo("writing_results"));
            assertThat(progress.get(progress.size() - 1).getPhase(), equalTo("inference"));
        } else {
            assertThat(progress.get(progress.size() - 1).getPhase(), equalTo("writing_results"));
        }
        return progress;
    }

    protected SearchResponse searchStoredProgress(String jobId) {
        String docId = StoredProgress.documentId(jobId);
        return client().prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern()).setQuery(QueryBuilders.idsQuery().addIds(docId)).get();
    }

    protected void assertExactlyOneInferenceModelPersisted(String jobId) {
        assertInferenceModelPersisted(jobId, equalTo(1));
    }

    protected void assertAtLeastOneInferenceModelPersisted(String jobId) {
        assertInferenceModelPersisted(jobId, greaterThanOrEqualTo(1));
    }

    private void assertInferenceModelPersisted(String jobId, Matcher<? super Integer> modelHitsArraySizeMatcher) {
        SearchResponse searchResponse = client().prepareSearch(InferenceIndexConstants.LATEST_INDEX_NAME)
            .setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery(TrainedModelConfig.TAGS.getPreferredName(), jobId)))
            .get();
        // If the job is stopped during writing_results phase and it is then restarted, there is a chance two trained models
        // were persisted as there is no way currently for the process to be certain the model was persisted.
        assertThat(
            "Hits were: " + Strings.toString(searchResponse.getHits()),
            searchResponse.getHits().getHits(),
            is(arrayWithSize(modelHitsArraySizeMatcher))
        );
    }

    protected Collection<PersistentTasksCustomMetadata.PersistentTask<?>> analyticsTaskList() {
        ClusterState masterClusterState = client().admin().cluster().prepareState().all().get().getState();
        PersistentTasksCustomMetadata persistentTasks = masterClusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        return persistentTasks != null
            ? persistentTasks.findTasks(MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, task -> true)
            : Collections.emptyList();
    }

    protected List<TaskInfo> analyticsAssignedTaskList() {
        return client().admin().cluster().prepareListTasks().setActions(MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME + "[c]").get().getTasks();
    }

    protected void waitUntilSomeProgressHasBeenMadeForPhase(String jobId, String phase) throws Exception {
        assertBusy(() -> {
            List<PhaseProgress> progress = getAnalyticsStats(jobId).getProgress();
            Optional<PhaseProgress> phaseProgress = progress.stream().filter(p -> phase.equals(p.getPhase())).findFirst();
            assertThat("unexpected phase [" + phase + "]; progress was " + progress, phaseProgress.isEmpty(), is(false));
            assertThat(phaseProgress.get().getProgressPercent(), greaterThan(1));
        }, 60, TimeUnit.SECONDS);
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
        assertBusy(() -> assertTrue(indexExists(NotificationsIndex.NOTIFICATIONS_INDEX)));

        @SuppressWarnings("unchecked")
        Matcher<String>[] itemMatchers = Arrays.stream(expectedAuditMessagePrefixes).map(Matchers::startsWith).toArray(Matcher[]::new);
        assertBusy(() -> {
            List<String> allAuditMessages = fetchAllAuditMessages(configId);
            assertThat(allAuditMessages, hasItems(itemMatchers));
            // TODO: Consider restoring this assertion when we are sure all the audit messages are available at this point.
            // assertThat("Messages: " + allAuditMessages, allAuditMessages, hasSize(expectedAuditMessagePrefixes.length));
        });
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
        assertThat("Hits were: " + Strings.toString(searchResponse.getHits()), searchResponse.getHits().getHits(), is(arrayWithSize(1)));
    }

    protected static void assertMlResultsFieldMappings(String index, String predictedClassField, String expectedType) {
        Map<String, Object> mappings = client().execute(GetIndexAction.INSTANCE, new GetIndexRequest().indices(index))
            .actionGet()
            .mappings()
            .get(index)
            .sourceAsMap();
        assertThat(
            mappings.toString(),
            getFieldValue(
                mappings,
                "properties",
                "ml",
                "properties",
                String.join(".properties.", predictedClassField.split("\\.")),
                "type"
            ),
            equalTo(expectedType)
        );
        if (getFieldValue(mappings, "properties", "ml", "properties", "top_classes") != null) {
            assertThat(
                mappings.toString(),
                getFieldValue(mappings, "properties", "ml", "properties", "top_classes", "type"),
                equalTo("nested")
            );
            assertThat(
                mappings.toString(),
                getFieldValue(mappings, "properties", "ml", "properties", "top_classes", "properties", "class_name", "type"),
                equalTo(expectedType)
            );
            assertThat(
                mappings.toString(),
                getFieldValue(mappings, "properties", "ml", "properties", "top_classes", "properties", "class_probability", "type"),
                equalTo("double")
            );
        }
    }

    /**
     * Wrapper around extractValue that:
     * - allows dots (".") in the path elements provided as arguments
     * - supports implicit casting to the appropriate type
     */
    @SuppressWarnings("unchecked")
    protected static <T> T getFieldValue(Map<String, Object> doc, String... path) {
        return (T) extractValue(String.join(".", path), doc);
    }
}
