/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.persistent.UpdatePersistentTaskStatusAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.DataCounts;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask.StartingState;
import org.elasticsearch.xpack.ml.dataframe.stats.ProgressTracker;
import org.elasticsearch.xpack.ml.dataframe.stats.StatsHolder;
import org.elasticsearch.xpack.ml.dataframe.steps.DataFrameAnalyticsStep;
import org.elasticsearch.xpack.ml.dataframe.steps.StepResponse;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class DataFrameAnalyticsTaskTests extends ESTestCase {

    public void testDetermineStartingState_GivenZeroProgress() {
        List<PhaseProgress> progress = Arrays.asList(
            new PhaseProgress("reindexing", 0),
            new PhaseProgress("loading_data", 0),
            new PhaseProgress("analyzing", 0),
            new PhaseProgress("writing_results", 0)
        );

        StartingState startingState = DataFrameAnalyticsTask.determineStartingState("foo", progress);

        assertThat(startingState, equalTo(StartingState.FIRST_TIME));
    }

    public void testDetermineStartingState_GivenReindexingIsIncomplete() {
        List<PhaseProgress> progress = Arrays.asList(
            new PhaseProgress("reindexing", 99),
            new PhaseProgress("loading_data", 0),
            new PhaseProgress("analyzing", 0),
            new PhaseProgress("writing_results", 0)
        );

        StartingState startingState = DataFrameAnalyticsTask.determineStartingState("foo", progress);

        assertThat(startingState, equalTo(StartingState.RESUMING_REINDEXING));
    }

    public void testDetermineStartingState_GivenLoadingDataIsIncomplete() {
        List<PhaseProgress> progress = Arrays.asList(
            new PhaseProgress("reindexing", 100),
            new PhaseProgress("loading_data", 1),
            new PhaseProgress("analyzing", 0),
            new PhaseProgress("writing_results", 0)
        );

        StartingState startingState = DataFrameAnalyticsTask.determineStartingState("foo", progress);

        assertThat(startingState, equalTo(StartingState.RESUMING_ANALYZING));
    }

    public void testDetermineStartingState_GivenAnalyzingIsIncomplete() {
        List<PhaseProgress> progress = Arrays.asList(
            new PhaseProgress("reindexing", 100),
            new PhaseProgress("loading_data", 100),
            new PhaseProgress("analyzing", 99),
            new PhaseProgress("writing_results", 0)
        );

        StartingState startingState = DataFrameAnalyticsTask.determineStartingState("foo", progress);

        assertThat(startingState, equalTo(StartingState.RESUMING_ANALYZING));
    }

    public void testDetermineStartingState_GivenWritingResultsIsIncomplete() {
        List<PhaseProgress> progress = Arrays.asList(
            new PhaseProgress("reindexing", 100),
            new PhaseProgress("loading_data", 100),
            new PhaseProgress("analyzing", 100),
            new PhaseProgress("writing_results", 1)
        );

        StartingState startingState = DataFrameAnalyticsTask.determineStartingState("foo", progress);

        assertThat(startingState, equalTo(StartingState.RESUMING_ANALYZING));
    }

    public void testDetermineStartingState_GivenInferenceIsIncomplete() {
        List<PhaseProgress> progress = Arrays.asList(
            new PhaseProgress("reindexing", 100),
            new PhaseProgress("loading_data", 100),
            new PhaseProgress("analyzing", 100),
            new PhaseProgress("writing_results", 100),
            new PhaseProgress("inference", 40)
        );

        StartingState startingState = DataFrameAnalyticsTask.determineStartingState("foo", progress);

        assertThat(startingState, equalTo(StartingState.RESUMING_INFERENCE));
    }

    public void testDetermineStartingState_GivenFinished() {
        List<PhaseProgress> progress = Arrays.asList(
            new PhaseProgress("reindexing", 100),
            new PhaseProgress("loading_data", 100),
            new PhaseProgress("analyzing", 100),
            new PhaseProgress("writing_results", 100)
        );

        StartingState startingState = DataFrameAnalyticsTask.determineStartingState("foo", progress);

        assertThat(startingState, equalTo(StartingState.FINISHED));
    }

    public void testDetermineStartingState_GivenEmptyProgress() {
        StartingState startingState = DataFrameAnalyticsTask.determineStartingState("foo", Collections.emptyList());
        assertThat(startingState, equalTo(StartingState.FINISHED));
    }

    private void testPersistProgress(SearchHits searchHits, String expectedIndexOrAlias) throws IOException {
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);

        ClusterService clusterService = mock(ClusterService.class);
        DataFrameAnalyticsManager analyticsManager = mock(DataFrameAnalyticsManager.class);
        DataFrameAnalyticsAuditor auditor = mock(DataFrameAnalyticsAuditor.class);
        PersistentTasksService persistentTasksService = new PersistentTasksService(clusterService, mock(ThreadPool.class), client);

        List<PhaseProgress> progress = List.of(
            new PhaseProgress(ProgressTracker.REINDEXING, 100),
            new PhaseProgress(ProgressTracker.LOADING_DATA, 50),
            new PhaseProgress(ProgressTracker.WRITING_RESULTS, 0)
        );

        StartDataFrameAnalyticsAction.TaskParams taskParams = new StartDataFrameAnalyticsAction.TaskParams(
            "task_id",
            MlConfigVersion.CURRENT,
            false
        );

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(searchHits);
        doAnswer(withResponse(searchResponse)).when(client).execute(eq(TransportSearchAction.TYPE), any(), any());

        IndexResponse indexResponse = mock(IndexResponse.class);
        doAnswer(withResponse(indexResponse)).when(client).execute(eq(TransportIndexAction.TYPE), any(), any());

        TaskManager taskManager = mock(TaskManager.class);

        Runnable runnable = mock(Runnable.class);

        DataFrameAnalyticsTask task = new DataFrameAnalyticsTask(
            123,
            "type",
            "action",
            null,
            Map.of(),
            client,
            analyticsManager,
            auditor,
            taskParams,
            mock(XPackLicenseState.class)
        );
        task.init(persistentTasksService, taskManager, "task-id", 42);
        task.setStatsHolder(new StatsHolder(progress, null, null, new DataCounts("test_job")));

        task.persistProgress(client, "task_id", runnable);

        ArgumentCaptor<IndexRequest> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);

        InOrder inOrder = inOrder(client, runnable);
        inOrder.verify(client).execute(eq(TransportSearchAction.TYPE), any(), any());
        inOrder.verify(client).execute(eq(TransportIndexAction.TYPE), indexRequestCaptor.capture(), any());
        inOrder.verify(runnable).run();
        inOrder.verifyNoMoreInteractions();

        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertThat(indexRequest.index(), equalTo(expectedIndexOrAlias));
        assertThat(indexRequest.isRequireAlias(), equalTo(".ml-state-write".equals(expectedIndexOrAlias)));
        assertThat(indexRequest.id(), equalTo("data_frame_analytics-task_id-progress"));

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                XContentParserConfiguration.EMPTY,
                indexRequest.source().utf8ToString()
            )
        ) {
            StoredProgress parsedProgress = StoredProgress.PARSER.apply(parser, null);
            assertThat(parsedProgress.get(), equalTo(progress));
        }
    }

    public void testPersistProgress_ProgressDocumentCreated() throws IOException {
        testPersistProgress(SearchHits.EMPTY_WITH_TOTAL_HITS, ".ml-state-write");
    }

    public void testPersistProgress_ProgressDocumentUpdated() throws IOException {
        var hits = new SearchHits(
            new SearchHit[] { SearchResponseUtils.searchHitFromMap(Map.of("_index", ".ml-state-dummy")) },
            null,
            0.0f
        );
        try {
            testPersistProgress(hits, ".ml-state-dummy");
        } finally {
            hits.decRef();
        }
    }

    public void testSetFailed() throws IOException {
        testSetFailed(false);
    }

    public void testSetFailedDuringNodeShutdown() throws IOException {
        testSetFailed(true);
    }

    private void testSetFailed(boolean nodeShuttingDown) throws IOException {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        ClusterService clusterService = mock(ClusterService.class);
        DataFrameAnalyticsManager analyticsManager = mock(DataFrameAnalyticsManager.class);
        when(analyticsManager.isNodeShuttingDown()).thenReturn(nodeShuttingDown);
        DataFrameAnalyticsAuditor auditor = mock(DataFrameAnalyticsAuditor.class);
        PersistentTasksService persistentTasksService = new PersistentTasksService(clusterService, mock(ThreadPool.class), client);
        TaskManager taskManager = mock(TaskManager.class);

        // We leave reindexing progress here to zero in order to check it is updated before it is persisted
        List<PhaseProgress> progress = List.of(
            new PhaseProgress(ProgressTracker.REINDEXING, 0),
            new PhaseProgress(ProgressTracker.LOADING_DATA, 100),
            new PhaseProgress(ProgressTracker.WRITING_RESULTS, 30)
        );

        StartDataFrameAnalyticsAction.TaskParams taskParams = new StartDataFrameAnalyticsAction.TaskParams(
            "job-id",
            MlConfigVersion.CURRENT,
            false
        );

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(SearchHits.EMPTY_WITH_TOTAL_HITS);
        doAnswer(withResponse(searchResponse)).when(client).execute(eq(TransportSearchAction.TYPE), any(), any());

        IndexResponse indexResponse = mock(IndexResponse.class);
        doAnswer(withResponse(indexResponse)).when(client).execute(eq(TransportIndexAction.TYPE), any(), any());

        DataFrameAnalyticsTask task = new DataFrameAnalyticsTask(
            123,
            "type",
            "action",
            null,
            Map.of(),
            client,
            analyticsManager,
            auditor,
            taskParams,
            mock(XPackLicenseState.class)
        );
        task.init(persistentTasksService, taskManager, "task-id", 42);
        task.setStatsHolder(new StatsHolder(progress, null, null, new DataCounts("test_job")));
        task.setStep(new StubReindexingStep(task.getStatsHolder().getProgressTracker()));
        Exception exception = new Exception("some exception");

        task.setFailed(exception);

        verify(analyticsManager).isNodeShuttingDown();
        verify(client, atLeastOnce()).settings();
        verify(client, atLeastOnce()).threadPool();

        if (nodeShuttingDown == false) {
            // Verify progress was persisted
            ArgumentCaptor<IndexRequest> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
            verify(client).execute(eq(TransportSearchAction.TYPE), any(), any());
            verify(client).execute(eq(TransportIndexAction.TYPE), indexRequestCaptor.capture(), any());

            IndexRequest indexRequest = indexRequestCaptor.getValue();
            assertThat(indexRequest.index(), equalTo(AnomalyDetectorsIndex.jobStateIndexWriteAlias()));
            assertThat(indexRequest.id(), equalTo("data_frame_analytics-job-id-progress"));

            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    XContentParserConfiguration.EMPTY,
                    indexRequest.source().utf8ToString()
                )
            ) {
                StoredProgress parsedProgress = StoredProgress.PARSER.apply(parser, null);
                assertThat(parsedProgress.get(), hasSize(3));
                assertThat(parsedProgress.get().get(0), equalTo(new PhaseProgress("reindexing", 100)));
            }

            ArgumentCaptor<UpdatePersistentTaskStatusAction.Request> captor = ArgumentCaptor.forClass(
                UpdatePersistentTaskStatusAction.Request.class
            );

            verify(client).execute(same(UpdatePersistentTaskStatusAction.INSTANCE), captor.capture(), any());

            UpdatePersistentTaskStatusAction.Request request = captor.getValue();
            assertThat(request.getTaskId(), equalTo("task-id"));
            DataFrameAnalyticsTaskState state = (DataFrameAnalyticsTaskState) request.getState();
            assertThat(state.getState(), equalTo(DataFrameAnalyticsState.FAILED));
            assertThat(state.getAllocationId(), equalTo(42L));
            assertThat(state.getReason(), equalTo("some exception"));
        }

        verifyNoMoreInteractions(client, analyticsManager, auditor, taskManager);
    }

    @SuppressWarnings("unchecked")
    private static <Response> Answer<Response> withResponse(Response response) {
        return invocationOnMock -> {
            ActionListener<Response> listener = (ActionListener<Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        };
    }

    private static class StubReindexingStep implements DataFrameAnalyticsStep {

        private final ProgressTracker progressTracker;

        StubReindexingStep(ProgressTracker progressTracker) {
            this.progressTracker = progressTracker;
        }

        @Override
        public Name name() {
            return Name.REINDEXING;
        }

        @Override
        public void execute(ActionListener<StepResponse> listener) {}

        @Override
        public void cancel(String reason, TimeValue timeout) {}

        @Override
        public void updateProgress(ActionListener<Void> listener) {
            progressTracker.updateReindexingProgress(100);
            listener.onResponse(null);
        }
    }
}
