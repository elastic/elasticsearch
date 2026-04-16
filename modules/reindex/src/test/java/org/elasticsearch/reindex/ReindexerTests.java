/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.logging.log4j.Level;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.PaginatedSearchFailure;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.index.reindex.ResumeBulkByScrollRequest;
import org.elasticsearch.index.reindex.ResumeBulkByScrollResponse;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.index.reindex.ResumeReindexAction;
import org.elasticsearch.index.reindex.TaskRelocatedException;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.util.concurrent.EsExecutors.DIRECT_EXECUTOR_SERVICE;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.elasticsearch.rest.RestStatus.TOO_MANY_REQUESTS;
import static org.elasticsearch.test.ActionListenerUtils.neverCalledListener;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressForbidden(reason = "use a http server")
public class ReindexerTests extends ESTestCase {

    // --- wrapWithMetrics tests ---

    public void testWrapWithMetricsSuccess() {
        ReindexMetrics metrics = mock();
        ActionListener<BulkByScrollResponse> listener = spy(ActionListener.noop());
        BulkByScrollTask task = createNonSlicedWorkerTask();
        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, task, reindexRequest());

        BulkByScrollResponse response = reindexResponseWithBulkAndSearchFailures(null, null);
        wrapped.onResponse(response);

        verify(listener).onResponse(response);
        verify(metrics).recordSuccess(eq(false), any());
        verify(metrics, never()).recordFailure(anyBoolean(), any(), any());
        verify(metrics).recordTookTime(anyLong(), eq(false), any());
    }

    public void testWrapWithMetricsFailure() {
        ReindexMetrics metrics = mock();
        ActionListener<BulkByScrollResponse> listener = spy(ActionListener.noop());
        BulkByScrollTask task = createNonSlicedWorkerTask();
        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, task, reindexRequest());

        Exception exception = new Exception("random failure");
        wrapped.onFailure(exception);

        verify(listener).onFailure(exception);
        verify(metrics, never()).recordSuccess(anyBoolean(), any());
        verify(metrics).recordFailure(eq(false), any(), eq(exception));
        verify(metrics).recordTookTime(anyLong(), eq(false), any());
    }

    public void testWrapWithMetricsBulkFailure() {
        ReindexMetrics metrics = mock();
        ActionListener<BulkByScrollResponse> listener = spy(ActionListener.noop());
        BulkByScrollTask task = createNonSlicedWorkerTask();
        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, task, reindexRequest());

        Exception exception = new Exception("random failure");
        Exception anotherException = new Exception("another failure");
        BulkByScrollResponse response = reindexResponseWithBulkAndSearchFailures(
            List.of(new BulkItemResponse.Failure("0", "0", exception), new BulkItemResponse.Failure("1", "1", anotherException)),
            null
        );
        wrapped.onResponse(response);

        verify(listener).onResponse(response);
        verify(metrics, never()).recordSuccess(anyBoolean(), any());
        verify(metrics).recordFailure(eq(false), any(), eq(exception));
        verify(metrics).recordTookTime(anyLong(), eq(false), any());
    }

    public void testWrapWithMetricsSearchFailure() {
        ReindexMetrics metrics = mock();
        ActionListener<BulkByScrollResponse> listener = spy(ActionListener.noop());
        BulkByScrollTask task = createNonSlicedWorkerTask();
        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, task, reindexRequest());

        Exception exception = new Exception("random failure");
        Exception anotherException = new Exception("another failure");
        BulkByScrollResponse response = reindexResponseWithBulkAndSearchFailures(
            null,
            List.of(new PaginatedSearchFailure(exception), new PaginatedSearchFailure(anotherException))
        );
        wrapped.onResponse(response);

        verify(listener).onResponse(response);
        verify(metrics, never()).recordSuccess(anyBoolean(), any());
        verify(metrics).recordFailure(eq(false), any(), eq(exception));
        verify(metrics).recordTookTime(anyLong(), eq(false), any());
    }

    public void testWrapWithMetricsSkipsSliceWorker() {
        ReindexMetrics metrics = mock();
        ActionListener<BulkByScrollResponse> listener = spy(ActionListener.noop());
        BulkByScrollTask task = createSliceWorkerTask();

        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, task, reindexRequest());

        assertSame(listener, wrapped);
        verifyNoMoreInteractions(metrics);
    }

    public void testWrapWithMetricsWrapsLeader() {
        ReindexMetrics metrics = mock();
        ActionListener<BulkByScrollResponse> listener = spy(ActionListener.noop());
        BulkByScrollTask task = createLeaderTask();

        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, task, reindexRequest());

        assertNotSame(listener, wrapped);

        BulkByScrollResponse response = reindexResponseWithBulkAndSearchFailures(null, null);
        wrapped.onResponse(response);

        verify(metrics).recordSuccess(eq(false), any());
        verify(metrics).recordTookTime(anyLong(), eq(false), any());
    }

    public void testWrapWithMetricsSkipsMetricsWhenRelocating() {
        ReindexMetrics metrics = mock();
        ActionListener<BulkByScrollResponse> listener = spy(ActionListener.noop());
        BulkByScrollTask task = createNonSlicedWorkerTask();

        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, task, reindexRequest());

        BulkByScrollResponse response = reindexResponseWithResumeInfo();
        wrapped.onResponse(response);

        verify(listener).onResponse(response);
        verifyNoMoreInteractions(metrics);
    }

    public void testWrapWithMetricsRecordsDurationFromRelocationOrigin() {
        final long taskStartTimeMillis = TimeUnit.SECONDS.toMillis(randomLongBetween(0, 100));
        final long currentTimeMillis = taskStartTimeMillis + TimeUnit.SECONDS.toMillis(randomIntBetween(0, 100));
        final long expectedElapsedSeconds = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis - taskStartTimeMillis);

        final ReindexMetrics metrics = mock();
        final ActionListener<BulkByScrollResponse> listener = spy(ActionListener.noop());
        final ResumeInfo.RelocationOrigin origin = new ResumeInfo.RelocationOrigin(randomTaskId(), taskStartTimeMillis);
        final BulkByScrollTask task = nonSlicedWorkerTaskWithOrigin(origin);

        final var wrapped = Reindexer.wrapWithMetrics(listener, metrics, task, reindexRequest(), () -> currentTimeMillis);

        final BulkByScrollResponse response = reindexResponseWithBulkAndSearchFailures(null, null);
        wrapped.onResponse(response);

        final ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
        verify(metrics).recordTookTime(captor.capture(), eq(false), any());
        assertThat(captor.getValue(), equalTo(expectedElapsedSeconds));
    }

    public void testWrapWithMetricsRecordsDurationForNewTask() {
        final ReindexMetrics metrics = mock();
        final ActionListener<BulkByScrollResponse> listener = spy(ActionListener.noop());
        final BulkByScrollTask task = nonSlicedWorkerTaskWithOrigin(null);
        final long taskStartTime = task.getStartTime();
        final long currentTimeMillis = taskStartTime + TimeUnit.SECONDS.toMillis(randomIntBetween(0, 100));
        final long expectedElapsedSeconds = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis - taskStartTime);

        final var wrapped = Reindexer.wrapWithMetrics(listener, metrics, task, reindexRequest(), () -> currentTimeMillis);

        final BulkByScrollResponse response = reindexResponseWithBulkAndSearchFailures(null, null);
        wrapped.onResponse(response);

        final ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
        verify(metrics).recordTookTime(captor.capture(), eq(false), any());
        assertThat(captor.getValue(), equalTo(expectedElapsedSeconds));
    }

    // listenerWithRelocations tests

    public void testListenerWithRelocationsPassesThroughForWorkerWithLeaderParent() {
        assumeTrue("reindex resilience enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final long parentTaskId = 99;
        final BulkByScrollTask leaderTask = new BulkByScrollTask(
            new TaskId(randomAlphaOfLength(10), parentTaskId),
            "test_type",
            "test_action",
            "test",
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            true,
            randomOrigin()
        );
        leaderTask.setWorkerCount(2);

        final TaskManager taskManager = mock(TaskManager.class);
        when(taskManager.getCancellableTasks()).thenReturn(Map.of(parentTaskId, leaderTask));
        final TransportService transportService = mock(TransportService.class);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        final Reindexer reindexer = reindexerWithRelocation(mock(ClusterService.class), transportService);
        final BulkByScrollTask task = createTaskWithParentIdAndRelocationEnabled(new TaskId("node", parentTaskId));
        task.setWorker(Float.POSITIVE_INFINITY, null);

        final ActionListener<BulkByScrollResponse> original = spy(ActionListener.noop());
        final ActionListener<BulkByScrollResponse> wrapped = reindexer.listenerWithRelocations(
            task,
            reindexRequest(),
            neverCalledListener(),
            original
        );

        assertSame(original, wrapped);
        verifyNoMoreInteractions(original);
    }

    public void testListenerWithRelocationsPassesThroughWhenNoRelocationRequested() {
        assumeTrue("reindex resilience enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final Reindexer reindexer = reindexerWithRelocation();
        final BulkByScrollTask task = createTaskWithParentIdAndRelocationEnabled(TaskId.EMPTY_TASK_ID);
        task.setWorker(Float.POSITIVE_INFINITY, null);
        task.getWorkerState().setNodeToRelocateToSupplier(() -> Optional.of("target-node"));
        // do NOT call task.requestRelocation()

        final ActionListener<BulkByScrollResponse> original = spy(ActionListener.noop());
        final ActionListener<BulkByScrollResponse> wrapped = reindexer.listenerWithRelocations(
            task,
            reindexRequest(),
            neverCalledListener(),
            original
        );

        final BulkByScrollResponse response = reindexResponseWithBulkAndSearchFailures(null, null);
        wrapped.onResponse(response);

        verify(original).onResponse(response);
        verify(original).delegateFailureAndWrap(any());
        verifyNoMoreInteractions(original);
    }

    public void testListenerWithRelocationsPassesThroughWhenNoResumeInfo() {
        assumeTrue("reindex resilience enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final Reindexer reindexer = reindexerWithRelocation();
        final BulkByScrollTask task = createTaskWithParentIdAndRelocationEnabled(TaskId.EMPTY_TASK_ID);
        task.setWorker(Float.POSITIVE_INFINITY, null);
        task.getWorkerState().setNodeToRelocateToSupplier(() -> Optional.of("target-node"));
        task.requestRelocation();

        final ActionListener<BulkByScrollResponse> original = spy(ActionListener.noop());
        final ActionListener<BulkByScrollResponse> wrapped = reindexer.listenerWithRelocations(
            task,
            reindexRequest(),
            neverCalledListener(),
            original
        );

        // response without ResumeInfo
        final BulkByScrollResponse response = reindexResponseWithBulkAndSearchFailures(null, null);
        wrapped.onResponse(response);

        verify(original).onResponse(response);
        verify(original).delegateFailureAndWrap(any());
        verifyNoMoreInteractions(original);
    }

    public void testListenerWithRelocationsTriggersRelocationWhenResumeInfoPresent() {
        assumeTrue("reindex resilience enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterState clusterState = mock(ClusterState.class);
        final DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        final DiscoveryNode sourceNode = DiscoveryNodeUtils.builder("source-node").build();
        final DiscoveryNode targetNode = DiscoveryNodeUtils.builder("target-node").build();
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.localNode()).thenReturn(sourceNode);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        when(discoveryNodes.get("target-node")).thenReturn(targetNode);

        final TransportService transportService = mock(TransportService.class);
        doAnswer(invocation -> {
            TransportResponseHandler<ResumeBulkByScrollResponse> handler = invocation.getArgument(3);
            handler.handleResponse(new ResumeBulkByScrollResponse(new TaskId("target-node:123")));
            return null;
        }).when(transportService).sendRequest(eq(targetNode), eq(ResumeReindexAction.NAME), any(ResumeBulkByScrollRequest.class), any());

        final Reindexer reindexer = reindexerWithRelocation(clusterService, transportService);
        final BulkByScrollTask task = createTaskWithParentIdAndRelocationEnabled(TaskId.EMPTY_TASK_ID);
        task.setWorker(Float.POSITIVE_INFINITY, null);
        task.getWorkerState().setNodeToRelocateToSupplier(() -> Optional.of("target-node"));
        task.requestRelocation();

        final ResumeInfo.RelocationOrigin origin = new ResumeInfo.RelocationOrigin(new TaskId("source-node", 987), randomNonNegativeLong());
        final PlainActionFuture<BulkByScrollResponse> future = new PlainActionFuture<>();
        final ActionListener<ResumeBulkByScrollResponse> relocationListener = spy(ActionListener.noop());
        final ActionListener<BulkByScrollResponse> wrapped = reindexer.listenerWithRelocations(
            task,
            reindexRequest(),
            relocationListener,
            future
        );

        wrapped.onResponse(reindexResponseWithResumeInfo(origin));

        assertTrue(future.isDone());
        verify(relocationListener).onResponse(any());
        verifyNoMoreInteractions(relocationListener);
        TaskRelocatedException exception = expectThrows(TaskRelocatedException.class, future::actionGet);
        assertThat(exception.getMessage(), equalTo("Task was relocated"));
        assertThat(exception.getMetadataKeys(), equalTo(Set.of("es.original_task_id", "es.relocated_task_id")));
        assertThat(exception.getMetadata("es.original_task_id"), equalTo(List.of("source-node:987")));
        assertThat(exception.getMetadata("es.relocated_task_id"), equalTo(List.of("target-node:123")));
    }

    public void testRelocationListenerIsNoopWithoutMetrics() {
        final var listener = Reindexer.relocationResponseListenerWithMetrics(null);
        assertThat(listener.toString(), is(equalTo("NoopActionListener")));
    }

    public void testRelocationListenerRecordsSuccessMetric() {
        final ReindexMetrics metrics = mock(ReindexMetrics.class);
        final ActionListener<ResumeBulkByScrollResponse> listener = Reindexer.relocationResponseListenerWithMetrics(metrics);
        final ResumeBulkByScrollResponse response = new ResumeBulkByScrollResponse(new TaskId("target-node:123"));
        listener.onResponse(response);
        verify(metrics).recordRelocationSuccess();
        verifyNoMoreInteractions(metrics);
    }

    public void testRelocationListenerRecordsFailureMetric() {
        final ReindexMetrics metrics = mock(ReindexMetrics.class);
        final ActionListener<ResumeBulkByScrollResponse> listener = Reindexer.relocationResponseListenerWithMetrics(metrics);
        final Exception e = new IllegalStateException(randomAlphaOfLength(5));
        listener.onFailure(e);
        verify(metrics).recordRelocationFailure(e);
        verifyNoMoreInteractions(metrics);
    }

    public void testRelocationListenerCalledForBothSuccessAndFailureFails() {
        final ReindexMetrics metrics = mock(ReindexMetrics.class);
        final ActionListener<ResumeBulkByScrollResponse> listener = Reindexer.relocationResponseListenerWithMetrics(metrics);
        final ResumeBulkByScrollResponse response = new ResumeBulkByScrollResponse(new TaskId("target-node:123"));
        final Exception e = new IllegalStateException(randomAlphaOfLength(5));
        if (randomBoolean()) {
            listener.onResponse(response);
            assertThrows(AssertionError.class, () -> listener.onFailure(e));
            verify(metrics).recordRelocationSuccess();
        } else {
            listener.onFailure(e);
            assertThrows(AssertionError.class, () -> listener.onResponse(response));
            verify(metrics).recordRelocationFailure(e);
        }
        verifyNoMoreInteractions(metrics);
    }

    public void testListenerWithRelocationsSendsSourceTaskResultInResumeRequest() {
        assumeTrue("reindex resilience enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterState clusterState = mock(ClusterState.class);
        final DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        final DiscoveryNode sourceNode = DiscoveryNodeUtils.builder("source-node").build();
        final DiscoveryNode targetNode = DiscoveryNodeUtils.builder("target-node").build();
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.localNode()).thenReturn(sourceNode);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        when(discoveryNodes.get("target-node")).thenReturn(targetNode);

        final TransportService transportService = mock(TransportService.class);
        doAnswer(invocation -> {
            ResumeBulkByScrollRequest resumeRequest = invocation.getArgument(2);
            TaskResult sourceTaskResult = resumeRequest.getDelegate().getResumeInfo().get().sourceTaskResult();
            assertNotNull("source task result should be set on the resume request", sourceTaskResult);
            assertThat(sourceTaskResult.getTask().taskId(), equalTo(new TaskId("source-node", 987)));
            assertTrue("source task result should be completed", sourceTaskResult.isCompleted());

            TransportResponseHandler<ResumeBulkByScrollResponse> handler = invocation.getArgument(3);
            handler.handleResponse(new ResumeBulkByScrollResponse(new TaskId("target-node:123")));
            return null;
        }).when(transportService).sendRequest(eq(targetNode), eq(ResumeReindexAction.NAME), any(ResumeBulkByScrollRequest.class), any());

        final Reindexer reindexer = reindexerWithRelocation(clusterService, transportService);
        final BulkByScrollTask task = createTaskWithParentIdAndRelocationEnabled(TaskId.EMPTY_TASK_ID);
        task.setWorker(Float.POSITIVE_INFINITY, null);
        task.getWorkerState().setNodeToRelocateToSupplier(() -> Optional.of("target-node"));
        task.requestRelocation();

        final PlainActionFuture<BulkByScrollResponse> future = new PlainActionFuture<>();
        final ActionListener<ResumeBulkByScrollResponse> resumeListener = spy(ActionListener.noop());
        final ActionListener<BulkByScrollResponse> wrapped = reindexer.listenerWithRelocations(
            task,
            reindexRequest(),
            resumeListener,
            future
        );
        wrapped.onResponse(reindexResponseWithResumeInfo());

        assertTrue(future.isDone());
        verify(transportService).sendRequest(eq(targetNode), eq(ResumeReindexAction.NAME), any(ResumeBulkByScrollRequest.class), any());
        verify(resumeListener).onResponse(any());
        verifyNoMoreInteractions(resumeListener);
    }

    public void testExecuteStoresSourceTaskResult() throws Exception {
        assumeTrue("reindex resilience enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final TaskId sourceTaskId = new TaskId("source-node", 42);
        final ResumeInfo.RelocationOrigin origin = new ResumeInfo.RelocationOrigin(sourceTaskId, System.currentTimeMillis());
        final BulkByScrollTask task = createTaskWithParentIdAndRelocationEnabled(TaskId.EMPTY_TASK_ID);
        task.setWorker(Float.POSITIVE_INFINITY, null);

        final TaskResultsService taskResultsService = mock(TaskResultsService.class);
        doAnswer(invocation -> {
            TaskResult stored = invocation.getArgument(0);
            assertThat(stored.getTask().taskId(), equalTo(new TaskId("source-node", task.getId())));
            assertThat(stored.getTask().action(), equalTo("test_action"));
            final Map<String, Object> errorMap = stored.getErrorAsMap();
            assertThat(errorMap.get("type"), equalTo("task_relocated_exception"));
            assertThat(errorMap.get("original_task_id"), equalTo(sourceTaskId.toString()));
            assertThat(errorMap.get("relocated_task_id"), equalTo("dest-node:" + task.getId()));
            invocation.<ActionListener<Void>>getArgument(1).onResponse(null);
            return null;
        }).when(taskResultsService).storeResult(any(TaskResult.class), any());

        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.builder("dest-node").build());
        final Reindexer reindexer = reindexerWithRelocation(clusterService, mock(TransportService.class), taskResultsService);

        final TaskResult sourceTaskResult = task.result(DiscoveryNodeUtils.builder("source-node").build(), new TaskRelocatedException());
        final var workerResumeInfo = new ResumeInfo.ScrollWorkerResumeInfo(
            "test-scroll-id",
            System.currentTimeMillis(),
            new BulkByScrollTask.Status(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, timeValueMillis(0), 0f, null, timeValueMillis(0)),
            null
        );
        final ReindexRequest request = reindexRequest();
        request.setResumeInfo(new ResumeInfo(origin, workerResumeInfo, null, sourceTaskResult));

        final PlainActionFuture<BulkByScrollResponse> future = new PlainActionFuture<>();
        reindexer.execute(task, request, mock(Client.class), future);

        verify(taskResultsService).storeResult(any(TaskResult.class), any());
    }

    public void testRelocationsSetsHandoffFlag() {
        assumeTrue("reindex resilience enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterState clusterState = mock(ClusterState.class);
        final DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        final DiscoveryNode sourceNode = DiscoveryNodeUtils.builder("source-node").build();
        final DiscoveryNode targetNode = DiscoveryNodeUtils.builder("target-node").build();
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.localNode()).thenReturn(sourceNode);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        when(discoveryNodes.get("target-node")).thenReturn(targetNode);

        final Reindexer reindexer = reindexerWithRelocation(clusterService, mock(TransportService.class));
        final BulkByScrollTask task = createTaskWithParentIdAndRelocationEnabled(TaskId.EMPTY_TASK_ID);
        task.setWorker(Float.POSITIVE_INFINITY, null);
        task.getWorkerState().setNodeToRelocateToSupplier(() -> Optional.of("target-node"));
        task.requestRelocation();

        assertFalse("handoff flag should not be set before relocation", task.useCreateSemanticsForResultStorage());

        final PlainActionFuture<BulkByScrollResponse> future = new PlainActionFuture<>();
        final ActionListener<BulkByScrollResponse> wrapped = reindexer.listenerWithRelocations(
            task,
            reindexRequest(),
            ActionListener.noop(),
            future
        );
        wrapped.onResponse(reindexResponseWithResumeInfo());

        assertTrue("handoff flag should be set after relocation", task.useCreateSemanticsForResultStorage());
    }

    public void testExecuteFailsWhenSourceTaskResultStorageFails() throws Exception {
        assumeTrue("reindex resilience enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final TaskResultsService taskResultsService = mock(TaskResultsService.class);
        final Exception storageFailure = new RuntimeException("simulated .tasks write failure");
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(1);
            listener.onFailure(storageFailure);
            return null;
        }).when(taskResultsService).storeResult(any(TaskResult.class), any());

        final TaskId sourceTaskId = new TaskId("source-node", 42);
        final ResumeInfo.RelocationOrigin origin = new ResumeInfo.RelocationOrigin(sourceTaskId, System.currentTimeMillis());
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.builder("dest-node").build());
        final Reindexer reindexer = reindexerWithRelocation(clusterService, mock(TransportService.class), taskResultsService);
        final BulkByScrollTask task = createTaskWithParentIdAndRelocationEnabled(TaskId.EMPTY_TASK_ID);
        task.setWorker(Float.POSITIVE_INFINITY, null);

        final TaskResult sourceTaskResult = task.result(DiscoveryNodeUtils.builder("source-node").build(), new TaskRelocatedException());
        final var workerResumeInfo = new ResumeInfo.ScrollWorkerResumeInfo(
            "test-scroll-id",
            System.currentTimeMillis(),
            new BulkByScrollTask.Status(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, timeValueMillis(0), 0f, null, timeValueMillis(0)),
            null
        );
        final ReindexRequest request = reindexRequest();
        request.setResumeInfo(new ResumeInfo(origin, workerResumeInfo, null, sourceTaskResult));

        final PlainActionFuture<BulkByScrollResponse> future = new PlainActionFuture<>();
        reindexer.execute(task, request, mock(Client.class), future);

        assertTrue(future.isDone());
        ExecutionException e = expectThrows(ExecutionException.class, future::get);
        assertSame(storageFailure, e.getCause());
    }

    /**
     * When the response has TaskResumeInfo (relocation), wrapListenerWithClosePit must not close the PIT.
     */
    public void testWrapListenerWithClosePitDoesNotCloseOnResponseWithResumeInfo() {
        final AtomicInteger closeCount = new AtomicInteger(0);
        final ActionListener<BulkByScrollResponse> delegate = spy(ActionListener.noop());
        final BytesReference pitId = new BytesArray("pit-id");
        final ActionListener<BulkByScrollResponse> wrapped = Reindexer.wrapListenerWithClosePit(
            pitId,
            delegate,
            id -> closeCount.incrementAndGet()
        );

        wrapped.onResponse(reindexResponseWithResumeInfo());

        verify(delegate).onResponse(any());
        assertThat(closeCount.get(), equalTo(0));
    }

    /**
     * When the failure is TaskRelocatedException, wrapListenerWithClosePit must not close the PIT.
     */
    public void testWrapListenerWithClosePitDoesNotCloseOnTaskRelocatedException() {
        final AtomicInteger closeCount = new AtomicInteger(0);
        final ActionListener<BulkByScrollResponse> delegate = spy(ActionListener.noop());
        final BytesReference pitId = new BytesArray("pit-id");
        final ActionListener<BulkByScrollResponse> wrapped = Reindexer.wrapListenerWithClosePit(
            pitId,
            delegate,
            id -> closeCount.incrementAndGet()
        );

        wrapped.onFailure(new TaskRelocatedException());

        verify(delegate).onFailure(any());
        assertThat(closeCount.get(), equalTo(0));
    }

    /**
     * When the response has no TaskResumeInfo, wrapListenerWithClosePit must close the PIT.
     */
    public void testWrapListenerWithClosePitClosesOnNormalResponse() {
        final AtomicInteger closeCount = new AtomicInteger(0);
        final ActionListener<BulkByScrollResponse> delegate = spy(ActionListener.noop());
        final BytesReference pitId = new BytesArray("pit-id");
        final ActionListener<BulkByScrollResponse> wrapped = Reindexer.wrapListenerWithClosePit(
            pitId,
            delegate,
            id -> closeCount.incrementAndGet()
        );

        wrapped.onResponse(reindexResponseWithBulkAndSearchFailures(null, null));

        verify(delegate).onResponse(any());
        assertThat(closeCount.get(), equalTo(1));
    }

    /**
     * When the response has a pitId, wrapListenerWithClosePit must close using the response's pitId (latest).
     */
    public void testWrapListenerWithClosePitUsesResponsePitIdWhenPresent() {
        final BytesReference initialPitId = new BytesArray("initial-pit-id");
        final BytesReference latestPitId = new BytesArray("latest-pit-id");
        final BytesReference[] closedPitId = new BytesReference[1];
        final ActionListener<BulkByScrollResponse> delegate = spy(ActionListener.noop());
        final ActionListener<BulkByScrollResponse> wrapped = Reindexer.wrapListenerWithClosePit(
            initialPitId,
            delegate,
            id -> closedPitId[0] = id
        );

        wrapped.onResponse(reindexResponseWithPitId(latestPitId));

        verify(delegate).onResponse(any());
        assertThat(closedPitId[0], equalTo(latestPitId));
    }

    /**
     * When the response has no pitId, wrapListenerWithClosePit must fall back to the initial pitId.
     */
    public void testWrapListenerWithClosePitFallsBackToInitialPitIdWhenResponseHasNone() {
        final BytesReference initialPitId = new BytesArray("initial-pit-id");
        final BytesReference[] closedPitId = new BytesReference[1];
        final ActionListener<BulkByScrollResponse> delegate = spy(ActionListener.noop());
        final ActionListener<BulkByScrollResponse> wrapped = Reindexer.wrapListenerWithClosePit(
            initialPitId,
            delegate,
            id -> closedPitId[0] = id
        );

        wrapped.onResponse(reindexResponseWithBulkAndSearchFailures(null, null));

        verify(delegate).onResponse(any());
        assertThat(closedPitId[0], equalTo(initialPitId));
    }

    /**
     * When shouldNotCloseOnResponse returns true (e.g. sliced worker), wrapListenerWithClosePit must not close the PIT on response.
     */
    public void testWrapListenerWithClosePitDoesNotCloseOnResponseWhenShouldNotClose() {
        final AtomicInteger closeCount = new AtomicInteger(0);
        final ActionListener<BulkByScrollResponse> delegate = spy(ActionListener.noop());
        final BytesReference pitId = new BytesArray("pit-id");
        final ActionListener<BulkByScrollResponse> wrapped = Reindexer.wrapListenerWithClosePit(
            pitId,
            delegate,
            id -> closeCount.incrementAndGet(),
            null,
            () -> true
        );

        wrapped.onResponse(reindexResponseWithBulkAndSearchFailures(null, null));

        verify(delegate).onResponse(any());
        assertThat(closeCount.get(), equalTo(0));
    }

    /**
     * When the failure is not TaskRelocatedException, wrapListenerWithClosePit must close the PIT.
     */
    public void testWrapListenerWithClosePitClosesOnOtherFailure() {
        final AtomicInteger closeCount = new AtomicInteger(0);
        final ActionListener<BulkByScrollResponse> delegate = spy(ActionListener.noop());
        final BytesReference pitId = new BytesArray("pit-id");
        final ActionListener<BulkByScrollResponse> wrapped = Reindexer.wrapListenerWithClosePit(
            pitId,
            delegate,
            id -> closeCount.incrementAndGet()
        );

        wrapped.onFailure(new RuntimeException("other failure"));

        verify(delegate).onFailure(any());
        assertThat(closeCount.get(), equalTo(1));
    }

    /**
     * When the task is non-null but does not have relocation requested, wrapListenerWithClosePit must close the PIT on failure.
     */
    public void testWrapListenerWithClosePitClosesOnFailureWhenTaskHasNoRelocationRequested() {
        final AtomicInteger closeCount = new AtomicInteger(0);
        final ActionListener<BulkByScrollResponse> delegate = spy(ActionListener.noop());
        final BytesReference pitId = new BytesArray("pit-id");
        final BulkByScrollTask task = createTaskWithParentIdAndRelocationEnabled(new TaskId("node", 1));
        task.setWorker(Float.POSITIVE_INFINITY, 0);
        // Do not call requestRelocation() - task.isRelocationRequested() is false

        final ActionListener<BulkByScrollResponse> wrapped = Reindexer.wrapListenerWithClosePit(
            pitId,
            delegate,
            id -> closeCount.incrementAndGet(),
            task,
            () -> false
        );

        wrapped.onFailure(new RuntimeException("other failure"));

        verify(delegate).onFailure(any());
        assertThat(closeCount.get(), equalTo(1));
    }

    /**
     * When the task has relocation requested and the failure is TaskCancelledException,
     * wrapListenerWithClosePit must not close the PIT (relocated task will use it).
     */
    public void testWrapListenerWithClosePitDoesNotCloseOnCancellationDuringRelocation() {
        final AtomicInteger closeCount = new AtomicInteger(0);
        final ActionListener<BulkByScrollResponse> delegate = spy(ActionListener.noop());
        final BytesReference pitId = new BytesArray("pit-id");
        final BulkByScrollTask task = createTaskWithParentIdAndRelocationEnabled(new TaskId("node", 1));
        task.setWorker(Float.POSITIVE_INFINITY, 0);
        task.requestRelocation();

        final ActionListener<BulkByScrollResponse> wrapped = Reindexer.wrapListenerWithClosePit(
            pitId,
            delegate,
            id -> closeCount.incrementAndGet(),
            task,
            () -> true
        );

        wrapped.onFailure(new TaskCancelledException("cancelled during relocation"));

        verify(delegate).onFailure(any());
        assertThat(closeCount.get(), equalTo(0));
    }

    /**
     * When a worker with PIT already set completes normally, the PIT must be closed.
     */
    public void testWorkerWithPitAlreadySetClosesPitOnCompletion() {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final OpenPitCapturingClient client = new OpenPitCapturingClient(getTestName());
        try {
            final TestThreadPool threadPool = new TestThreadPool(getTestName()) {
                @Override
                public ExecutorService executor(String name) {
                    return DIRECT_EXECUTOR_SERVICE;
                }
            };
            try {
                final ProjectResolver projectResolver = mock(ProjectResolver.class);
                when(projectResolver.getProjectState(any())).thenReturn(ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID));

                FeatureService featureService = mock(FeatureService.class);
                when(featureService.clusterHasFeature(any(), eq(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(true);

                final Reindexer reindexer = new Reindexer(
                    clusterServiceMock(),
                    projectResolver,
                    client,
                    threadPool,
                    mock(ScriptService.class),
                    mock(ReindexSslConfig.class),
                    null,
                    mock(TransportService.class),
                    mock(ReindexRelocationNodePicker.class),
                    featureService,
                    mock(TaskResultsService.class)
                );

                // Simulate a worker request: PIT already set by leader, slice info from leader
                final int sliceMax = randomIntBetween(2, 10);
                final ReindexRequest request = new ReindexRequest();
                request.setDestIndex("dest");
                request.setSlices(1);
                request.getSearchRequest().indices(Strings.EMPTY_ARRAY);
                request.getSearchRequest()
                    .source(
                        new SearchSourceBuilder().pointInTimeBuilder(
                            new PointInTimeBuilder(new BytesArray(randomAlphaOfLengthBetween(4, 16))).setKeepAlive(
                                TimeValue.timeValueMinutes(randomIntBetween(1, 15))
                            )
                        ).slice(new SliceBuilder(IdFieldMapper.NAME, 0, sliceMax))
                    );

                final BulkByScrollTask task = new BulkByScrollTask(
                    randomTaskId(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false,
                    randomOrigin()
                );

                final PlainActionFuture<BulkByScrollResponse> initFuture = new PlainActionFuture<>();
                reindexer.initTask(task, request, initFuture.delegateFailure((l, v) -> reindexer.execute(task, request, client, l)));
                initFuture.actionGet();

                assertNull("Worker with PIT already set must not open a new PIT", client.getCapturedPitRequest());
                assertNotNull(initFuture.actionGet());
                assertThat("PIT must be closed when worker completes", client.getCloseCount(), equalTo(1));
            } finally {
                terminate(threadPool);
            }
        } finally {
            client.shutdown();
        }
    }

    /**
     * When the remote version lookup fails in lookupRemoteVersionAndExecute
     * (e.g. server returns 500), the failure propagates to the listener.
     * Uses MockHttpServer instead of a non-connectable host to avoid unreliable connection timeouts.
     */
    @SuppressForbidden(reason = "use http server for testing")
    public void testRemoteReindexingRequestFailsWhenVersionLookupFails() throws Exception {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        HttpServer server = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(INTERNAL_SERVER_ERROR.getStatus(), -1);
            exchange.close();
        });
        server.start();
        try {
            runRemotePitTestWithMockServer(server, request -> request.setMaxRetries(0), initFuture -> {
                ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, initFuture::actionGet);
                assertThat(e.status(), equalTo(INTERNAL_SERVER_ERROR));
            });
        } finally {
            server.stop(0);
        }
    }

    /**
     * When the remote version lookup is rejected (429), the failure propagates to the listener
     * after retries are exhausted.
     */
    @SuppressForbidden(reason = "use http server for testing")
    public void testRemoteReindexingRequestFailsWhenVersionLookupRejected() throws Exception {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        HttpServer server = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(TOO_MANY_REQUESTS.getStatus(), -1);
            exchange.close();
        });
        server.start();
        try {
            runRemotePitTestWithMockServer(server, request -> request.setMaxRetries(0), initFuture -> {
                ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, initFuture::actionGet);
                assertThat(e.status(), equalTo(TOO_MANY_REQUESTS));
            });
        } finally {
            server.stop(0);
        }
    }

    /**
     * When opening the remote PIT fails in openRemotePitAndExecute, the failure propagates to the listener.
     */
    @SuppressForbidden(reason = "use http server for testing")
    public void testRemoteReindexingRequestFailsToOpenPit() throws Exception {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        AtomicInteger requestCount = new AtomicInteger(0);
        HttpServer server = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            int count = requestCount.getAndIncrement();
            if (count == 0) {
                respondJson(exchange, 200, REMOTE_PIT_TEST_VERSION_JSON);
            } else {
                exchange.sendResponseHeaders(500, -1);
            }
            exchange.close();
        });
        server.start();
        try {
            runRemotePitTestWithMockServer(server, request -> {}, initFuture -> {
                ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, initFuture::actionGet);
                assertThat(e.status(), equalTo(INTERNAL_SERVER_ERROR));
            });
        } finally {
            server.stop(0);
        }
    }

    /**
     * When closing the remote PIT fails in openRemotePitAndExecute, the failure is logged
     * but the main listener still receives success.
     */
    @SuppressForbidden(reason = "use http server for testing")
    public void testRemoteReindexingRequestFailsToClosePit() throws Exception {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        HttpServer server = createRemotePitMockServer((path, method) -> path.contains("_pit") && "DELETE".equals(method), exchange -> {
            try {
                exchange.sendResponseHeaders(500, -1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        server.start();
        try {
            MockLog.awaitLogger(() -> {
                try {
                    runRemotePitTestWithMockServer(server, request -> {}, initFuture -> {
                        BulkByScrollResponse response = initFuture.actionGet();
                        assertNotNull(response);
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            },
                Reindexer.class,
                new MockLog.SeenEventExpectation(
                    "Failed to close remote PIT should be logged",
                    Reindexer.class.getCanonicalName(),
                    Level.WARN,
                    "Failed to close remote PIT"
                )
            );
        } finally {
            server.stop(0);
        }
    }

    /**
     * When closing the remote PIT is rejected (429) in openRemotePitAndExecute,
     * the rejection is logged but the main listener still receives success.
     */
    @SuppressForbidden(reason = "use http server for testing")
    public void testRemoteReindexingRequestFailsWhenClosePitIsRejected() throws Exception {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        HttpServer server = createRemotePitMockServer((path, method) -> path.contains("_pit") && "DELETE".equals(method), exchange -> {
            try {
                exchange.sendResponseHeaders(TOO_MANY_REQUESTS.getStatus(), -1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        server.start();
        try {
            MockLog.awaitLogger(() -> {
                try {
                    runRemotePitTestWithMockServer(server, request -> {}, initFuture -> {
                        BulkByScrollResponse response = initFuture.actionGet();
                        assertNotNull(response);
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            },
                Reindexer.class,
                new MockLog.SeenEventExpectation(
                    "Failed to close remote PIT (rejected) should be logged",
                    Reindexer.class.getCanonicalName(),
                    Level.WARN,
                    "Failed to close remote PIT (rejected)"
                )
            );
        } finally {
            server.stop(0);
        }
    }

    /**
     * When TransportOpenPointInTimeAction fails in openPitAndExecute, the failure propagates to the listener.
     * We use a custom Client that fails on OpenPointInTimeRequest; the listener receives that failure.
     */
    public void testLocalReindexingRequestFailsToOpenPit() {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final String expectedMessage = "open-pit-failure-" + randomAlphaOfLength(8);
        final OpenPitFailingClient client = new OpenPitFailingClient(getTestName(), expectedMessage);
        try {
            final ThreadPool threadPool = mock(ThreadPool.class);
            when(threadPool.generic()).thenReturn(DIRECT_EXECUTOR_SERVICE);

            final ClusterService clusterService = mock(ClusterService.class);
            final DiscoveryNode localNode = DiscoveryNodeUtils.builder("local-node").build();
            when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
            when(clusterService.localNode()).thenReturn(localNode);
            when(clusterService.getSettings()).thenReturn(Settings.EMPTY);

            final ProjectResolver projectResolver = mock(ProjectResolver.class);
            when(projectResolver.getProjectState(any())).thenReturn(ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID));

            FeatureService featureService = mock(FeatureService.class);
            when(featureService.clusterHasFeature(any(), eq(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(true);

            final Reindexer reindexer = new Reindexer(
                clusterService,
                projectResolver,
                client,
                threadPool,
                mock(ScriptService.class),
                mock(ReindexSslConfig.class),
                null,
                mock(TransportService.class),
                mock(ReindexRelocationNodePicker.class),
                featureService,
                mock(TaskResultsService.class)
            );

            final ReindexRequest request = new ReindexRequest();
            request.setSourceIndices("source");
            request.setDestIndex("dest");
            request.setSlices(1);

            final BulkByScrollTask task = new BulkByScrollTask(
                randomTaskId(),
                "reindex",
                "reindex",
                "test",
                TaskId.EMPTY_TASK_ID,
                Collections.emptyMap(),
                false,
                randomOrigin()
            );

            final PlainActionFuture<BulkByScrollResponse> initFuture = new PlainActionFuture<>();
            reindexer.initTask(task, request, initFuture.delegateFailure((l, v) -> reindexer.execute(task, request, client, l)));
            initFuture.actionGet();

            fail("expected listener to receive failure");
        } catch (Exception e) {
            assertThat(ExceptionsHelper.unwrapCause(e).getMessage(), containsString(expectedMessage));
        } finally {
            client.shutdown();
        }
    }

    /**
     * When PIT search is enabled and the local PIT close fails, the failure is logged but the main listener
     * still receives success. This verifies that close failures are handled gracefully and don't propagate.
     */
    public void testLocalReindexingRequestFailsToClosePit() {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final String closeFailureMessage = "close-pit-failure-" + randomAlphaOfLength(8);
        final ClosePitFailingClient client = new ClosePitFailingClient(getTestName(), closeFailureMessage);
        try {
            final TestThreadPool threadPool = new TestThreadPool(getTestName()) {
                @Override
                public ExecutorService executor(String name) {
                    return DIRECT_EXECUTOR_SERVICE;
                }
            };
            try {
                final ProjectResolver projectResolver = mock(ProjectResolver.class);
                when(projectResolver.getProjectState(any())).thenReturn(ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID));

                FeatureService featureService = mock(FeatureService.class);
                when(featureService.clusterHasFeature(any(), eq(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(true);

                final Reindexer reindexer = new Reindexer(
                    clusterServiceMock(),
                    projectResolver,
                    client,
                    threadPool,
                    mock(ScriptService.class),
                    mock(ReindexSslConfig.class),
                    null,
                    mock(TransportService.class),
                    mock(ReindexRelocationNodePicker.class),
                    featureService,
                    mock(TaskResultsService.class)
                );

                final ReindexRequest request = new ReindexRequest();
                request.setSourceIndices("source");
                request.setDestIndex("dest");
                request.setSlices(1);

                final BulkByScrollTask task = new BulkByScrollTask(
                    randomTaskId(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false,
                    randomOrigin()
                );

                MockLog.awaitLogger(() -> {
                    final PlainActionFuture<BulkByScrollResponse> initFuture = new PlainActionFuture<>();
                    reindexer.initTask(task, request, initFuture.delegateFailure((l, v) -> reindexer.execute(task, request, client, l)));
                    final BulkByScrollResponse response = initFuture.actionGet();
                    assertNotNull(response);
                },
                    Reindexer.class,
                    new MockLog.SeenEventExpectation(
                        "Failed to close local PIT should be logged",
                        Reindexer.class.getCanonicalName(),
                        Level.WARN,
                        "Failed to close local PIT"
                    )
                );
            } finally {
                terminate(threadPool);
            }
        } finally {
            client.shutdown();
        }
    }

    /**
     * Verifies that the OpenPointInTimeRequest built in openPitAndExecute has routing and preference unset,
     * and allowPartialSearchResults explicitly set to false.
     */
    public void testLocalOpenPitRequestHasExpectedProperties() {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final OpenPitCapturingClient client = new OpenPitCapturingClient(getTestName());
        try {
            final TestThreadPool threadPool = new TestThreadPool(getTestName()) {
                @Override
                public ExecutorService executor(String name) {
                    return DIRECT_EXECUTOR_SERVICE;
                }
            };
            try {
                final ProjectResolver projectResolver = mock(ProjectResolver.class);
                when(projectResolver.getProjectState(any())).thenReturn(ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID));

                FeatureService featureService = mock(FeatureService.class);
                when(featureService.clusterHasFeature(any(), eq(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(true);

                final Reindexer reindexer = new Reindexer(
                    clusterServiceMock(),
                    projectResolver,
                    client,
                    threadPool,
                    mock(ScriptService.class),
                    mock(ReindexSslConfig.class),
                    null,
                    mock(TransportService.class),
                    mock(ReindexRelocationNodePicker.class),
                    featureService,
                    mock(TaskResultsService.class)
                );

                final ReindexRequest request = new ReindexRequest();
                request.setSourceIndices("source");
                request.setDestIndex("dest");
                request.setSlices(1);

                final BulkByScrollTask task = new BulkByScrollTask(
                    randomTaskId(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false,
                    randomOrigin()
                );

                final PlainActionFuture<BulkByScrollResponse> initFuture = new PlainActionFuture<>();
                reindexer.initTask(task, request, initFuture.delegateFailure((l, v) -> reindexer.execute(task, request, client, l)));
                initFuture.actionGet();

                OpenPointInTimeRequest pitRequest = client.getCapturedPitRequest();
                assertNotNull("Expected OpenPointInTimeRequest to have been captured", pitRequest);
                assertNull("routing should not be set", pitRequest.routing());
                assertNull("preference should not be set", pitRequest.preference());
                assertFalse("allowPartialSearchResults should be false", pitRequest.allowPartialSearchResults());
            } finally {
                terminate(threadPool);
            }
        } finally {
            client.shutdown();
        }
    }

    /**
     * This tests that when a source query is provided, the open pit request includes an index filter.
     * The case when the source query is null is tested in {@link #testLocalOpenPitRequestHasExpectedProperties} above
     */
    public void testLocalOpenPitSetsIndexFilterFromSourceQuery() {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final OpenPitCapturingClient client = new OpenPitCapturingClient(getTestName());
        try {
            final TestThreadPool threadPool = new TestThreadPool(getTestName()) {
                @Override
                public ExecutorService executor(String name) {
                    return DIRECT_EXECUTOR_SERVICE;
                }
            };
            try {
                final ProjectResolver projectResolver = mock(ProjectResolver.class);
                when(projectResolver.getProjectState(any())).thenReturn(ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID));

                FeatureService featureService = mock(FeatureService.class);
                when(featureService.clusterHasFeature(any(), eq(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(true);

                final Reindexer reindexer = new Reindexer(
                    clusterServiceMock(),
                    projectResolver,
                    client,
                    threadPool,
                    mock(ScriptService.class),
                    mock(ReindexSslConfig.class),
                    null,
                    mock(TransportService.class),
                    mock(ReindexRelocationNodePicker.class),
                    featureService,
                    mock(TaskResultsService.class)
                );

                final var termQuery = QueryBuilders.termQuery("field", "value");
                final ReindexRequest request = new ReindexRequest();
                request.setSourceIndices("source");
                request.setDestIndex("dest");
                request.setSlices(1);
                request.getSearchRequest().source().query(termQuery);

                final BulkByScrollTask task = new BulkByScrollTask(
                    randomTaskId(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false,
                    randomOrigin()
                );

                final PlainActionFuture<BulkByScrollResponse> initFuture = new PlainActionFuture<>();
                reindexer.initTask(task, request, initFuture.delegateFailure((l, v) -> reindexer.execute(task, request, client, l)));
                initFuture.actionGet();

                OpenPointInTimeRequest pitRequest = client.getCapturedPitRequest();
                assertNotNull(pitRequest);
                assertSame(termQuery, pitRequest.indexFilter());
            } finally {
                terminate(threadPool);
            }
        } finally {
            client.shutdown();
        }
    }

    /**
     * Cross-project reindex: project routing must be applied when opening the PIT, then cleared on the search request
     * so PIT searches validate (see {@link org.elasticsearch.action.search.SearchRequest#validate()}).
     */
    public void testLocalOpenPitCopiesProjectRoutingAndClearsSearchRequest() {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final OpenPitCapturingClient client = new OpenPitCapturingClient(getTestName());
        try {
            final TestThreadPool threadPool = new TestThreadPool(getTestName()) {
                @Override
                public ExecutorService executor(String name) {
                    return DIRECT_EXECUTOR_SERVICE;
                }
            };
            try {
                final ProjectResolver projectResolver = mock(ProjectResolver.class);
                when(projectResolver.getProjectState(any())).thenReturn(ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID));

                FeatureService featureService = mock(FeatureService.class);
                when(featureService.clusterHasFeature(any(), eq(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(true);

                final Reindexer reindexer = new Reindexer(
                    clusterServiceMock(),
                    projectResolver,
                    client,
                    threadPool,
                    mock(ScriptService.class),
                    mock(ReindexSslConfig.class),
                    null,
                    mock(TransportService.class),
                    mock(ReindexRelocationNodePicker.class),
                    featureService,
                    mock(TaskResultsService.class)
                );

                final String projectRouting = "_alias:linked";
                final ReindexRequest request = new ReindexRequest();
                request.setSourceIndices("source");
                request.setDestIndex("dest");
                request.setSlices(1);
                request.getSearchRequest().setProjectRouting(projectRouting);

                final BulkByScrollTask task = new BulkByScrollTask(
                    randomTaskId(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false,
                    randomOrigin()
                );

                final PlainActionFuture<BulkByScrollResponse> initFuture = new PlainActionFuture<>();
                reindexer.initTask(task, request, initFuture.delegateFailure((l, v) -> reindexer.execute(task, request, client, l)));
                initFuture.actionGet();

                OpenPointInTimeRequest pitRequest = client.getCapturedPitRequest();
                assertNotNull(pitRequest);
                assertEquals(projectRouting, pitRequest.getProjectRouting());
                assertNull(request.getSearchRequest().getProjectRouting());
            } finally {
                terminate(threadPool);
            }
        } finally {
            client.shutdown();
        }
    }

    /**
     * When a worker receives a sliced request from the leader, the request already has PIT set.
     * The worker must skip openPitAndExecute and go straight to executePaginatedSearch.
     * Verifies that no OpenPointInTimeRequest is sent in this case.
     */
    public void testWorkerWithPitAlreadySetSkipsOpenPit() {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final OpenPitCapturingClient client = new OpenPitCapturingClient(getTestName());
        try {
            final TestThreadPool threadPool = new TestThreadPool(getTestName()) {
                @Override
                public ExecutorService executor(String name) {
                    return DIRECT_EXECUTOR_SERVICE;
                }
            };
            try {
                final ProjectResolver projectResolver = mock(ProjectResolver.class);
                when(projectResolver.getProjectState(any())).thenReturn(ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID));

                FeatureService featureService = mock(FeatureService.class);
                when(featureService.clusterHasFeature(any(), eq(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(true);

                final Reindexer reindexer = new Reindexer(
                    clusterServiceMock(),
                    projectResolver,
                    client,
                    threadPool,
                    mock(ScriptService.class),
                    mock(ReindexSslConfig.class),
                    null,
                    mock(TransportService.class),
                    mock(ReindexRelocationNodePicker.class),
                    featureService,
                    mock(TaskResultsService.class)
                );

                // Simulate a worker request: PIT already set by leader, slice info from leader
                final int sliceMax = randomIntBetween(2, 10);
                final ReindexRequest request = new ReindexRequest();
                request.setDestIndex("dest");
                request.setSlices(1);
                request.getSearchRequest().indices(Strings.EMPTY_ARRAY);
                request.getSearchRequest()
                    .source(
                        new SearchSourceBuilder().pointInTimeBuilder(
                            new PointInTimeBuilder(new BytesArray(randomAlphaOfLengthBetween(4, 16))).setKeepAlive(
                                TimeValue.timeValueMinutes(randomIntBetween(1, 15))
                            )
                        ).slice(new SliceBuilder(IdFieldMapper.NAME, 0, sliceMax))
                    );

                final BulkByScrollTask task = new BulkByScrollTask(
                    randomTaskId(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false,
                    randomOrigin()
                );

                final PlainActionFuture<BulkByScrollResponse> initFuture = new PlainActionFuture<>();
                reindexer.initTask(task, request, initFuture.delegateFailure((l, v) -> reindexer.execute(task, request, client, l)));
                initFuture.actionGet();

                assertNull("Worker with PIT already set must not open a new PIT", client.getCapturedPitRequest());
            } finally {
                terminate(threadPool);
            }
        } finally {
            client.shutdown();
        }
    }

    /**
     * Verifies that openPitAndExecute throws AssertionError when the SearchRequest has routing set.
     */
    public void testLocalOpenPitFailsWhenRoutingSet() {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final OpenPitCapturingClient client = new OpenPitCapturingClient(getTestName());
        try {
            final TestThreadPool threadPool = new TestThreadPool(getTestName()) {
                @Override
                public ExecutorService executor(String name) {
                    return DIRECT_EXECUTOR_SERVICE;
                }
            };
            try {
                final ProjectResolver projectResolver = mock(ProjectResolver.class);
                when(projectResolver.getProjectState(any())).thenReturn(ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID));

                FeatureService featureService = mock(FeatureService.class);
                when(featureService.clusterHasFeature(any(), eq(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(true);

                final Reindexer reindexer = new Reindexer(
                    clusterServiceMock(),
                    projectResolver,
                    client,
                    threadPool,
                    mock(ScriptService.class),
                    mock(ReindexSslConfig.class),
                    null,
                    mock(TransportService.class),
                    mock(ReindexRelocationNodePicker.class),
                    featureService,
                    mock(TaskResultsService.class)
                );

                final ReindexRequest request = new ReindexRequest();
                request.setSourceIndices("source");
                request.setDestIndex("dest");
                request.setSlices(1);
                request.getSearchRequest().routing("r1");

                final BulkByScrollTask task = new BulkByScrollTask(
                    randomTaskId(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false,
                    randomOrigin()
                );

                final PlainActionFuture<BulkByScrollResponse> initFuture = new PlainActionFuture<>();
                Throwable e = expectThrows(
                    Throwable.class,
                    () -> reindexer.initTask(
                        task,
                        request,
                        initFuture.delegateFailure((l, v) -> reindexer.execute(task, request, client, l))
                    )
                );
                assertThat(ExceptionsHelper.unwrapCause(e).getMessage(), containsString("Routing is set in the search request"));
            } finally {
                terminate(threadPool);
            }
        } finally {
            client.shutdown();
        }
    }

    /**
     * Verifies that openPitAndExecute throws AssertionError when the SearchRequest has preference set.
     */
    public void testLocalOpenPitFailsWhenPreferenceSet() {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final OpenPitCapturingClient client = new OpenPitCapturingClient(getTestName());
        try {
            final TestThreadPool threadPool = new TestThreadPool(getTestName()) {
                @Override
                public ExecutorService executor(String name) {
                    return DIRECT_EXECUTOR_SERVICE;
                }
            };
            try {
                final ProjectResolver projectResolver = mock(ProjectResolver.class);
                when(projectResolver.getProjectState(any())).thenReturn(ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID));

                FeatureService featureService = mock(FeatureService.class);
                when(featureService.clusterHasFeature(any(), eq(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(true);

                final Reindexer reindexer = new Reindexer(
                    clusterServiceMock(),
                    projectResolver,
                    client,
                    threadPool,
                    mock(ScriptService.class),
                    mock(ReindexSslConfig.class),
                    null,
                    mock(TransportService.class),
                    mock(ReindexRelocationNodePicker.class),
                    featureService,
                    mock(TaskResultsService.class)
                );

                final ReindexRequest request = new ReindexRequest();
                request.setSourceIndices("source");
                request.setDestIndex("dest");
                request.setSlices(1);
                request.getSearchRequest().preference("_local");

                final BulkByScrollTask task = new BulkByScrollTask(
                    randomTaskId(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false,
                    randomOrigin()
                );

                final PlainActionFuture<BulkByScrollResponse> initFuture = new PlainActionFuture<>();
                Throwable e = expectThrows(
                    Throwable.class,
                    () -> reindexer.initTask(
                        task,
                        request,
                        initFuture.delegateFailure((l, v) -> reindexer.execute(task, request, client, l))
                    )
                );
                assertThat(ExceptionsHelper.unwrapCause(e).getMessage(), containsString("Preference is set in the search request"));
            } finally {
                terminate(threadPool);
            }
        } finally {
            client.shutdown();
        }
    }

    /**
     * Verifies that openPitAndExecute throws AssertionError when the SearchRequest has allowPartialSearchResults set to true.
     */
    public void testLocalOpenPitFailsWhenAllowPartialSearchResultsTrue() {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final OpenPitCapturingClient client = new OpenPitCapturingClient(getTestName());
        try {
            final TestThreadPool threadPool = new TestThreadPool(getTestName()) {
                @Override
                public ExecutorService executor(String name) {
                    return DIRECT_EXECUTOR_SERVICE;
                }
            };
            try {
                final ProjectResolver projectResolver = mock(ProjectResolver.class);
                when(projectResolver.getProjectState(any())).thenReturn(ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID));

                FeatureService featureService = mock(FeatureService.class);
                when(featureService.clusterHasFeature(any(), eq(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(true);

                final Reindexer reindexer = new Reindexer(
                    clusterServiceMock(),
                    projectResolver,
                    client,
                    threadPool,
                    mock(ScriptService.class),
                    mock(ReindexSslConfig.class),
                    null,
                    mock(TransportService.class),
                    mock(ReindexRelocationNodePicker.class),
                    featureService,
                    mock(TaskResultsService.class)
                );

                final ReindexRequest request = new ReindexRequest();
                request.setSourceIndices("source");
                request.setDestIndex("dest");
                request.setSlices(1);
                request.getSearchRequest().allowPartialSearchResults(true);

                final BulkByScrollTask task = new BulkByScrollTask(
                    randomTaskId(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false,
                    randomOrigin()
                );

                final PlainActionFuture<BulkByScrollResponse> initFuture = new PlainActionFuture<>();
                Throwable e = expectThrows(
                    Throwable.class,
                    () -> reindexer.initTask(
                        task,
                        request,
                        initFuture.delegateFailure((l, v) -> reindexer.execute(task, request, client, l))
                    )
                );
                assertThat(
                    ExceptionsHelper.unwrapCause(e).getMessage(),
                    containsString("allow_partial_search_results must be false when opening a PIT")
                );
            } finally {
                terminate(threadPool);
            }
        } finally {
            client.shutdown();
        }
    }

    /**
     * When PIT search is enabled and reindex uses multiple slices, the leader opens a single point-in-time context and slice workers
     * share it. Verifies there is exactly one {@code OpenPointInTime} and one {@code ClosePointInTime} request on the local client
     */
    public void testLocalSlicedReindexOpensPitOnceAndClosesOnce() {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final TestThreadPool threadPool = new TestThreadPool(getTestName()) {
            @Override
            public ExecutorService executor(String name) {
                return DIRECT_EXECUTOR_SERVICE;
            }
        };
        try {
            final ClusterService clusterService = mock(ClusterService.class);
            final DiscoveryNode localNode = DiscoveryNodeUtils.builder("local-node").build();
            when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
            when(clusterService.localNode()).thenReturn(localNode);

            final ProjectResolver projectResolver = mock(ProjectResolver.class);
            when(projectResolver.getProjectState(any())).thenReturn(ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID));

            final FeatureService featureService = mock(FeatureService.class);
            when(featureService.clusterHasFeature(any(), eq(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(true);

            final long leaderTaskId = randomLongBetween(1_000L, Long.MAX_VALUE / 4);
            final long firstChildTaskId = leaderTaskId + randomIntBetween(1, 10_000);
            final int numSlices = randomIntBetween(2, 5);

            final BulkByScrollTask leaderTask = new BulkByScrollTask(
                new TaskId("local-node", leaderTaskId),
                "reindex",
                ReindexAction.NAME,
                "test",
                TaskId.EMPTY_TASK_ID,
                Collections.emptyMap(),
                false,
                randomOrigin()
            );

            final TaskManager taskManager = mock(TaskManager.class);
            when(taskManager.getCancellableTasks()).thenReturn(Map.of(leaderTaskId, leaderTask));
            final TransportService transportService = mock(TransportService.class);
            when(transportService.getTaskManager()).thenReturn(taskManager);

            final AtomicReference<Reindexer> reindexerRef = new AtomicReference<>();
            final SlicedLocalPitReindexClient client = new SlicedLocalPitReindexClient(
                getTestName(),
                clusterService,
                reindexerRef,
                firstChildTaskId
            );
            try {
                final Reindexer reindexer = new Reindexer(
                    clusterServiceMock(),
                    projectResolver,
                    client,
                    threadPool,
                    mock(ScriptService.class),
                    mock(ReindexSslConfig.class),
                    null,
                    transportService,
                    mock(ReindexRelocationNodePicker.class),
                    featureService,
                    mock(TaskResultsService.class)
                );
                reindexerRef.set(reindexer);

                final ReindexRequest request = new ReindexRequest();
                request.setSourceIndices("source");
                request.setDestIndex("dest");
                request.setSlices(numSlices);

                final PlainActionFuture<BulkByScrollResponse> initFuture = new PlainActionFuture<>();
                reindexer.initTask(
                    leaderTask,
                    request,
                    initFuture.delegateFailure((l, v) -> reindexer.execute(leaderTask, request, client, l))
                );
                initFuture.actionGet();

                assertThat("one shared PIT open for the leader", client.getOpenPitCount(), equalTo(1));
                assertThat("leader closes PIT once after all " + numSlices + " slices", client.getCloseCount(), equalTo(1));
            } finally {
                client.shutdown();
            }
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Remote clusters before PIT support (below 7.10) must use scroll-based pagination. Verifies the mock server receives no
     * {@code POST} to open a point-in-time while a 7.9-style version response still allows reindex to complete using scroll.
     */
    @SuppressForbidden(reason = "use http server for testing")
    public void testRemoteReindexUsesScrollWhenVersionBeforePitSupport() throws Exception {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final AtomicInteger pitPostCount = new AtomicInteger();
        final String version79 = "{\"version\":{\"number\":\"7.9.0\"},\"tagline\":\"You Know, for Search\"}";
        final String scrollBody = "{\"_scroll_id\":\"fake-scroll\",\"timed_out\":false,\"hits\":{\"total\":0,\"hits\":[]}"
            + ",\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0}}";

        HttpServer server = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            try {
                String path = exchange.getRequestURI().getPath();
                String method = exchange.getRequestMethod();
                if (path.contains("_pit") && "POST".equals(method)) {
                    pitPostCount.incrementAndGet();
                    respondJson(exchange, 500, "{\"error\":\"unexpected pit\"}");
                    return;
                }
                if (path.equals("/") || path.isEmpty()) {
                    respondJson(exchange, 200, version79);
                    return;
                }
                if (path.contains("/_search") && "POST".equals(method) && path.contains("scroll") == false) {
                    respondJson(exchange, 200, scrollBody);
                    return;
                }
                if (path.contains("_search/scroll") && "POST".equals(method)) {
                    respondJson(exchange, 200, scrollBody);
                    return;
                }
                if (path.contains("_search/scroll") && "DELETE".equals(method)) {
                    exchange.sendResponseHeaders(200, -1);
                    return;
                }
                exchange.sendResponseHeaders(404, -1);
            } finally {
                exchange.close();
            }
        });
        server.start();
        try {
            runRemotePitTestWithMockServer(server, r -> {}, initFuture -> {
                BulkByScrollResponse response = initFuture.actionGet();
                assertNotNull(response);
                assertThat(pitPostCount.get(), equalTo(0));
            });
        } finally {
            server.stop(0);
        }
    }

    /**
     * Local PIT open uses the reindex request's scroll keep-alive as the PIT keep-alive when the user has set a scroll timeout.
     */
    public void testLocalOpenPitUsesScrollTimeAsKeepAliveWhenSet() {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final OpenPitCapturingClient client = new OpenPitCapturingClient(getTestName());
        try {
            final TestThreadPool threadPool = new TestThreadPool(getTestName()) {
                @Override
                public ExecutorService executor(String name) {
                    return DIRECT_EXECUTOR_SERVICE;
                }
            };
            try {
                final ProjectResolver projectResolver = mock(ProjectResolver.class);
                when(projectResolver.getProjectState(any())).thenReturn(ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID));

                final FeatureService featureService = mock(FeatureService.class);
                when(featureService.clusterHasFeature(any(), eq(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(true);

                final Reindexer reindexer = new Reindexer(
                    clusterServiceMock(),
                    projectResolver,
                    client,
                    threadPool,
                    mock(ScriptService.class),
                    mock(ReindexSslConfig.class),
                    null,
                    mock(TransportService.class),
                    mock(ReindexRelocationNodePicker.class),
                    featureService,
                    mock(TaskResultsService.class)
                );

                final int scrollMinutes = randomIntBetween(3, 20);
                final TimeValue scrollKeepAlive = TimeValue.timeValueMinutes(scrollMinutes);
                final ReindexRequest request = new ReindexRequest();
                request.setSourceIndices("source");
                request.setDestIndex("dest");
                request.setSlices(1);
                request.setScroll(scrollKeepAlive);

                final BulkByScrollTask task = new BulkByScrollTask(
                    randomTaskId(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false,
                    randomOrigin()
                );

                final PlainActionFuture<BulkByScrollResponse> initFuture = new PlainActionFuture<>();
                reindexer.initTask(task, request, initFuture.delegateFailure((l, v) -> reindexer.execute(task, request, client, l)));
                initFuture.actionGet();

                OpenPointInTimeRequest pitRequest = client.getCapturedPitRequest();
                assertNotNull(pitRequest);
                assertThat(pitRequest.keepAlive(), equalTo(scrollKeepAlive));
            } finally {
                terminate(threadPool);
            }
        } finally {
            client.shutdown();
        }
    }

    /**
     * When the search request has no scroll keep-alive, local PIT open uses the default keep-alive (five minutes), matching
     * {@link Reindexer}'s {@code pitKeepAlive} fallback.
     */
    public void testLocalOpenPitUsesDefaultKeepAliveWhenScrollTimeUnset() {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final OpenPitCapturingClient client = new OpenPitCapturingClient(getTestName());
        try {
            final TestThreadPool threadPool = new TestThreadPool(getTestName()) {
                @Override
                public ExecutorService executor(String name) {
                    return DIRECT_EXECUTOR_SERVICE;
                }
            };
            try {
                final ProjectResolver projectResolver = mock(ProjectResolver.class);
                when(projectResolver.getProjectState(any())).thenReturn(ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID));

                final FeatureService featureService = mock(FeatureService.class);
                when(featureService.clusterHasFeature(any(), eq(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(true);

                final Reindexer reindexer = new Reindexer(
                    clusterServiceMock(),
                    projectResolver,
                    client,
                    threadPool,
                    mock(ScriptService.class),
                    mock(ReindexSslConfig.class),
                    null,
                    mock(TransportService.class),
                    mock(ReindexRelocationNodePicker.class),
                    featureService,
                    mock(TaskResultsService.class)
                );

                final ReindexRequest request = new ReindexRequest();
                request.setSourceIndices("source");
                request.setDestIndex("dest");
                request.setSlices(1);
                request.getSearchRequest().scroll((TimeValue) null);

                final BulkByScrollTask task = new BulkByScrollTask(
                    randomTaskId(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false,
                    randomOrigin()
                );

                final PlainActionFuture<BulkByScrollResponse> initFuture = new PlainActionFuture<>();
                reindexer.initTask(task, request, initFuture.delegateFailure((l, v) -> reindexer.execute(task, request, client, l)));
                initFuture.actionGet();

                OpenPointInTimeRequest pitRequest = client.getCapturedPitRequest();
                assertNotNull(pitRequest);
                assertThat(pitRequest.keepAlive(), equalTo(TimeValue.timeValueMinutes(5)));
            } finally {
                terminate(threadPool);
            }
        } finally {
            client.shutdown();
        }
    }

    /**
     * {@link OpenPointInTimeRequest} must carry the same source index names and
     * {@link IndicesOptions} as the reindex search request so PIT resolution matches the user's source selection.
     */
    public void testLocalOpenPitRequestCopiesIndicesAndIndicesOptions() {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final OpenPitCapturingClient client = new OpenPitCapturingClient(getTestName());
        try {
            final TestThreadPool threadPool = new TestThreadPool(getTestName()) {
                @Override
                public ExecutorService executor(String name) {
                    return DIRECT_EXECUTOR_SERVICE;
                }
            };
            try {
                final ProjectResolver projectResolver = mock(ProjectResolver.class);
                when(projectResolver.getProjectState(any())).thenReturn(ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID));

                final FeatureService featureService = mock(FeatureService.class);
                when(featureService.clusterHasFeature(any(), eq(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(true);

                final Reindexer reindexer = new Reindexer(
                    clusterServiceMock(),
                    projectResolver,
                    client,
                    threadPool,
                    mock(ScriptService.class),
                    mock(ReindexSslConfig.class),
                    null,
                    mock(TransportService.class),
                    mock(ReindexRelocationNodePicker.class),
                    featureService,
                    mock(TaskResultsService.class)
                );

                final IndicesOptions indicesOptions = IndicesOptions.lenientExpandOpen();
                final ReindexRequest request = new ReindexRequest();
                request.setDestIndex("dest");
                request.setSlices(1);
                request.getSearchRequest().indices("idx-a", "idx-b");
                request.getSearchRequest().indicesOptions(indicesOptions);

                final BulkByScrollTask task = new BulkByScrollTask(
                    randomTaskId(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false,
                    randomOrigin()
                );

                final PlainActionFuture<BulkByScrollResponse> initFuture = new PlainActionFuture<>();
                reindexer.initTask(task, request, initFuture.delegateFailure((l, v) -> reindexer.execute(task, request, client, l)));
                initFuture.actionGet();

                OpenPointInTimeRequest pitRequest = client.getCapturedPitRequest();
                assertNotNull(pitRequest);
                assertThat(pitRequest.indices(), equalTo(new String[] { "idx-a", "idx-b" }));
                assertThat(pitRequest.indicesOptions(), equalTo(indicesOptions));
            } finally {
                terminate(threadPool);
            }
        } finally {
            client.shutdown();
        }
    }

    /**
     * If the cluster does not advertise {@link ReindexPlugin#REINDEX_PIT_SEARCH_FEATURE}, reindex stays on the scroll path and
     * must not invoke local open-PIT transport actions.
     */
    public void testLocalReindexDoesNotOpenPitWhenClusterFeatureDisabled() {
        final OpenPitCapturingClient client = new OpenPitCapturingClient(getTestName());
        try {
            final TestThreadPool threadPool = new TestThreadPool(getTestName()) {
                @Override
                public ExecutorService executor(String name) {
                    return DIRECT_EXECUTOR_SERVICE;
                }
            };
            try {
                final ProjectResolver projectResolver = mock(ProjectResolver.class);
                when(projectResolver.getProjectState(any())).thenReturn(ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID));

                final FeatureService featureService = mock(FeatureService.class);
                when(featureService.clusterHasFeature(any(), eq(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(false);

                final Reindexer reindexer = new Reindexer(
                    clusterServiceMock(),
                    projectResolver,
                    client,
                    threadPool,
                    mock(ScriptService.class),
                    mock(ReindexSslConfig.class),
                    null,
                    mock(TransportService.class),
                    mock(ReindexRelocationNodePicker.class),
                    featureService,
                    mock(TaskResultsService.class)
                );

                final ReindexRequest request = new ReindexRequest();
                request.setSourceIndices("source");
                request.setDestIndex("dest");
                request.setSlices(1);

                final BulkByScrollTask task = new BulkByScrollTask(
                    randomTaskId(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false,
                    randomOrigin()
                );

                final PlainActionFuture<BulkByScrollResponse> initFuture = new PlainActionFuture<>();
                reindexer.initTask(task, request, initFuture.delegateFailure((l, v) -> reindexer.execute(task, request, client, l)));
                initFuture.actionGet();

                assertThat(client.getOpenPitCount(), equalTo(0));
                assertNull(client.getCapturedPitRequest());
            } finally {
                terminate(threadPool);
            }
        } finally {
            client.shutdown();
        }
    }

    /**
     * After version negotiation, if the search request already contains a point-in-time id (e.g. resume or internal injection),
     * remote reindex must not send an extra {@code POST} to open PIT on the remote cluster.
     */
    @SuppressForbidden(reason = "use http server for testing")
    public void testRemoteReindexSkipsOpenPitWhenPointInTimeAlreadySet() throws Exception {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final AtomicInteger pitOpenPostCount = new AtomicInteger();
        HttpServer server = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            try {
                String path = exchange.getRequestURI().getPath();
                String method = exchange.getRequestMethod();
                if (path.contains("_pit") && "POST".equals(method)) {
                    pitOpenPostCount.incrementAndGet();
                    respondJson(exchange, 200, REMOTE_PIT_OPEN_RESPONSE);
                    return;
                }
                if (path.equals("/") || path.isEmpty()) {
                    respondJson(exchange, 200, REMOTE_PIT_TEST_VERSION_JSON);
                    return;
                }
                if (path.contains("_search") && "POST".equals(method)) {
                    respondJson(exchange, 200, REMOTE_PIT_EMPTY_SEARCH_RESPONSE);
                    return;
                }
                if (path.contains("_pit") && "DELETE".equals(method)) {
                    exchange.sendResponseHeaders(200, -1);
                    return;
                }
                exchange.sendResponseHeaders(404, -1);
            } finally {
                exchange.close();
            }
        });
        server.start();
        try {
            BytesArray matchAll = new BytesArray("{\"match_all\":{}}");
            RemoteInfo remoteInfo = new RemoteInfo(
                "http",
                server.getAddress().getHostString(),
                server.getAddress().getPort(),
                null,
                matchAll,
                null,
                null,
                emptyMap(),
                TimeValue.timeValueSeconds(5),
                TimeValue.timeValueSeconds(5)
            );

            ClusterService clusterService = mock(ClusterService.class);
            DiscoveryNode localNode = DiscoveryNodeUtils.builder("local-node").build();
            when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
            when(clusterService.localNode()).thenReturn(localNode);

            ProjectResolver projectResolver = mock(ProjectResolver.class);
            when(projectResolver.getProjectState(any())).thenReturn(ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID));

            TestThreadPool threadPool = new TestThreadPool(getTestName()) {
                @Override
                public ExecutorService executor(String name) {
                    return DIRECT_EXECUTOR_SERVICE;
                }
            };
            try {
                Environment environment = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());
                ReindexSslConfig sslConfig = new ReindexSslConfig(environment.settings(), environment, mock(ResourceWatcherService.class));

                FeatureService featureService = mock(FeatureService.class);
                when(featureService.clusterHasFeature(any(), eq(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(true);

                Reindexer reindexer = new Reindexer(
                    clusterServiceMock(),
                    projectResolver,
                    mock(Client.class),
                    threadPool,
                    mock(ScriptService.class),
                    sslConfig,
                    null,
                    mock(TransportService.class),
                    mock(ReindexRelocationNodePicker.class),
                    featureService,
                    mock(TaskResultsService.class)
                );

                ReindexRequest request = new ReindexRequest();
                request.setDestIndex("dest");
                request.setRemoteInfo(remoteInfo);
                request.setSlices(1);
                request.getSearchRequest().indices(Strings.EMPTY_ARRAY);
                request.getSearchRequest()
                    .source(
                        new SearchSourceBuilder().pointInTimeBuilder(
                            new PointInTimeBuilder(new BytesArray("c29tZXBpdGlk")).setKeepAlive(TimeValue.timeValueMinutes(5))
                        )
                    );
                // ReindexRequest applies default scroll; PIT and scroll cannot both be set.
                request.getSearchRequest().scroll(null);
                assertNull(request.validate());

                BulkByScrollTask task = new BulkByScrollTask(
                    randomTaskId(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false,
                    randomOrigin()
                );

                PlainActionFuture<BulkByScrollResponse> initFuture = new PlainActionFuture<>();
                reindexer.initTask(
                    task,
                    request,
                    initFuture.delegateFailure((l, v) -> reindexer.execute(task, request, mock(Client.class), l))
                );
                assertNotNull(initFuture.actionGet());
                assertThat("remote open PIT must be skipped when id already on request", pitOpenPostCount.get(), equalTo(0));
            } finally {
                terminate(threadPool);
            }
        } finally {
            server.stop(0);
        }
    }

    /**
     * When the remote cluster returns HTTP 429 (too many requests) for open PIT, the reindex request fails with that status
     * after retries are exhausted.
     */
    @SuppressForbidden(reason = "use http server for testing")
    public void testRemoteReindexingFailsWhenOpenPitIsRejected() throws Exception {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        AtomicInteger requestSeq = new AtomicInteger();
        HttpServer server = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            try {
                String path = exchange.getRequestURI().getPath();
                String method = exchange.getRequestMethod();
                int seq = requestSeq.getAndIncrement();
                if (path.equals("/") || path.isEmpty()) {
                    respondJson(exchange, 200, REMOTE_PIT_TEST_VERSION_JSON);
                    return;
                }
                if (path.contains("_pit") && "POST".equals(method)) {
                    if (seq >= 1) {
                        exchange.sendResponseHeaders(TOO_MANY_REQUESTS.getStatus(), -1);
                        return;
                    }
                }
                exchange.sendResponseHeaders(404, -1);
            } finally {
                exchange.close();
            }
        });
        server.start();
        try {
            runRemotePitTestWithMockServer(server, r -> r.setMaxRetries(0), initFuture -> {
                ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, initFuture::actionGet);
                assertThat(e.status(), equalTo(TOO_MANY_REQUESTS));
            });
        } finally {
            server.stop(0);
        }
    }

    /**
     * If {@link RestClient#close()} throws while shutting down after remote version or reindex work, the failure is logged at WARN
     * and the completion runnable still runs, so callers are not left hanging.
     */
    public void testCloseRestClientAndRunLogsWhenCloseThrows() throws IOException {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.generic()).thenReturn(DIRECT_EXECUTOR_SERVICE);
        Reindexer reindexer = new Reindexer(
            clusterServiceMock(),
            mock(ProjectResolver.class),
            mock(Client.class),
            threadPool,
            mock(ScriptService.class),
            mock(ReindexSslConfig.class),
            null,
            mock(TransportService.class),
            mock(ReindexRelocationNodePicker.class),
            mock(FeatureService.class),
            mock(TaskResultsService.class)
        );
        RestClient restClient = mock(RestClient.class);
        IOException ioException = new IOException("simulated close failure");
        doThrow(ioException).when(restClient).close();
        AtomicBoolean completion = new AtomicBoolean(false);
        MockLog.awaitLogger(
            () -> reindexer.closeRestClientAndRun(restClient, () -> completion.set(true)),
            Reindexer.class,
            new MockLog.SeenEventExpectation(
                "Failed to close RestClient after version lookup should be logged",
                Reindexer.class.getCanonicalName(),
                Level.WARN,
                "Failed to close RestClient after version lookup"
            )
        );
        assertTrue(completion.get());
    }

    /**
     * Client that succeeds on OpenPointInTime and Search (empty results) but fails on ClosePointInTime.
     * Used to verify that PIT close failures are logged but don't propagate to the main listener.
     */
    private static final class ClosePitFailingClient extends NoOpClient {
        private final String closeFailureMessage;
        private final TestThreadPool threadPool;

        ClosePitFailingClient(String threadPoolName, String closeFailureMessage) {
            super(new TestThreadPool(threadPoolName), TestProjectResolvers.DEFAULT_PROJECT_ONLY);
            this.threadPool = (TestThreadPool) super.threadPool();
            this.closeFailureMessage = closeFailureMessage;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (action == TransportOpenPointInTimeAction.TYPE && request instanceof OpenPointInTimeRequest) {
                OpenPointInTimeResponse response = new OpenPointInTimeResponse(new BytesArray("pit-id"), 1, 1, 0, 0);
                listener.onResponse((Response) response);
                return;
            }
            if (action == TransportSearchAction.TYPE && request instanceof SearchRequest) {
                SearchResponse response = SearchResponseUtils.successfulResponse(
                    SearchHits.empty(new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0)
                );
                listener.onResponse((Response) response);
                response.decRef();
                return;
            }
            if (action == TransportClosePointInTimeAction.TYPE && request instanceof ClosePointInTimeRequest) {
                listener.onFailure(new RuntimeException(closeFailureMessage));
                return;
            }
            super.doExecute(action, request, listener);
        }

        void shutdown() {
            terminate(threadPool);
        }
    }

    /**
     * Client that fails when it receives an OpenPointInTimeRequest. Used to verify the local PIT path is taken.
     */
    private static final class OpenPitFailingClient extends NoOpClient {
        private final String failureMessage;
        private final TestThreadPool threadPool;

        OpenPitFailingClient(String threadPoolName, String failureMessage) {
            super(new TestThreadPool(threadPoolName), TestProjectResolvers.DEFAULT_PROJECT_ONLY);
            this.threadPool = (TestThreadPool) super.threadPool();
            this.failureMessage = failureMessage;
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (action == TransportOpenPointInTimeAction.TYPE && request instanceof OpenPointInTimeRequest) {
                listener.onFailure(new RuntimeException(failureMessage));
            } else {
                super.doExecute(action, request, listener);
            }
        }

        void shutdown() {
            terminate(threadPool);
        }
    }

    /**
     * Client that captures the OpenPointInTimeRequest when received and returns success.
     * Counts ClosePointInTimeRequest invocations. Used to verify PIT close behavior.
     */
    private static class OpenPitCapturingClient extends NoOpClient {
        private final AtomicInteger closeCount = new AtomicInteger(0);
        private final AtomicInteger openPitCount = new AtomicInteger(0);

        int getCloseCount() {
            return closeCount.get();
        }

        int getOpenPitCount() {
            return openPitCount.get();
        }

        private final TestThreadPool threadPool;
        private volatile OpenPointInTimeRequest capturedPitRequest;

        OpenPitCapturingClient(String threadPoolName) {
            super(new TestThreadPool(threadPoolName), TestProjectResolvers.DEFAULT_PROJECT_ONLY);
            this.threadPool = (TestThreadPool) super.threadPool();
        }

        OpenPointInTimeRequest getCapturedPitRequest() {
            return capturedPitRequest;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (action == TransportOpenPointInTimeAction.TYPE && request instanceof OpenPointInTimeRequest pitRequest) {
                openPitCount.incrementAndGet();
                capturedPitRequest = pitRequest;
                OpenPointInTimeResponse response = new OpenPointInTimeResponse(new BytesArray("pit-id"), 1, 1, 0, 0);
                listener.onResponse((Response) response);
                return;
            }
            if (action == TransportSearchAction.TYPE && request instanceof SearchRequest) {
                SearchResponse response = SearchResponseUtils.successfulResponse(
                    SearchHits.empty(new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0)
                );
                listener.onResponse((Response) response);
                response.decRef();
                return;
            }
            if (action == TransportClosePointInTimeAction.TYPE && request instanceof ClosePointInTimeRequest) {
                closeCount.incrementAndGet();
                listener.onResponse((Response) new ClosePointInTimeResponse(true, 1));
                return;
            }
            super.doExecute(action, request, listener);
        }

        void shutdown() {
            terminate(threadPool);
        }
    }

    /**
     * Runs slice child {@link ReindexAction} requests by delegating to the same {@link Reindexer} (see {@code reindexerRef}).
     */
    private static final class SlicedLocalPitReindexClient extends OpenPitCapturingClient {
        private final ClusterService clusterService;
        private final AtomicReference<Reindexer> reindexerRef;
        private final AtomicLong nextChildTaskId;

        SlicedLocalPitReindexClient(
            String threadPoolName,
            ClusterService clusterService,
            AtomicReference<Reindexer> reindexerRef,
            long firstChildTaskId
        ) {
            super(threadPoolName);
            this.clusterService = clusterService;
            this.reindexerRef = reindexerRef;
            this.nextChildTaskId = new AtomicLong(firstChildTaskId);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (action == ReindexAction.INSTANCE && request instanceof ReindexRequest rr && rr.getParentTask().isSet()) {
                Reindexer reindexer = reindexerRef.get();
                assertNotNull("Reindexer must be set before slice execution", reindexer);
                long childId = nextChildTaskId.getAndIncrement();
                TaskId workerTaskId = new TaskId(clusterService.localNode().getId(), childId);
                BulkByScrollTask workerTask = new BulkByScrollTask(
                    workerTaskId,
                    "reindex",
                    ReindexAction.NAME,
                    rr.getDescription(),
                    rr.getParentTask(),
                    Collections.emptyMap(),
                    false,
                    new ResumeInfo.RelocationOrigin(workerTaskId, System.currentTimeMillis())
                );
                ActionListener<BulkByScrollResponse> bulkListener = (ActionListener<BulkByScrollResponse>) listener;
                reindexer.initTask(
                    workerTask,
                    rr,
                    ActionListener.wrap(v -> reindexer.execute(workerTask, rr, this, bulkListener), bulkListener::onFailure)
                );
                return;
            }
            super.doExecute(action, request, listener);
        }
    }

    // --- helpers ---

    private static final String REMOTE_PIT_TEST_VERSION_JSON = "{\"version\":{\"number\":\"7.10.0\"},\"tagline\":\"You Know, for Search\"}";
    private static final String REMOTE_PIT_OPEN_RESPONSE = "{\"id\":\"c29tZXBpdGlk\"}";
    private static final String REMOTE_PIT_EMPTY_SEARCH_RESPONSE = "{"
        + "\"_scroll_id\":\"scroll1\","
        + "\"timed_out\":false,"
        + "\"hits\":{"
        + "\"total\":0,"
        + "\"hits\":[]"
        + "},"
        + "\"_shards\":{"
        + "\"total\":1,"
        + "\"successful\":1,"
        + "\"failed\":0"
        + "}"
        + "}";

    /**
     * Creates a MockHttpServer that handles the full remote PIT flow (version, open PIT, search, close PIT).
     * For requests matching the predicate, the customHandler is used; otherwise standard success responses are returned.
     */
    @SuppressForbidden(reason = "use http server for testing")
    private HttpServer createRemotePitMockServer(BiPredicate<String, String> useCustomHandler, Consumer<HttpExchange> customHandler)
        throws IOException {
        HttpServer server = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String method = exchange.getRequestMethod();
            if (useCustomHandler.test(path, method)) {
                customHandler.accept(exchange);
            } else if (path.equals("/") || path.isEmpty()) {
                respondJson(exchange, 200, REMOTE_PIT_TEST_VERSION_JSON);
            } else if (path.contains("_pit") && "POST".equals(method)) {
                respondJson(exchange, 200, REMOTE_PIT_OPEN_RESPONSE);
            } else if (path.contains("_search") && "POST".equals(method)) {
                respondJson(exchange, 200, REMOTE_PIT_EMPTY_SEARCH_RESPONSE);
            } else if (path.contains("_search/scroll") && "DELETE".equals(method)) {
                exchange.sendResponseHeaders(200, -1);
            } else {
                exchange.sendResponseHeaders(404, -1);
            }
            exchange.close();
        });
        return server;
    }

    private static void respondJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    /**
     * Runs a remote PIT reindex test against a MockHttpServer. The server must already be started.
     */
    @SuppressForbidden(reason = "use http server for testing")
    private void runRemotePitTestWithMockServer(
        HttpServer server,
        Consumer<ReindexRequest> requestConfigurer,
        Consumer<PlainActionFuture<BulkByScrollResponse>> assertions
    ) {
        BytesArray matchAll = new BytesArray("{\"match_all\":{}}");
        RemoteInfo remoteInfo = new RemoteInfo(
            "http",
            server.getAddress().getHostString(),
            server.getAddress().getPort(),
            null,
            matchAll,
            null,
            null,
            emptyMap(),
            TimeValue.timeValueSeconds(5),
            TimeValue.timeValueSeconds(5)
        );

        ReindexRequest request = new ReindexRequest();
        request.setSourceIndices("source");
        request.setDestIndex("dest");
        request.setRemoteInfo(remoteInfo);
        request.setSlices(1);
        requestConfigurer.accept(request);

        ClusterService clusterService = mock(ClusterService.class);
        DiscoveryNode localNode = DiscoveryNodeUtils.builder("local-node").build();
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        when(clusterService.localNode()).thenReturn(localNode);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);

        ProjectResolver projectResolver = mock(ProjectResolver.class);
        when(projectResolver.getProjectState(any())).thenReturn(ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID));

        TestThreadPool threadPool = new TestThreadPool(getTestName()) {
            @Override
            public ExecutorService executor(String name) {
                return DIRECT_EXECUTOR_SERVICE;
            }
        };
        try {
            Environment environment = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());
            ReindexSslConfig sslConfig = new ReindexSslConfig(environment.settings(), environment, mock(ResourceWatcherService.class));

            FeatureService featureService = mock(FeatureService.class);
            when(featureService.clusterHasFeature(any(), eq(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE))).thenReturn(true);

            Reindexer reindexer = new Reindexer(
                clusterService,
                projectResolver,
                mock(Client.class),
                threadPool,
                mock(ScriptService.class),
                sslConfig,
                null,
                mock(TransportService.class),
                mock(ReindexRelocationNodePicker.class),
                featureService,
                mock(TaskResultsService.class)
            );

            BulkByScrollTask task = new BulkByScrollTask(
                randomTaskId(),
                "reindex",
                "reindex",
                "test",
                TaskId.EMPTY_TASK_ID,
                Collections.emptyMap(),
                false,
                randomOrigin()
            );

            PlainActionFuture<BulkByScrollResponse> initFuture = new PlainActionFuture<>();
            reindexer.initTask(
                task,
                request,
                initFuture.delegateFailure((l, v) -> reindexer.execute(task, request, mock(Client.class), l))
            );
            assertions.accept(initFuture);
        } finally {
            terminate(threadPool);
        }
    }

    private BulkByScrollResponse reindexResponseWithBulkAndSearchFailures(
        final List<BulkItemResponse.Failure> bulkFailures,
        List<PaginatedSearchFailure> searchFailures
    ) {
        return new BulkByScrollResponse(
            TimeValue.ZERO,
            new BulkByScrollTask.Status(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, timeValueMillis(0), 0f, null, timeValueMillis(0)),
            bulkFailures,
            searchFailures,
            false
        );
    }

    private BulkByScrollResponse reindexResponseWithPitId(BytesReference pitId) {
        return new BulkByScrollResponse(
            TimeValue.ZERO,
            new BulkByScrollTask.Status(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, timeValueMillis(0), 0f, null, timeValueMillis(0)),
            List.of(),
            List.of(),
            false,
            null,
            pitId
        );
    }

    private BulkByScrollResponse reindexResponseWithResumeInfo() {
        return reindexResponseWithResumeInfo(randomOrigin());
    }

    private BulkByScrollResponse reindexResponseWithResumeInfo(ResumeInfo.RelocationOrigin origin) {
        final var workerResumeInfo = new ResumeInfo.ScrollWorkerResumeInfo(
            "test-scroll-id",
            System.nanoTime(),
            new BulkByScrollTask.Status(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, timeValueMillis(0), 0f, null, timeValueMillis(0)),
            null
        );
        return new BulkByScrollResponse(
            TimeValue.MINUS_ONE,
            new BulkByScrollTask.Status(List.of(), null),
            List.of(),
            List.of(),
            false,
            new ResumeInfo(origin, workerResumeInfo, null)
        );
    }

    private static BulkByScrollTask createNonSlicedWorkerTask() {
        return nonSlicedWorkerTaskWithOrigin(randomOrigin());
    }

    private static BulkByScrollTask nonSlicedWorkerTaskWithOrigin(ResumeInfo.RelocationOrigin origin) {
        BulkByScrollTask task = new BulkByScrollTask(
            randomTaskId(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            randomBoolean(),
            origin
        );
        task.setWorker(Float.POSITIVE_INFINITY, null);
        return task;
    }

    private static BulkByScrollTask createSliceWorkerTask() {
        BulkByScrollTask task = new BulkByScrollTask(
            randomTaskId(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            new TaskId("node", 1),
            Map.of(),
            randomBoolean(),
            randomOrigin()
        );
        task.setWorker(randomFloat(), 0);
        return task;
    }

    private static BulkByScrollTask createLeaderTask() {
        BulkByScrollTask task = new BulkByScrollTask(
            randomTaskId(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            randomBoolean(),
            randomOrigin()
        );
        task.setWorkerCount(randomIntBetween(2, 10));
        return task;
    }

    private static BulkByScrollTask createTaskWithParentIdAndRelocationEnabled(final TaskId parentTaskId) {
        return new BulkByScrollTask(
            new TaskId(randomAlphaOfLength(10), 987),
            "test_type",
            "test_action",
            "test",
            parentTaskId,
            Collections.emptyMap(),
            true,
            randomOrigin()
        );
    }

    private static Reindexer reindexerWithRelocation() {
        final TaskManager taskManager = mock(TaskManager.class);
        final TransportService transportService = mock(TransportService.class);
        when(transportService.getTaskManager()).thenReturn(taskManager);
        return reindexerWithRelocation(mock(ClusterService.class), transportService);
    }

    private static Reindexer reindexerWithRelocation(ClusterService clusterService, TransportService transportService) {
        return reindexerWithRelocation(clusterService, transportService, mock(TaskResultsService.class));
    }

    private static Reindexer reindexerWithRelocation(
        ClusterService clusterService,
        TransportService transportService,
        TaskResultsService taskResultsService
    ) {
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.generic()).thenReturn(mock(ExecutorService.class));
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        return new Reindexer(
            clusterService,
            mock(ProjectResolver.class),
            mock(Client.class),
            threadPool,
            mock(ScriptService.class),
            mock(ReindexSslConfig.class),
            null,
            transportService,
            mock(ReindexRelocationNodePicker.class),
            // Will default REINDEX_PIT_SEARCH_FEATURE to false
            mock(FeatureService.class),
            taskResultsService
        );
    }

    private static ReindexRequest reindexRequest() {
        return new ReindexRequest();
    }

    private static ResumeInfo.RelocationOrigin randomOrigin() {
        return new ResumeInfo.RelocationOrigin(randomRealTaskId(), randomNonNegativeLong());
    }

    private static TaskId randomTaskId() {
        return randomBoolean() ? TaskId.EMPTY_TASK_ID : randomRealTaskId();
    }

    private static TaskId randomRealTaskId() {
        return new TaskId(randomAlphaOfLength(10), randomNonNegativeLong());
    }

    private static ClusterService clusterServiceMock() {
        final ClusterService clusterService = mock(ClusterService.class);
        final DiscoveryNode localNode = DiscoveryNodeUtils.builder("local-node").build();
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        when(clusterService.localNode()).thenReturn(localNode);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        return clusterService;
    }
}
