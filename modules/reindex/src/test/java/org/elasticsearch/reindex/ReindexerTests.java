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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.PaginatedHitSource;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.index.reindex.ResumeBulkByScrollRequest;
import org.elasticsearch.index.reindex.ResumeBulkByScrollResponse;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.index.reindex.ResumeReindexAction;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.ResourceWatcherService;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.util.concurrent.EsExecutors.DIRECT_EXECUTOR_SERVICE;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.elasticsearch.rest.RestStatus.TOO_MANY_REQUESTS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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
        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, task, reindexRequest(), randomNonNegativeLong());

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
        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, task, reindexRequest(), randomNonNegativeLong());

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
        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, task, reindexRequest(), randomNonNegativeLong());

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
        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, task, reindexRequest(), randomNonNegativeLong());

        Exception exception = new Exception("random failure");
        Exception anotherException = new Exception("another failure");
        BulkByScrollResponse response = reindexResponseWithBulkAndSearchFailures(
            null,
            List.of(new PaginatedHitSource.SearchFailure(exception), new PaginatedHitSource.SearchFailure(anotherException))
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

        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, task, reindexRequest(), randomNonNegativeLong());

        assertSame(listener, wrapped);
        verifyNoMoreInteractions(metrics);
    }

    public void testWrapWithMetricsWrapsLeader() {
        ReindexMetrics metrics = mock();
        ActionListener<BulkByScrollResponse> listener = spy(ActionListener.noop());
        BulkByScrollTask task = createLeaderTask();

        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, task, reindexRequest(), randomNonNegativeLong());

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

        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, task, reindexRequest(), randomNonNegativeLong());

        BulkByScrollResponse response = reindexResponseWithResumeInfo();
        wrapped.onResponse(response);

        verify(listener).onResponse(response);
        verifyNoMoreInteractions(metrics);
    }

    // listenerWithRelocations tests

    public void testListenerWithRelocationsPassesThroughForWorkerWithLeaderParent() {
        assumeTrue("reindex resilience enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final long parentTaskId = 99;
        final BulkByScrollTask leaderTask = new BulkByScrollTask(
            parentTaskId,
            "test_type",
            "test_action",
            "test",
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            true
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
        final ActionListener<BulkByScrollResponse> wrapped = reindexer.listenerWithRelocations(task, reindexRequest(), original);

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
        final ActionListener<BulkByScrollResponse> wrapped = reindexer.listenerWithRelocations(task, reindexRequest(), original);

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
        final ActionListener<BulkByScrollResponse> wrapped = reindexer.listenerWithRelocations(task, reindexRequest(), original);

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

        final PlainActionFuture<BulkByScrollResponse> future = new PlainActionFuture<>();
        final ActionListener<BulkByScrollResponse> wrapped = reindexer.listenerWithRelocations(task, reindexRequest(), future);

        final BulkByScrollResponse response = reindexResponseWithResumeInfo();
        wrapped.onResponse(response);

        assertTrue(future.isDone());
        TaskRelocatedException exception = expectThrows(TaskRelocatedException.class, future::actionGet);
        assertThat(exception.getMessage(), equalTo("Task was relocated"));
        assertThat(exception.getMetadataKeys(), equalTo(Set.of("es.original_task_id", "es.relocated_task_id")));
        assertThat(exception.getMetadata("es.original_task_id"), equalTo(List.of("source-node:987")));
        assertThat(exception.getMetadata("es.relocated_task_id"), equalTo(List.of("target-node:123")));
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
                featureService
            );

            final ReindexRequest request = new ReindexRequest();
            request.setSourceIndices("source");
            request.setDestIndex("dest");
            request.setSlices(1);

            final BulkByScrollTask task = new BulkByScrollTask(
                randomLong(),
                "reindex",
                "reindex",
                "test",
                TaskId.EMPTY_TASK_ID,
                Collections.emptyMap(),
                false
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
                final ClusterService clusterService = mock(ClusterService.class);
                final DiscoveryNode localNode = DiscoveryNodeUtils.builder("local-node").build();
                when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
                when(clusterService.localNode()).thenReturn(localNode);

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
                    featureService
                );

                final ReindexRequest request = new ReindexRequest();
                request.setSourceIndices("source");
                request.setDestIndex("dest");
                request.setSlices(1);

                final BulkByScrollTask task = new BulkByScrollTask(
                    randomLong(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false
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
                final ClusterService clusterService = mock(ClusterService.class);
                final DiscoveryNode localNode = DiscoveryNodeUtils.builder("local-node").build();
                when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
                when(clusterService.localNode()).thenReturn(localNode);

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
                    featureService
                );

                final ReindexRequest request = new ReindexRequest();
                request.setSourceIndices("source");
                request.setDestIndex("dest");
                request.setSlices(1);

                final BulkByScrollTask task = new BulkByScrollTask(
                    randomLong(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false
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
                final ClusterService clusterService = mock(ClusterService.class);
                final DiscoveryNode localNode = DiscoveryNodeUtils.builder("local-node").build();
                when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
                when(clusterService.localNode()).thenReturn(localNode);

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
                    featureService
                );

                final ReindexRequest request = new ReindexRequest();
                request.setSourceIndices("source");
                request.setDestIndex("dest");
                request.setSlices(1);
                request.getSearchRequest().routing("r1");

                final BulkByScrollTask task = new BulkByScrollTask(
                    randomLong(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false
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
                final ClusterService clusterService = mock(ClusterService.class);
                final DiscoveryNode localNode = DiscoveryNodeUtils.builder("local-node").build();
                when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
                when(clusterService.localNode()).thenReturn(localNode);

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
                    featureService
                );

                final ReindexRequest request = new ReindexRequest();
                request.setSourceIndices("source");
                request.setDestIndex("dest");
                request.setSlices(1);
                request.getSearchRequest().preference("_local");

                final BulkByScrollTask task = new BulkByScrollTask(
                    randomLong(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false
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
                final ClusterService clusterService = mock(ClusterService.class);
                final DiscoveryNode localNode = DiscoveryNodeUtils.builder("local-node").build();
                when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
                when(clusterService.localNode()).thenReturn(localNode);

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
                    featureService
                );

                final ReindexRequest request = new ReindexRequest();
                request.setSourceIndices("source");
                request.setDestIndex("dest");
                request.setSlices(1);
                request.getSearchRequest().allowPartialSearchResults(true);

                final BulkByScrollTask task = new BulkByScrollTask(
                    randomLong(),
                    "reindex",
                    "reindex",
                    "test",
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    false
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
     * Used to verify the local PIT request has the expected properties.
     */
    private static final class OpenPitCapturingClient extends NoOpClient {
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
                listener.onResponse((Response) new ClosePointInTimeResponse(true, 1));
                return;
            }
            super.doExecute(action, request, listener);
        }

        void shutdown() {
            terminate(threadPool);
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
                featureService
            );

            BulkByScrollTask task = new BulkByScrollTask(
                randomLong(),
                "reindex",
                "reindex",
                "test",
                TaskId.EMPTY_TASK_ID,
                Collections.emptyMap(),
                false
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
        List<PaginatedHitSource.SearchFailure> searchFailures
    ) {
        return new BulkByScrollResponse(
            TimeValue.ZERO,
            new BulkByScrollTask.Status(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, timeValueMillis(0), 0f, null, timeValueMillis(0)),
            bulkFailures,
            searchFailures,
            false
        );
    }

    private BulkByScrollResponse reindexResponseWithResumeInfo() {
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
            new ResumeInfo(workerResumeInfo, null)
        );
    }

    private static BulkByScrollTask createNonSlicedWorkerTask() {
        BulkByScrollTask task = new BulkByScrollTask(
            randomNonNegativeLong(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            randomBoolean()
        );
        task.setWorker(Float.POSITIVE_INFINITY, null);
        return task;
    }

    private static BulkByScrollTask createSliceWorkerTask() {
        BulkByScrollTask task = new BulkByScrollTask(
            randomNonNegativeLong(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            new TaskId("node", 1),
            Map.of(),
            randomBoolean()
        );
        task.setWorker(randomFloat(), 0);
        return task;
    }

    private static BulkByScrollTask createLeaderTask() {
        BulkByScrollTask task = new BulkByScrollTask(
            randomNonNegativeLong(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            randomBoolean()
        );
        task.setWorkerCount(randomIntBetween(2, 10));
        return task;
    }

    private static BulkByScrollTask createTaskWithParentIdAndRelocationEnabled(final TaskId parentTaskId) {
        return new BulkByScrollTask(987, "test_type", "test_action", "test", parentTaskId, Collections.emptyMap(), true);
    }

    private static Reindexer reindexerWithRelocation() {
        final TaskManager taskManager = mock(TaskManager.class);
        final TransportService transportService = mock(TransportService.class);
        when(transportService.getTaskManager()).thenReturn(taskManager);
        return reindexerWithRelocation(mock(ClusterService.class), transportService);
    }

    private static Reindexer reindexerWithRelocation(ClusterService clusterService, TransportService transportService) {
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.generic()).thenReturn(mock(ExecutorService.class));
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
            mock(FeatureService.class)
        );
    }

    private static Reindexer reindexerWithRelocationAndMetrics(final ReindexMetrics metrics) {
        return new Reindexer(
            mock(ClusterService.class),
            mock(ProjectResolver.class),
            mock(Client.class),
            mock(ThreadPool.class),
            mock(ScriptService.class),
            mock(ReindexSslConfig.class),
            metrics,
            mock(TransportService.class),
            mock(ReindexRelocationNodePicker.class),
            // Will default REINDEX_PIT_SEARCH_FEATURE to false
            mock(FeatureService.class)
        );
    }

    private static ReindexRequest reindexRequest() {
        return new ReindexRequest();
    }
}
