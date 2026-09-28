/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.exchange.ExchangeResponse;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.tracing.TracingContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlSearchShardsAction;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchBoundaryExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataNodeComputeHandlerTests extends ESTestCase {

    /**
     * Real transport and exchange callbacks exercise sequential dispatch; mocked compute and cluster services avoid shard execution.
     * The compute service assigns distinct non-recording group spans so parent selection can be checked without an SDK exporter.
     */
    public void testSequentialComputeGroupsRestoreQueryTraceContext() throws Exception {
        var threadPool = new TestThreadPool(getTestName());
        var transport = new CapturingTransport();
        var localNode = DiscoveryNodeUtils.create("coordinator");
        var dataNodes = List.of(
            DiscoveryNodeUtils.create("data-1"),
            DiscoveryNodeUtils.create("data-2"),
            DiscoveryNodeUtils.create("data-3")
        );
        var blockFactory = TestBlockFactory.getNonBreakingInstance();
        try (
            var transportService = transport.createTransportService(
                Settings.EMPTY,
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                ignored -> localNode,
                null,
                Set.of(Task.TRACE_ID)
            );
            var exchangeService = new ExchangeService(Settings.EMPTY, threadPool, ThreadPool.Names.SEARCH, blockFactory)
        ) {
            var executor = threadPool.executor(ThreadPool.Names.SEARCH);
            var groups = new ArrayList<SearchShardsGroup>();
            for (int shard = 0; shard < dataNodes.size(); shard++) {
                groups.add(
                    new SearchShardsGroup(
                        new ShardId("test", "index-uuid", shard),
                        List.of(dataNodes.get(shard).getId()),
                        false,
                        SplitShardCountSummary.UNSET
                    )
                );
            }
            transportService.registerRequestHandler(
                EsqlSearchShardsAction.TYPE.name(),
                executor,
                SearchShardsRequest::new,
                (request, channel, task) -> channel.sendResponse(
                    new SearchShardsResponse(groups, 0, dataNodes, Map.of("index-uuid", AliasFilter.EMPTY))
                )
            );
            var clusterService = mock(ClusterService.class);
            when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
            when(clusterService.state()).thenReturn(ClusterState.builder(ClusterName.DEFAULT).build());
            var computeService = mock(ComputeService.class);
            var sessions = new AtomicInteger();
            when(computeService.newChildSession(anyString())).thenAnswer(ignored -> "session-" + sessions.incrementAndGet());
            when(computeService.cancelQueryOnFailure(any())).thenReturn(() -> fail("unexpected group failure"));
            var handler = new DataNodeComputeHandler(
                computeService,
                clusterService,
                mock(ProjectResolver.class),
                mock(SearchService.class),
                transportService,
                exchangeService,
                executor
            );
            transportService.start();
            transportService.acceptIncomingRequests();
            var configuration = new Configuration(
                Instant.now(),
                Locale.ROOT,
                null,
                "test",
                new QueryPragmas(
                    Settings.builder()
                        .put(QueryPragmas.MAX_CONCURRENT_NODES_PER_CLUSTER.getKey(), 1)
                        .put(ExchangeSourceHandler.CONCURRENT_CLIENTS_SETTING.getKey(), 1)
                        .build()
                ),
                10000,
                1000,
                "FROM test",
                false,
                Map.of(),
                System.nanoTime(),
                true,
                10000,
                1000,
                ResolvedSettings.EMPTY,
                Map.of()
            );
            var querySpan = SpanContext.create(
                "0af7651916cd43dd8448eb211c80319d",
                "b7ad6b7169203332",
                TraceFlags.getSampled(),
                TraceState.getDefault()
            );
            var groupParents = new ArrayList<SpanContext>();
            var threadContext = threadPool.getThreadContext();
            try (var scope = TracingContext.activate(threadContext, Context.root().with(Span.wrap(querySpan)))) {
                var taskManager = transportService.getTaskManager();
                var parentTask = (CancellableTask) taskManager.register("transport", "query", new EsqlQueryRequest(), false);
                when(computeService.createGroupTask(eq(parentTask), any())).thenAnswer(ignored -> {
                    groupParents.add(Span.current().getSpanContext());
                    var groupTask = (CancellableTask) taskManager.register(
                        "transport",
                        "esql_compute_group",
                        new EsqlQueryRequest(),
                        false
                    );
                    groupTask.startTrace(
                        Context.current(),
                        Span.wrap(
                            SpanContext.create(
                                querySpan.getTraceId(),
                                String.format(Locale.ROOT, "%016x", groupTask.getId()),
                                TraceFlags.getSampled(),
                                TraceState.getDefault()
                            )
                        )
                    );
                    return groupTask;
                });
                try {
                    var completion = new PlainActionFuture<ComputeResponse>();
                    var exchangeSource = new ExchangeSourceHandler(10, executor);
                    executor.execute(
                        () -> handler.startComputeOnDataNodes(
                            "session",
                            "",
                            parentTask,
                            new EsqlFlags(false),
                            configuration,
                            new ExchangeSourceExec(Source.EMPTY, List.of(), false),
                            Set.of("test"),
                            new OriginalIndices(new String[] { "test" }, IndicesOptions.STRICT_EXPAND_OPEN),
                            exchangeSource,
                            false,
                            null,
                            () -> fail("unexpected query failure"),
                            completion
                        )
                    );
                    for (int group = 0; group < dataNodes.size(); group++) {
                        assertBusy(() -> assertEquals(1, transport.capturedRequests().length));
                        var openRequest = transport.getCapturedRequestsAndClear()[0];
                        assertEquals(ExchangeService.OPEN_EXCHANGE_ACTION_NAME, openRequest.action());
                        executor.submit(() -> transport.handleResponse(openRequest.requestId(), ActionResponse.Empty.INSTANCE))
                            .get(10, TimeUnit.SECONDS);
                        assertBusy(() -> assertEquals(2, transport.capturedRequests().length));
                        var requests = transport.getCapturedRequestsAndClear();
                        for (var request : requests) {
                            executor.submit(() -> {
                                switch (request.action()) {
                                    case ComputeService.DATA_ACTION_NAME -> transport.handleResponse(
                                        request.requestId(),
                                        new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())
                                    );
                                    case ExchangeService.EXCHANGE_ACTION_NAME -> {
                                        try (var response = new ExchangeResponse(blockFactory, null, true)) {
                                            transport.handleResponse(request.requestId(), response);
                                        }
                                    }
                                    default -> throw new AssertionError("unexpected action " + request.action());
                                }
                            }).get(10, TimeUnit.SECONDS);
                        }
                    }
                    var response = completion.actionGet(10, TimeUnit.SECONDS);
                    assertEquals(dataNodes.size(), response.successfulShards);
                    assertEquals(0, response.failedShards);
                    assertEquals(List.of(querySpan, querySpan, querySpan), groupParents);
                    assertTrue(exchangeSource.isFinished());
                } finally {
                    taskManager.unregister(parentTask);
                }
                assertTrue(taskManager.getTasks().isEmpty());
            }
        } finally {
            terminate(threadPool);
        }
    }

    public void testMalformedRemoteFetchBoundaryFinishesOpenSinkBeforeComputeStarts() {
        // This path fails before compute starts, so transport/search collaborators are inert mocks; a real ExchangeService verifies
        // that the already-open sink is actually removed rather than merely checking a mocked invocation.
        ComputeService computeService = mock(ComputeService.class);
        PlannerSettings.Holder plannerSettings = mock(PlannerSettings.Holder.class);
        when(computeService.plannerSettings()).thenReturn(plannerSettings);
        when(plannerSettings.get()).thenReturn(PlannerSettings.DEFAULTS);
        when(computeService.createFlags()).thenReturn(new EsqlFlags(false));

        ClusterService clusterService = mock(ClusterService.class);
        DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.localNode()).thenReturn(localNode);
        when(localNode.getId()).thenReturn("node-a");

        TransportService transportService = mock(TransportService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadPool.relativeTimeInMillisSupplier()).thenReturn(System::currentTimeMillis);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        ExchangeService exchangeService = new ExchangeService(
            Settings.EMPTY,
            threadPool,
            ThreadPool.Names.SEARCH,
            TestBlockFactory.getNonBreakingInstance()
        );
        Executor directExecutor = Runnable::run;
        DataNodeComputeHandler handler = new DataNodeComputeHandler(
            computeService,
            clusterService,
            mock(ProjectResolver.class),
            mock(SearchService.class),
            transportService,
            exchangeService,
            directExecutor
        );

        DataNodeRequest request = malformedRemoteFetchRequest("session-a");
        exchangeService.createSinkHandler(request.sessionId(), 1);
        TransportChannel channel = mock(TransportChannel.class);
        when(channel.getVersion()).thenReturn(TransportVersion.current());

        handler.messageReceived(request, channel, mock(Task.class));

        expectThrows(ResourceNotFoundException.class, () -> exchangeService.getSinkHandler(request.sessionId()));
    }

    private static DataNodeRequest malformedRemoteFetchRequest(String sessionId) {
        Attribute doc = new MetadataAttribute(Source.EMPTY, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, false);
        Attribute handle = new ReferenceAttribute(
            Source.EMPTY,
            null,
            RemoteFetchHandle.ATTRIBUTE_NAME,
            DataType.KEYWORD,
            Nullability.FALSE,
            null,
            true
        );
        RemoteFetchBoundaryExec boundary = new RemoteFetchBoundaryExec(
            Source.EMPTY,
            new ExchangeSourceExec(Source.EMPTY, List.of(doc), false),
            doc,
            handle,
            List.of()
        );
        ExchangeSinkExec sink = new ExchangeSinkExec(Source.EMPTY, boundary.handoffOutput(), false, boundary);
        return new DataNodeRequest(
            sessionId,
            EsqlTestUtils.TEST_CFG,
            "",
            List.of(),
            Map.of(),
            sink,
            new String[0],
            IndicesOptions.STRICT_EXPAND_OPEN,
            true,
            true,
            true,
            randomBoolean()
        );
    }
}
