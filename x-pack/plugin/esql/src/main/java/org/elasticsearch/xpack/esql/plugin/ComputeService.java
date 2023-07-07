/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShardsAction;
import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.exchange.ExchangeResponse;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupService;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_THREAD_POOL_NAME;

/**
 * Computes the result of a {@link PhysicalPlan}.
 */
public class ComputeService {
    private static final Logger LOGGER = LogManager.getLogger(ComputeService.class);
    private final SearchService searchService;
    private final BigArrays bigArrays;
    private final TransportService transportService;
    private final DriverTaskRunner driverRunner;
    private final ExchangeService exchangeService;
    private final EnrichLookupService enrichLookupService;

    public ComputeService(
        SearchService searchService,
        TransportService transportService,
        ExchangeService exchangeService,
        EnrichLookupService enrichLookupService,
        ThreadPool threadPool,
        BigArrays bigArrays
    ) {
        this.searchService = searchService;
        this.transportService = transportService;
        this.bigArrays = bigArrays.withCircuitBreaking();
        transportService.registerRequestHandler(
            DATA_ACTION_NAME,
            ESQL_THREAD_POOL_NAME,
            DataNodeRequest::new,
            new DataNodeRequestHandler()
        );
        this.driverRunner = new DriverTaskRunner(transportService, threadPool.executor(ESQL_THREAD_POOL_NAME));
        this.exchangeService = exchangeService;
        this.enrichLookupService = enrichLookupService;
    }

    public void execute(
        String sessionId,
        CancellableTask rootTask,
        PhysicalPlan physicalPlan,
        EsqlConfiguration configuration,
        ActionListener<List<Page>> listener
    ) {
        Tuple<PhysicalPlan, PhysicalPlan> coordinatorAndDataNodePlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(physicalPlan);
        final List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());
        PhysicalPlan coordinatorPlan = new OutputExec(coordinatorAndDataNodePlan.v1(), collectedPages::add);
        PhysicalPlan dataNodePlan = coordinatorAndDataNodePlan.v2();

        var concreteIndices = PlannerUtils.planConcreteIndices(physicalPlan);

        QueryPragmas queryPragmas = configuration.pragmas();

        if (concreteIndices.isEmpty()) {
            var computeContext = new ComputeContext(sessionId, List.of(), configuration, null, null);
            runCompute(rootTask, computeContext, coordinatorPlan, listener.map(unused -> collectedPages));
            return;
        }
        QueryBuilder requestFilter = PlannerUtils.requestFilter(dataNodePlan);
        String[] originalIndices = PlannerUtils.planOriginalIndices(physicalPlan);
        computeTargetNodes(rootTask, requestFilter, concreteIndices, originalIndices, ActionListener.wrap(targetNodes -> {
            final ExchangeSourceHandler exchangeSource = exchangeService.createSourceHandler(
                sessionId,
                queryPragmas.exchangeBufferSize(),
                ESQL_THREAD_POOL_NAME
            );
            try (
                Releasable ignored = exchangeSource::decRef;
                RefCountingListener requestRefs = new RefCountingListener(listener.map(unused -> collectedPages))
            ) {
                final AtomicBoolean cancelled = new AtomicBoolean();
                // wait until the source handler is completed
                exchangeSource.addCompletionListener(requestRefs.acquire());
                // run compute on the coordinator
                var computeContext = new ComputeContext(sessionId, List.of(), configuration, exchangeSource, null);
                runCompute(rootTask, computeContext, coordinatorPlan, cancelOnFailure(rootTask, cancelled, requestRefs.acquire()));
                // run compute on remote nodes
                // TODO: This is wrong, we need to be able to cancel
                runComputeOnRemoteNodes(
                    sessionId,
                    rootTask,
                    configuration,
                    dataNodePlan,
                    exchangeSource,
                    targetNodes,
                    () -> cancelOnFailure(rootTask, cancelled, requestRefs.acquire()).map(unused -> null)
                );
            }
        }, listener::onFailure));
    }

    private void runComputeOnRemoteNodes(
        String sessionId,
        CancellableTask rootTask,
        EsqlConfiguration configuration,
        PhysicalPlan dataNodePlan,
        ExchangeSourceHandler exchangeSource,
        List<TargetNode> targetNodes,
        Supplier<ActionListener<DataNodeResponse>> listener
    ) {
        // Do not complete the exchange sources until we have linked all remote sinks
        final ListenableActionFuture<Void> blockingSinkFuture = new ListenableActionFuture<>();
        exchangeSource.addRemoteSink(
            (sourceFinished, l) -> blockingSinkFuture.addListener(l.map(ignored -> new ExchangeResponse(null, true))),
            1
        );
        try (RefCountingRunnable exchangeRefs = new RefCountingRunnable(() -> blockingSinkFuture.onResponse(null))) {
            // For each target node, first open a remote exchange on the remote node, then link the exchange source to
            // the new remote exchange sink, and initialize the computation on the target node via data-node-request.
            for (TargetNode targetNode : targetNodes) {
                var targetNodeListener = ActionListener.releaseAfter(listener.get(), exchangeRefs.acquire());
                var queryPragmas = configuration.pragmas();
                ExchangeService.openExchange(
                    transportService,
                    targetNode.node(),
                    sessionId,
                    queryPragmas.exchangeBufferSize(),
                    ActionListener.wrap(unused -> {
                        var remoteSink = exchangeService.newRemoteSink(rootTask, sessionId, transportService, targetNode.node);
                        exchangeSource.addRemoteSink(remoteSink, queryPragmas.concurrentExchangeClients());
                        transportService.sendChildRequest(
                            targetNode.node,
                            DATA_ACTION_NAME,
                            new DataNodeRequest(sessionId, configuration, targetNode.shardIds, targetNode.aliasFilters, dataNodePlan),
                            rootTask,
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(targetNodeListener, DataNodeResponse::new)
                        );
                    }, targetNodeListener::onFailure)
                );
            }
        }
    }

    private ActionListener<Void> cancelOnFailure(CancellableTask task, AtomicBoolean cancelled, ActionListener<Void> listener) {
        return listener.delegateResponse((l, e) -> {
            l.onFailure(e);
            if (cancelled.compareAndSet(false, true)) {
                LOGGER.debug("cancelling ESQL task {} on failure", task);
                transportService.getTaskManager().cancelTaskAndDescendants(task, "cancelled", false, ActionListener.noop());
            }
        });
    }

    void runCompute(CancellableTask task, ComputeContext context, PhysicalPlan plan, ActionListener<Void> listener) {
        listener = ActionListener.runAfter(listener, () -> Releasables.close(context.searchContexts));
        final List<Driver> drivers;
        try {
            LocalExecutionPlanner planner = new LocalExecutionPlanner(
                context.sessionId,
                task,
                bigArrays,
                context.configuration,
                context.exchangeSource(),
                context.exchangeSink(),
                enrichLookupService,
                new EsPhysicalOperationProviders(context.searchContexts)
            );

            LOGGER.info("Received physical plan:\n{}", plan);
            plan = PlannerUtils.localPlan(context.searchContexts, context.configuration, plan);
            LocalExecutionPlanner.LocalExecutionPlan localExecutionPlan = planner.plan(plan);

            LOGGER.info("Local execution plan:\n{}", localExecutionPlan.describe());
            drivers = localExecutionPlan.createDrivers(context.sessionId);
            if (drivers.isEmpty()) {
                throw new IllegalStateException("no drivers created");
            }
            LOGGER.info("using {} drivers", drivers.size());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        driverRunner.executeDrivers(task, drivers, ActionListener.releaseAfter(listener, () -> Releasables.close(drivers)));
    }

    private void acquireSearchContexts(
        List<ShardId> shardIds,
        Map<Index, AliasFilter> aliasFilters,
        ActionListener<List<SearchContext>> listener
    ) {
        try {
            List<IndexShard> targetShards = new ArrayList<>();
            for (ShardId shardId : shardIds) {
                var indexShard = searchService.getIndicesService().indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
                targetShards.add(indexShard);
            }
            if (targetShards.isEmpty()) {
                listener.onResponse(List.of());
                return;
            }
            CountDown countDown = new CountDown(targetShards.size());
            for (IndexShard targetShard : targetShards) {
                targetShard.ensureShardSearchActive(ignored -> {
                    if (countDown.countDown()) {
                        ActionListener.completeWith(listener, () -> {
                            final List<SearchContext> searchContexts = new ArrayList<>(targetShards.size());
                            boolean success = false;
                            try {
                                for (IndexShard shard : targetShards) {
                                    var aliasFilter = aliasFilters.getOrDefault(shard.shardId().getIndex(), AliasFilter.EMPTY);
                                    ShardSearchRequest shardSearchLocalRequest = new ShardSearchRequest(shard.shardId(), 0, aliasFilter);
                                    SearchContext context = searchService.createSearchContext(
                                        shardSearchLocalRequest,
                                        SearchService.NO_TIMEOUT
                                    );
                                    searchContexts.add(context);
                                }
                                for (SearchContext searchContext : searchContexts) {
                                    searchContext.preProcess();
                                }
                                success = true;
                                return searchContexts;
                            } finally {
                                if (success == false) {
                                    IOUtils.close(searchContexts);
                                }
                            }
                        });
                    }
                });
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    record TargetNode(DiscoveryNode node, List<ShardId> shardIds, Map<Index, AliasFilter> aliasFilters) {

    }

    private void computeTargetNodes(
        Task parentTask,
        QueryBuilder filter,
        Set<String> concreteIndices,
        String[] originalIndices,
        ActionListener<List<TargetNode>> listener
    ) {
        // Ideally, the search_shards API should be called before the field-caps API; however, this can lead
        // to a situation where the column structure (i.e., matched data types) differs depending on the query.
        ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
        ActionListener<SearchShardsResponse> preservingContextListener = ContextPreservingActionListener.wrapPreservingContext(
            listener.map(resp -> {
                Map<String, DiscoveryNode> nodes = new HashMap<>();
                for (DiscoveryNode node : resp.getNodes()) {
                    nodes.put(node.getId(), node);
                }
                Map<String, List<ShardId>> nodeToShards = new HashMap<>();
                Map<String, Map<Index, AliasFilter>> nodeToAliasFilters = new HashMap<>();
                for (SearchShardsGroup group : resp.getGroups()) {
                    var shardId = group.shardId();
                    if (group.skipped()) {
                        continue;
                    }
                    if (group.allocatedNodes().isEmpty()) {
                        throw new ShardNotFoundException(group.shardId(), "no shard copies found {}", group.shardId());
                    }
                    if (concreteIndices.contains(shardId.getIndexName()) == false) {
                        continue;
                    }
                    String targetNode = group.allocatedNodes().get(0);
                    nodeToShards.computeIfAbsent(targetNode, k -> new ArrayList<>()).add(shardId);
                    AliasFilter aliasFilter = resp.getAliasFilters().get(shardId.getIndex().getUUID());
                    if (aliasFilter != null) {
                        nodeToAliasFilters.computeIfAbsent(targetNode, k -> new HashMap<>()).put(shardId.getIndex(), aliasFilter);
                    }
                }
                List<TargetNode> targetNodes = new ArrayList<>(nodeToShards.size());
                for (Map.Entry<String, List<ShardId>> e : nodeToShards.entrySet()) {
                    DiscoveryNode node = nodes.get(e.getKey());
                    Map<Index, AliasFilter> aliasFilters = nodeToAliasFilters.getOrDefault(e.getKey(), Map.of());
                    targetNodes.add(new TargetNode(node, e.getValue(), aliasFilters));
                }
                return targetNodes;
            }),
            threadContext
        );
        try (ThreadContext.StoredContext ignored = threadContext.newStoredContextPreservingResponseHeaders()) {
            threadContext.markAsSystemContext();
            SearchShardsRequest searchShardsRequest = new SearchShardsRequest(
                originalIndices,
                SearchRequest.DEFAULT_INDICES_OPTIONS,
                filter,
                null,
                null,
                false,
                null
            );
            transportService.sendChildRequest(
                transportService.getLocalNode(),
                SearchShardsAction.NAME,
                searchShardsRequest,
                parentTask,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(preservingContextListener, SearchShardsResponse::new)
            );
        }
    }

    // TODO: To include stats/profiles
    private static class DataNodeResponse extends TransportResponse {
        DataNodeResponse() {}

        DataNodeResponse(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) {

        }
    }

    // TODO: Use an internal action here
    public static final String DATA_ACTION_NAME = EsqlQueryAction.NAME + "/data";

    private class DataNodeRequestHandler implements TransportRequestHandler<DataNodeRequest> {
        @Override
        public void messageReceived(DataNodeRequest request, TransportChannel channel, Task task) {
            final var parentTask = (CancellableTask) task;
            final var sessionId = request.sessionId();
            final var exchangeSink = exchangeService.getSinkHandler(sessionId);
            parentTask.addListener(() -> exchangeService.finishSinkHandler(sessionId, new TaskCancelledException("task cancelled")));
            final ActionListener<Void> listener = new ChannelActionListener<>(channel).map(nullValue -> new DataNodeResponse());
            acquireSearchContexts(request.shardIds(), request.aliasFilters(), ActionListener.wrap(searchContexts -> {
                var computeContext = new ComputeContext(sessionId, searchContexts, request.configuration(), null, exchangeSink);
                runCompute(parentTask, computeContext, request.plan(), ActionListener.wrap(unused -> {
                    // don't return until all pages are fetched
                    exchangeSink.addCompletionListener(
                        ActionListener.releaseAfter(listener, () -> exchangeService.finishSinkHandler(sessionId, null))
                    );
                }, e -> {
                    exchangeService.finishSinkHandler(sessionId, e);
                    listener.onFailure(e);
                }));
            }, e -> {
                exchangeService.finishSinkHandler(sessionId, e);
                listener.onFailure(e);
            }));
        }
    }

    record ComputeContext(
        String sessionId,
        List<SearchContext> searchContexts,
        EsqlConfiguration configuration,
        ExchangeSourceHandler exchangeSource,
        ExchangeSinkHandler exchangeSink
    ) {}
}
