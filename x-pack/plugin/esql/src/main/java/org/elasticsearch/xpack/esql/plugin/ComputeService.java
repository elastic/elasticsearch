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
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.compute.operator.exchange.RemoteSink;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
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

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_THREAD_POOL_NAME;

/**
 * Computes the result of a {@link PhysicalPlan}.
 */
public class ComputeService {
    private static final Logger LOGGER = LogManager.getLogger(ComputeService.class);
    private final SearchService searchService;
    private final ThreadPool threadPool;
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
        this.threadPool = threadPool;
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

        var computeContext = new ComputeContext(sessionId, List.of(), configuration);

        if (concreteIndices.isEmpty()) {
            runCompute(rootTask, computeContext, coordinatorPlan, listener.map(unused -> collectedPages));
            return;
        }
        QueryBuilder requestFilter = PlannerUtils.requestFilter(dataNodePlan);
        String[] originalIndices = PlannerUtils.planOriginalIndices(physicalPlan);
        computeTargetNodes(rootTask, requestFilter, concreteIndices, originalIndices, ActionListener.wrap(targetNodes -> {
            final AtomicBoolean cancelled = new AtomicBoolean();
            final ExchangeSourceHandler sourceHandler = exchangeService.createSourceHandler(
                sessionId,
                queryPragmas.exchangeBufferSize(),
                ESQL_THREAD_POOL_NAME
            );
            try (
                Releasable ignored = sourceHandler::decRef;
                RefCountingListener refs = new RefCountingListener(listener.map(unused -> collectedPages))
            ) {
                // wait until the source handler is completed
                sourceHandler.addCompletionListener(refs.acquire());
                // run compute on the coordinator
                runCompute(rootTask, computeContext, coordinatorPlan, cancelOnFailure(rootTask, cancelled, refs.acquire()));
                // link with exchange sinks
                // link with exchange sinks
                if (targetNodes.isEmpty()) {
                    sourceHandler.addRemoteSink(RemoteSink.EMPTY, 1);
                } else {
                    for (TargetNode targetNode : targetNodes) {
                        var remoteSink = exchangeService.newRemoteSink(rootTask, sessionId, transportService, targetNode.node);
                        sourceHandler.addRemoteSink(remoteSink, queryPragmas.concurrentExchangeClients());
                    }
                }
                // dispatch compute requests to data nodes
                for (TargetNode targetNode : targetNodes) {
                    transportService.sendChildRequest(
                        targetNode.node,
                        DATA_ACTION_NAME,
                        new DataNodeRequest(sessionId, configuration, targetNode.shardIds, targetNode.aliasFilters, dataNodePlan),
                        rootTask,
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<TransportResponse>(
                            cancelOnFailure(rootTask, cancelled, refs.acquire()).map(unused -> null),
                            DataNodeResponse::new
                        )
                    );
                }
            }
        }, listener::onFailure));
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
        List<Driver> drivers = new ArrayList<>();
        listener = ActionListener.releaseAfter(listener, () -> Releasables.close(drivers));
        try {
            LocalExecutionPlanner planner = new LocalExecutionPlanner(
                context.sessionId,
                task,
                bigArrays,
                threadPool,
                context.configuration,
                exchangeService,
                enrichLookupService,
                new EsPhysicalOperationProviders(context.searchContexts)
            );

            LOGGER.info("Received physical plan:\n{}", plan);
            plan = PlannerUtils.localPlan(context.searchContexts, context.configuration, plan);
            LocalExecutionPlanner.LocalExecutionPlan localExecutionPlan = planner.plan(plan);

            LOGGER.info("Local execution plan:\n{}", localExecutionPlan.describe());
            drivers.addAll(localExecutionPlan.createDrivers(context.sessionId));
            if (drivers.isEmpty()) {
                throw new IllegalStateException("no drivers created");
            }
            LOGGER.info("using {} drivers", drivers.size());
            driverRunner.executeDrivers(task, drivers, listener.map(unused -> null));
        } catch (Exception e) {
            listener.onFailure(e);
        }
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
                targetShard.awaitShardSearchActive(ignored -> {
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
                    if (concreteIndices.contains(shardId.getIndexName()) == false) {
                        continue;
                    }
                    if (group.skipped() || group.allocatedNodes().isEmpty()) {
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
            final var sessionId = request.sessionId();
            var listener = new ChannelActionListener<DataNodeResponse>(channel);
            acquireSearchContexts(request.shardIds(), request.aliasFilters(), ActionListener.wrap(searchContexts -> {
                Releasable releasable = () -> Releasables.close(
                    () -> Releasables.close(searchContexts),
                    () -> exchangeService.completeSinkHandler(sessionId)
                );
                exchangeService.createSinkHandler(sessionId, request.pragmas().exchangeBufferSize());
                runCompute(
                    (CancellableTask) task,
                    new ComputeContext(sessionId, searchContexts, request.configuration()),
                    request.plan(),
                    ActionListener.releaseAfter(listener.map(unused -> new DataNodeResponse()), releasable)
                );
            }, listener::onFailure));
        }
    }

    record ComputeContext(String sessionId, List<SearchContext> searchContexts, EsqlConfiguration configuration) {}
}
