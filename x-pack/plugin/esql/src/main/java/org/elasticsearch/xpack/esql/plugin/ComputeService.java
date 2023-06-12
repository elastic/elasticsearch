/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.CountDown;
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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_THREAD_POOL_NAME;

/**
 * Computes the result of a {@link PhysicalPlan}.
 */
public class ComputeService {
    private static final Logger LOGGER = LogManager.getLogger(ComputeService.class);
    private final SearchService searchService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final BigArrays bigArrays;
    private final TransportService transportService;
    private final DriverTaskRunner driverRunner;
    private final ExchangeService exchangeService;
    private final EnrichLookupService enrichLookupService;

    public ComputeService(
        SearchService searchService,
        ClusterService clusterService,
        TransportService transportService,
        ExchangeService exchangeService,
        EnrichLookupService enrichLookupService,
        ThreadPool threadPool,
        BigArrays bigArrays
    ) {
        this.searchService = searchService;
        this.clusterService = clusterService;
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
        PhysicalPlan coordinatorPlan = coordinatorAndDataNodePlan.v1();
        PhysicalPlan dataNodePlan = coordinatorAndDataNodePlan.v2();

        var indexNames = PlannerUtils.planIndices(dataNodePlan);

        final List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());
        coordinatorPlan = new OutputExec(coordinatorPlan, collectedPages::add);
        QueryPragmas queryPragmas = configuration.pragmas();

        var computeContext = new ComputeContext(sessionId, List.of(), configuration);

        if (indexNames.length == 0) {
            runCompute(rootTask, computeContext, coordinatorPlan, listener.map(unused -> collectedPages));
            return;
        }

        ClusterState clusterState = clusterService.state();
        Map<String, List<ShardId>> targetNodes = computeTargetNodes(clusterState, indexNames);

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
            if (targetNodes.isEmpty()) {
                sourceHandler.addRemoteSink(RemoteSink.EMPTY, 1);
            } else {
                for (String targetNode : targetNodes.keySet()) {
                    DiscoveryNode remoteNode = clusterState.nodes().get(targetNode);
                    var remoteSink = exchangeService.newRemoteSink(rootTask, sessionId, transportService, remoteNode);
                    sourceHandler.addRemoteSink(remoteSink, queryPragmas.concurrentExchangeClients());
                }
            }
            // dispatch compute requests to data nodes
            for (Map.Entry<String, List<ShardId>> e : targetNodes.entrySet()) {
                DiscoveryNode targetNode = clusterState.nodes().get(e.getKey());
                transportService.sendChildRequest(
                    targetNode,
                    DATA_ACTION_NAME,
                    new DataNodeRequest(sessionId, configuration, e.getValue(), dataNodePlan),
                    rootTask,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<TransportResponse>(
                        cancelOnFailure(rootTask, cancelled, refs.acquire()).map(unused -> null),
                        DataNodeResponse::new
                    )
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

    private void acquireSearchContexts(List<ShardId> shardIds, ActionListener<List<SearchContext>> listener) {
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
                                    ShardSearchRequest shardSearchLocalRequest = new ShardSearchRequest(
                                        shard.shardId(),
                                        0,
                                        AliasFilter.EMPTY
                                    );
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

    private Map<String, List<ShardId>> computeTargetNodes(ClusterState clusterState, String[] indices) {
        // TODO: Integrate with ARS
        GroupShardsIterator<ShardIterator> shardIts = clusterService.operationRouting().searchShards(clusterState, indices, null, null);
        Map<String, List<ShardId>> nodes = new HashMap<>();
        for (ShardIterator shardIt : shardIts) {
            ShardRouting shardRouting = shardIt.nextOrNull();
            if (shardRouting != null) {
                nodes.computeIfAbsent(shardRouting.currentNodeId(), k -> new ArrayList<>()).add(shardRouting.shardId());
            }
        }
        return nodes;
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
            acquireSearchContexts(request.shardIds(), ActionListener.wrap(searchContexts -> {
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
