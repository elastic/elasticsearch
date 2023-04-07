/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.IndicesOptions;
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
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
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
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.util.Holder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public ComputeService(
        SearchService searchService,
        ClusterService clusterService,
        TransportService transportService,
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
            ThreadPool.Names.SEARCH,
            DataNodeRequest::new,
            new DataNodeRequestHandler()
        );
        this.driverRunner = new DriverTaskRunner(transportService, threadPool);
        this.exchangeService = new ExchangeService(transportService, threadPool);
    }

    public void execute(
        String sessionId,
        CancellableTask rootTask,
        PhysicalPlan physicalPlan,
        EsqlConfiguration configuration,
        ActionListener<List<Page>> outListener
    ) {
        final List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());
        String[] indexNames = physicalPlan.collect(l -> l instanceof EsQueryExec)
            .stream()
            .map(qe -> ((EsQueryExec) qe).index().concreteIndices())
            .flatMap(Collection::stream)
            .distinct()
            .toArray(String[]::new);
        PhysicalPlan planForDataNodes = planForDataNodes(physicalPlan);
        PhysicalPlan planForCoordinator = new OutputExec(physicalPlan, (c, p) -> collectedPages.add(p));
        QueryPragmas queryPragmas = configuration.pragmas();
        if (indexNames.length == 0 || planForDataNodes == null) {
            runCompute(sessionId, rootTask, planForCoordinator, List.of(), queryPragmas, outListener.map(unused -> collectedPages));
            return;
        }
        ClusterState clusterState = clusterService.state();
        Map<String, List<ShardId>> targetNodes = computeTargetNodes(clusterState, indexNames);
        final ExchangeSourceHandler sourceHandler = exchangeService.createSourceHandler(sessionId, queryPragmas.exchangeBufferSize());
        final ActionListener<Void> listener = ActionListener.releaseAfter(
            outListener.map(unused -> collectedPages),
            () -> exchangeService.completeSourceHandler(sessionId)
        );
        try (RefCountingListener refs = new RefCountingListener(listener)) {
            // run compute on the coordinator
            runCompute(sessionId, rootTask, planForCoordinator, List.of(), queryPragmas, cancelOnFailure(rootTask, refs.acquire()));
            // link with exchange sinks
            for (String targetNode : targetNodes.keySet()) {
                final var remoteSink = exchangeService.newRemoteSink(rootTask, sessionId, clusterState.nodes().get(targetNode));
                sourceHandler.addRemoteSink(remoteSink, queryPragmas.concurrentExchangeClients());
            }
            // dispatch compute requests to data nodes
            for (Map.Entry<String, List<ShardId>> e : targetNodes.entrySet()) {
                DiscoveryNode targetNode = clusterState.nodes().get(e.getKey());
                transportService.sendChildRequest(
                    targetNode,
                    DATA_ACTION_NAME,
                    new DataNodeRequest(sessionId, queryPragmas, e.getValue(), planForDataNodes),
                    rootTask,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<TransportResponse>(
                        cancelOnFailure(rootTask, refs.acquire()).map(unused -> null),
                        DataNodeResponse::new
                    )
                );
            }
        }
    }

    private ActionListener<Void> cancelOnFailure(CancellableTask task, ActionListener<Void> listener) {
        return listener.delegateResponse((l, e) -> {
            l.onFailure(e);
            transportService.getTaskManager().cancelTaskAndDescendants(task, "cancelled", false, ActionListener.noop());
        });
    }

    void runCompute(
        String sessionId,
        Task task,
        PhysicalPlan plan,
        List<SearchContext> searchContexts,
        QueryPragmas queryPragmas,
        ActionListener<Void> listener
    ) {
        List<Driver> drivers = new ArrayList<>();
        listener = ActionListener.releaseAfter(listener, () -> Releasables.close(drivers));
        try {
            LocalExecutionPlanner planner = new LocalExecutionPlanner(
                sessionId,
                bigArrays,
                threadPool,
                queryPragmas,
                exchangeService,
                new EsPhysicalOperationProviders(searchContexts)
            );
            LocalExecutionPlanner.LocalExecutionPlan localExecutionPlan = planner.plan(plan);
            LOGGER.info("Local execution plan:\n{}", localExecutionPlan.describe());
            drivers.addAll(localExecutionPlan.createDrivers(sessionId));
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

    public static PhysicalPlan planForDataNodes(PhysicalPlan plan) {
        Holder<ExchangeExec> exchange = new Holder<>();
        plan.forEachDown(ExchangeExec.class, e -> {
            if (e.mode() == ExchangeExec.Mode.REMOTE_SINK) {
                exchange.set(e);
            }
        });
        return exchange.get();
    }

    private static class DataNodeRequest extends TransportRequest implements IndicesRequest {
        private static final PlanNameRegistry planNameRegistry = new PlanNameRegistry();
        private final String sessionId;
        private final QueryPragmas pragmas;
        private final List<ShardId> shardIds;
        private final PhysicalPlan plan;

        private String[] indices; // lazily computed

        DataNodeRequest(String sessionId, QueryPragmas pragmas, List<ShardId> shardIds, PhysicalPlan plan) {
            this.sessionId = sessionId;
            this.pragmas = pragmas;
            this.shardIds = shardIds;
            this.plan = plan;
        }

        DataNodeRequest(StreamInput in) throws IOException {
            this.sessionId = in.readString();
            this.pragmas = new QueryPragmas(in);
            this.shardIds = in.readList(ShardId::new);
            this.plan = new PlanStreamInput(in, planNameRegistry, in.namedWriteableRegistry()).readPhysicalPlanNode();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(sessionId);
            pragmas.writeTo(out);
            out.writeList(shardIds);
            new PlanStreamOutput(out, planNameRegistry).writePhysicalPlanNode(plan);
        }

        @Override
        public String[] indices() {
            if (indices == null) {
                indices = shardIds.stream().map(ShardId::getIndexName).distinct().toArray(String[]::new);
            }
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers) {
                @Override
                public String getDescription() {
                    return "shards=" + shardIds + " plan=" + plan;
                }
            };
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
            final var sessionId = request.sessionId;
            var listener = new ChannelActionListener<DataNodeResponse>(channel);
            acquireSearchContexts(request.shardIds, ActionListener.wrap(searchContexts -> {
                Releasable releasable = () -> Releasables.close(
                    () -> Releasables.close(searchContexts),
                    () -> exchangeService.completeSinkHandler(sessionId)
                );
                exchangeService.createSinkHandler(sessionId, request.pragmas.exchangeBufferSize());
                runCompute(
                    sessionId,
                    task,
                    request.plan,
                    searchContexts,
                    request.pragmas,
                    ActionListener.releaseAfter(listener.map(unused -> new DataNodeResponse()), releasable)
                );
            }, listener::onFailure));
        }
    }
}
