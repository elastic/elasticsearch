/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.compute.EsqlRefCountingListener;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlSearchShardsAction;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME;

/**
 * Handles computes within a single cluster by dispatching {@link DataNodeRequest} to data nodes
 * and executing these computes on the data nodes.
 */
final class DataNodeComputeHandler implements TransportRequestHandler<DataNodeRequest> {
    private final ComputeService computeService;
    private final SearchService searchService;
    private final TransportService transportService;
    private final ExchangeService exchangeService;
    private final Executor esqlExecutor;

    DataNodeComputeHandler(
        ComputeService computeService,
        SearchService searchService,
        TransportService transportService,
        ExchangeService exchangeService,
        Executor esqlExecutor
    ) {
        this.computeService = computeService;
        this.searchService = searchService;
        this.transportService = transportService;
        this.exchangeService = exchangeService;
        this.esqlExecutor = esqlExecutor;
        transportService.registerRequestHandler(ComputeService.DATA_ACTION_NAME, esqlExecutor, DataNodeRequest::new, this);
    }

    void startComputeOnDataNodes(
        String sessionId,
        String clusterAlias,
        CancellableTask parentTask,
        Configuration configuration,
        PhysicalPlan dataNodePlan,
        Set<String> concreteIndices,
        OriginalIndices originalIndices,
        ExchangeSourceHandler exchangeSource,
        EsqlExecutionInfo executionInfo,
        ComputeListener computeListener
    ) {
        QueryBuilder requestFilter = PlannerUtils.requestTimestampFilter(dataNodePlan);
        var lookupListener = ActionListener.releaseAfter(computeListener.acquireAvoid(), exchangeSource.addEmptySink());
        // SearchShards API can_match is done in lookupDataNodes
        lookupDataNodes(parentTask, clusterAlias, requestFilter, concreteIndices, originalIndices, ActionListener.wrap(dataNodeResult -> {
            try (EsqlRefCountingListener refs = new EsqlRefCountingListener(lookupListener)) {
                // update ExecutionInfo with shard counts (total and skipped)
                executionInfo.swapCluster(
                    clusterAlias,
                    (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setTotalShards(dataNodeResult.totalShards())
                        // do not set successful or failed shard count here - do it when search is done
                        .setSkippedShards(dataNodeResult.skippedShards())
                        .build()
                );

                // For each target node, first open a remote exchange on the remote node, then link the exchange source to
                // the new remote exchange sink, and initialize the computation on the target node via data-node-request.
                for (DataNode node : dataNodeResult.dataNodes()) {
                    var queryPragmas = configuration.pragmas();
                    var childSessionId = computeService.newChildSession(sessionId);
                    ExchangeService.openExchange(
                        transportService,
                        node.connection,
                        childSessionId,
                        queryPragmas.exchangeBufferSize(),
                        esqlExecutor,
                        refs.acquire().delegateFailureAndWrap((l, unused) -> {
                            var remoteSink = exchangeService.newRemoteSink(parentTask, childSessionId, transportService, node.connection);
                            exchangeSource.addRemoteSink(remoteSink, true, queryPragmas.concurrentExchangeClients(), ActionListener.noop());
                            ActionListener<ComputeResponse> computeResponseListener = computeListener.acquireCompute(clusterAlias);
                            var dataNodeListener = ActionListener.runBefore(computeResponseListener, () -> l.onResponse(null));
                            final boolean sameNode = transportService.getLocalNode().getId().equals(node.connection.getNode().getId());
                            var dataNodeRequest = new DataNodeRequest(
                                childSessionId,
                                configuration,
                                clusterAlias,
                                node.shardIds,
                                node.aliasFilters,
                                dataNodePlan,
                                originalIndices.indices(),
                                originalIndices.indicesOptions(),
                                sameNode == false && queryPragmas.nodeLevelReduction()
                            );
                            transportService.sendChildRequest(
                                node.connection,
                                ComputeService.DATA_ACTION_NAME,
                                dataNodeRequest,
                                parentTask,
                                TransportRequestOptions.EMPTY,
                                new ActionListenerResponseHandler<>(dataNodeListener, ComputeResponse::new, esqlExecutor)
                            );
                        })
                    );
                }
            }
        }, lookupListener::onFailure));
    }

    private void acquireSearchContexts(
        String clusterAlias,
        List<ShardId> shardIds,
        Configuration configuration,
        Map<Index, AliasFilter> aliasFilters,
        ActionListener<List<SearchContext>> listener
    ) {
        final List<IndexShard> targetShards = new ArrayList<>();
        try {
            for (ShardId shardId : shardIds) {
                var indexShard = searchService.getIndicesService().indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
                targetShards.add(indexShard);
            }
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        final var doAcquire = ActionRunnable.supply(listener, () -> {
            final List<SearchContext> searchContexts = new ArrayList<>(targetShards.size());
            boolean success = false;
            try {
                for (IndexShard shard : targetShards) {
                    var aliasFilter = aliasFilters.getOrDefault(shard.shardId().getIndex(), AliasFilter.EMPTY);
                    var shardRequest = new ShardSearchRequest(
                        shard.shardId(),
                        configuration.absoluteStartedTimeInMillis(),
                        aliasFilter,
                        clusterAlias
                    );
                    // TODO: `searchService.createSearchContext` allows opening search contexts without limits,
                    // we need to limit the number of active search contexts here or in SearchService
                    SearchContext context = searchService.createSearchContext(shardRequest, SearchService.NO_TIMEOUT);
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
        final AtomicBoolean waitedForRefreshes = new AtomicBoolean();
        try (RefCountingRunnable refs = new RefCountingRunnable(() -> {
            if (waitedForRefreshes.get()) {
                esqlExecutor.execute(doAcquire);
            } else {
                doAcquire.run();
            }
        })) {
            for (IndexShard targetShard : targetShards) {
                final Releasable ref = refs.acquire();
                targetShard.ensureShardSearchActive(await -> {
                    try (ref) {
                        if (await) {
                            waitedForRefreshes.set(true);
                        }
                    }
                });
            }
        }
    }

    record DataNode(Transport.Connection connection, List<ShardId> shardIds, Map<Index, AliasFilter> aliasFilters) {

    }

    /**
     * Result from lookupDataNodes where can_match is performed to determine what shards can be skipped
     * and which target nodes are needed for running the ES|QL query
     *
     * @param dataNodes     list of DataNode to perform the ES|QL query on
     * @param totalShards   Total number of shards (from can_match phase), including skipped shards
     * @param skippedShards Number of skipped shards (from can_match phase)
     */
    record DataNodeResult(List<DataNode> dataNodes, int totalShards, int skippedShards) {}

    /**
     * Performs can_match and find the target nodes for the given target indices and filter.
     * <p>
     * Ideally, the search_shards API should be called before the field-caps API; however, this can lead
     * to a situation where the column structure (i.e., matched data types) differs depending on the query.
     */
    private void lookupDataNodes(
        Task parentTask,
        String clusterAlias,
        QueryBuilder filter,
        Set<String> concreteIndices,
        OriginalIndices originalIndices,
        ActionListener<DataNodeResult> listener
    ) {
        ActionListener<SearchShardsResponse> searchShardsListener = listener.map(resp -> {
            Map<String, DiscoveryNode> nodes = new HashMap<>();
            for (DiscoveryNode node : resp.getNodes()) {
                nodes.put(node.getId(), node);
            }
            Map<String, List<ShardId>> nodeToShards = new HashMap<>();
            Map<String, Map<Index, AliasFilter>> nodeToAliasFilters = new HashMap<>();
            int totalShards = 0;
            int skippedShards = 0;
            for (SearchShardsGroup group : resp.getGroups()) {
                var shardId = group.shardId();
                if (group.allocatedNodes().isEmpty()) {
                    throw new ShardNotFoundException(group.shardId(), "no shard copies found {}", group.shardId());
                }
                if (concreteIndices.contains(shardId.getIndexName()) == false) {
                    continue;
                }
                totalShards++;
                if (group.skipped()) {
                    skippedShards++;
                    continue;
                }
                String targetNode = group.allocatedNodes().get(0);
                nodeToShards.computeIfAbsent(targetNode, k -> new ArrayList<>()).add(shardId);
                AliasFilter aliasFilter = resp.getAliasFilters().get(shardId.getIndex().getUUID());
                if (aliasFilter != null) {
                    nodeToAliasFilters.computeIfAbsent(targetNode, k -> new HashMap<>()).put(shardId.getIndex(), aliasFilter);
                }
            }
            List<DataNode> dataNodes = new ArrayList<>(nodeToShards.size());
            for (Map.Entry<String, List<ShardId>> e : nodeToShards.entrySet()) {
                DiscoveryNode node = nodes.get(e.getKey());
                Map<Index, AliasFilter> aliasFilters = nodeToAliasFilters.getOrDefault(e.getKey(), Map.of());
                dataNodes.add(new DataNode(transportService.getConnection(node), e.getValue(), aliasFilters));
            }
            return new DataNodeResult(dataNodes, totalShards, skippedShards);
        });
        SearchShardsRequest searchShardsRequest = new SearchShardsRequest(
            originalIndices.indices(),
            originalIndices.indicesOptions(),
            filter,
            null,
            null,
            false,
            clusterAlias
        );
        transportService.sendChildRequest(
            transportService.getLocalNode(),
            EsqlSearchShardsAction.TYPE.name(),
            searchShardsRequest,
            parentTask,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(searchShardsListener, SearchShardsResponse::new, esqlExecutor)
        );
    }

    private class DataNodeRequestExecutor {
        private final DataNodeRequest request;
        private final CancellableTask parentTask;
        private final ExchangeSinkHandler exchangeSink;
        private final ComputeListener computeListener;
        private final int maxConcurrentShards;
        private final ExchangeSink blockingSink; // block until we have completed on all shards or the coordinator has enough data

        DataNodeRequestExecutor(
            DataNodeRequest request,
            CancellableTask parentTask,
            ExchangeSinkHandler exchangeSink,
            int maxConcurrentShards,
            ComputeListener computeListener
        ) {
            this.request = request;
            this.parentTask = parentTask;
            this.exchangeSink = exchangeSink;
            this.computeListener = computeListener;
            this.maxConcurrentShards = maxConcurrentShards;
            this.blockingSink = exchangeSink.createExchangeSink();
        }

        void start() {
            parentTask.addListener(
                () -> exchangeService.finishSinkHandler(request.sessionId(), new TaskCancelledException(parentTask.getReasonCancelled()))
            );
            runBatch(0);
        }

        private void runBatch(int startBatchIndex) {
            final Configuration configuration = request.configuration();
            final String clusterAlias = request.clusterAlias();
            final var sessionId = request.sessionId();
            final int endBatchIndex = Math.min(startBatchIndex + maxConcurrentShards, request.shardIds().size());
            List<ShardId> shardIds = request.shardIds().subList(startBatchIndex, endBatchIndex);
            ActionListener<ComputeResponse> batchListener = new ActionListener<>() {
                final ActionListener<ComputeResponse> ref = computeListener.acquireCompute();

                @Override
                public void onResponse(ComputeResponse result) {
                    try {
                        onBatchCompleted(endBatchIndex);
                    } finally {
                        ref.onResponse(result);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        exchangeService.finishSinkHandler(request.sessionId(), e);
                    } finally {
                        ref.onFailure(e);
                    }
                }
            };
            acquireSearchContexts(clusterAlias, shardIds, configuration, request.aliasFilters(), ActionListener.wrap(searchContexts -> {
                assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH, ESQL_WORKER_THREAD_POOL_NAME);
                var computeContext = new ComputeContext(
                    sessionId,
                    clusterAlias,
                    searchContexts,
                    configuration,
                    configuration.newFoldContext(),
                    null,
                    exchangeSink
                );
                computeService.runCompute(parentTask, computeContext, request.plan(), batchListener);
            }, batchListener::onFailure));
        }

        private void onBatchCompleted(int lastBatchIndex) {
            if (lastBatchIndex < request.shardIds().size() && exchangeSink.isFinished() == false) {
                runBatch(lastBatchIndex);
            } else {
                // don't return until all pages are fetched
                var completionListener = computeListener.acquireAvoid();
                exchangeSink.addCompletionListener(
                    ActionListener.runAfter(completionListener, () -> exchangeService.finishSinkHandler(request.sessionId(), null))
                );
                blockingSink.finish();
            }
        }
    }

    private void runComputeOnDataNode(
        CancellableTask task,
        String externalId,
        PhysicalPlan reducePlan,
        DataNodeRequest request,
        ComputeListener computeListener
    ) {
        var parentListener = computeListener.acquireAvoid();
        try {
            // run compute with target shards
            var internalSink = exchangeService.createSinkHandler(request.sessionId(), request.pragmas().exchangeBufferSize());
            DataNodeRequestExecutor dataNodeRequestExecutor = new DataNodeRequestExecutor(
                request,
                task,
                internalSink,
                request.configuration().pragmas().maxConcurrentShardsPerNode(),
                computeListener
            );
            dataNodeRequestExecutor.start();
            // run the node-level reduction
            var externalSink = exchangeService.getSinkHandler(externalId);
            task.addListener(() -> exchangeService.finishSinkHandler(externalId, new TaskCancelledException(task.getReasonCancelled())));
            var exchangeSource = new ExchangeSourceHandler(1, esqlExecutor, computeListener.acquireAvoid());
            exchangeSource.addRemoteSink(internalSink::fetchPageAsync, true, 1, ActionListener.noop());
            ActionListener<ComputeResponse> reductionListener = computeListener.acquireCompute();
            computeService.runCompute(
                task,
                new ComputeContext(
                    request.sessionId(),
                    request.clusterAlias(),
                    List.of(),
                    request.configuration(),
                    new FoldContext(request.pragmas().foldLimit().getBytes()),
                    exchangeSource,
                    externalSink
                ),
                reducePlan,
                ActionListener.wrap(resp -> {
                    // don't return until all pages are fetched
                    externalSink.addCompletionListener(ActionListener.running(() -> {
                        exchangeService.finishSinkHandler(externalId, null);
                        reductionListener.onResponse(resp);
                    }));
                }, e -> {
                    exchangeService.finishSinkHandler(externalId, e);
                    reductionListener.onFailure(e);
                })
            );
            parentListener.onResponse(null);
        } catch (Exception e) {
            exchangeService.finishSinkHandler(externalId, e);
            exchangeService.finishSinkHandler(request.sessionId(), e);
            parentListener.onFailure(e);
        }
    }

    @Override
    public void messageReceived(DataNodeRequest request, TransportChannel channel, Task task) {
        final ActionListener<ComputeResponse> listener = new ChannelActionListener<>(channel);
        final PhysicalPlan reductionPlan;
        if (request.plan() instanceof ExchangeSinkExec plan) {
            reductionPlan = ComputeService.reductionPlan(plan, request.runNodeLevelReduction());
        } else {
            listener.onFailure(new IllegalStateException("expected exchange sink for a remote compute; got " + request.plan()));
            return;
        }
        final String sessionId = request.sessionId();
        request = new DataNodeRequest(
            sessionId + "[n]", // internal session
            request.configuration(),
            request.clusterAlias(),
            request.shardIds(),
            request.aliasFilters(),
            request.plan(),
            request.indices(),
            request.indicesOptions(),
            request.runNodeLevelReduction()
        );
        try (var computeListener = ComputeListener.create(transportService, (CancellableTask) task, listener)) {
            runComputeOnDataNode((CancellableTask) task, sessionId, reductionPlan, request, computeListener);
        }
    }
}
