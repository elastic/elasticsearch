/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
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
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.PlanConcurrencyCalculator;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Handles computes within a single cluster by dispatching {@link DataNodeRequest} to data nodes
 * and executing these computes on the data nodes.
 */
final class DataNodeComputeHandler implements TransportRequestHandler<DataNodeRequest> {

    private static final TransportVersion ESQL_RETRY_ON_SHARD_LEVEL_FAILURE = TransportVersion.fromName(
        "esql_retry_on_shard_level_failure"
    );

    private final ComputeService computeService;
    private final SearchService searchService;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final TransportService transportService;
    private final ExchangeService exchangeService;
    private final Executor searchExecutor;
    private final ThreadPool threadPool;

    DataNodeComputeHandler(
        ComputeService computeService,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        SearchService searchService,
        TransportService transportService,
        ExchangeService exchangeService,
        Executor searchExecutor
    ) {
        this.computeService = computeService;
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.searchService = searchService;
        this.transportService = transportService;
        this.exchangeService = exchangeService;
        this.searchExecutor = searchExecutor;
        this.threadPool = transportService.getThreadPool();
        transportService.registerRequestHandler(ComputeService.DATA_ACTION_NAME, searchExecutor, DataNodeRequest::new, this);
    }

    void startComputeOnDataNodes(
        String sessionId,
        String clusterAlias,
        CancellableTask parentTask,
        EsqlFlags flags,
        Configuration configuration,
        PhysicalPlan dataNodePlan,
        Set<String> concreteIndices,
        OriginalIndices originalIndices,
        ExchangeSourceHandler exchangeSource,
        Runnable runOnTaskFailure,
        ActionListener<ComputeResponse> outListener
    ) {
        Integer maxConcurrentNodesPerCluster = PlanConcurrencyCalculator.INSTANCE.calculateNodesConcurrency(dataNodePlan, configuration);

        new DataNodeRequestSender(
            clusterService,
            projectResolver,
            transportService,
            searchExecutor,
            parentTask,
            originalIndices,
            PlannerUtils.canMatchFilter(flags, configuration, clusterService.state().getMinTransportVersion(), dataNodePlan),
            clusterAlias,
            configuration.allowPartialResults(),
            maxConcurrentNodesPerCluster == null ? -1 : maxConcurrentNodesPerCluster,
            configuration.pragmas().unavailableShardResolutionAttempts()
        ) {
            @Override
            protected void sendRequest(
                DiscoveryNode node,
                List<DataNodeRequest.Shard> shards,
                Map<Index, AliasFilter> aliasFilters,
                NodeListener nodeListener
            ) {
                if (exchangeSource.isFinished()) {
                    nodeListener.onSkip();
                    return;
                }

                final AtomicLong pagesFetched = new AtomicLong();
                var listener = ActionListener.wrap(nodeListener::onResponse, e -> nodeListener.onFailure(e, pagesFetched.get() > 0));
                final Transport.Connection connection;
                try {
                    connection = transportService.getConnection(node);
                } catch (Exception e) {
                    listener.onFailure(e);
                    return;
                }
                var queryPragmas = configuration.pragmas();
                var childSessionId = computeService.newChildSession(sessionId);
                // For each target node, first open a remote exchange on the remote node, then link the exchange source to
                // the new remote exchange sink, and initialize the computation on the target node via data-node-request.
                ExchangeService.openExchange(
                    transportService,
                    connection,
                    childSessionId,
                    queryPragmas.exchangeBufferSize(),
                    searchExecutor,
                    listener.delegateFailureAndWrap((l, unused) -> {
                        final Runnable onGroupFailure;
                        final CancellableTask groupTask;
                        if (configuration.allowPartialResults()) {
                            try {
                                groupTask = computeService.createGroupTask(
                                    parentTask,
                                    () -> "compute group: data-node [" + node.getName() + "], shards [" + shards + "]"
                                );
                            } catch (TaskCancelledException e) {
                                l.onFailure(e);
                                return;
                            }
                            onGroupFailure = computeService.cancelQueryOnFailure(groupTask);
                            l = ActionListener.runAfter(l, () -> transportService.getTaskManager().unregister(groupTask));
                        } else {
                            groupTask = parentTask;
                            onGroupFailure = runOnTaskFailure;
                        }
                        final AtomicReference<DataNodeComputeResponse> nodeResponseRef = new AtomicReference<>();
                        try (
                            var computeListener = new ComputeListener(threadPool, onGroupFailure, l.map(ignored -> nodeResponseRef.get()))
                        ) {
                            final boolean sameNodeAsCoordinator = transportService.getLocalNode()
                                .getId()
                                .equals(connection.getNode().getId());
                            boolean enableReduceNodeLateMaterialization = EsqlCapabilities.Cap.ENABLE_REDUCE_NODE_LATE_MATERIALIZATION
                                .isEnabled();
                            var dataNodeRequest = new DataNodeRequest(
                                childSessionId,
                                configuration,
                                clusterAlias,
                                shards,
                                aliasFilters,
                                dataNodePlan,
                                originalIndices.indices(),
                                originalIndices.indicesOptions(),
                                // If the coordinator and data node are the same, we don't need to run the node-level reduction (except for
                                // TopN late materialization, listed below), as the node-reduce driver would end up doing the exact same
                                // work as the final driver.
                                queryPragmas.nodeLevelReduction() && sameNodeAsCoordinator == false,
                                queryPragmas.nodeLevelReduction() && enableReduceNodeLateMaterialization,
                                false
                            );
                            transportService.sendChildRequest(
                                connection,
                                ComputeService.DATA_ACTION_NAME,
                                dataNodeRequest,
                                groupTask,
                                TransportRequestOptions.EMPTY,
                                new ActionListenerResponseHandler<>(computeListener.acquireCompute().map(r -> {
                                    nodeResponseRef.set(r);
                                    return r.completionInfo();
                                }), DataNodeComputeResponse::new, searchExecutor)
                            );
                            final var remoteSink = exchangeService.newRemoteSink(groupTask, childSessionId, transportService, connection);
                            exchangeSource.addRemoteSink(
                                remoteSink,
                                configuration.allowPartialResults() == false,
                                pagesFetched::incrementAndGet,
                                queryPragmas.concurrentExchangeClients(),
                                computeListener.acquireAvoid()
                            );
                        }
                    })
                );
            }
        }.startComputeOnDataNodes(
            concreteIndices,
            runOnTaskFailure,
            ActionListener.releaseAfter(outListener, exchangeSource.addEmptySink())
        );
    }

    void startExternalComputeOnDataNodes(
        String sessionId,
        CancellableTask parentTask,
        EsqlFlags flags,
        Configuration configuration,
        ExchangeSinkExec dataNodePlan,
        ExternalDistributionPlan distributionPlan,
        ExchangeSourceHandler exchangeSource,
        Runnable runOnTaskFailure,
        ComputeListener parentComputeListener
    ) {
        var queryPragmas = configuration.pragmas();
        boolean allowPartial = configuration.allowPartialResults();
        boolean sentAny = false;
        int nodesWithSplits = 0;
        AtomicInteger failedNodes = new AtomicInteger(0);

        final var keepAlive = new ExchangeSourceLinkKeepAlive(exchangeSource);
        try {
            for (Map.Entry<String, List<ExternalSplit>> entry : distributionPlan.nodeAssignments().entrySet()) {
                String nodeId = entry.getKey();
                List<ExternalSplit> nodeSplits = entry.getValue();
                if (nodeSplits.isEmpty()) {
                    continue;
                }
                nodesWithSplits++;

                DiscoveryNode node = clusterService.state().nodes().get(nodeId);
                if (node == null) {
                    var nodeError = new IllegalStateException(
                        "node [" + nodeId + "] assigned [" + nodeSplits.size() + "] external splits not found in cluster state"
                    );
                    if (allowPartial) {
                        LOGGER.warn(
                            "node [{}] assigned {} external splits is no longer in the cluster state; skipping (partial results enabled)",
                            nodeId,
                            nodeSplits.size()
                        );
                        failedNodes.incrementAndGet();
                        parentComputeListener.acquireCompute().onResponse(DriverCompletionInfo.EMPTY);
                        continue;
                    }
                    LOGGER.warn(
                        "node [{}] assigned {} external splits is no longer in the cluster state; failing external distribution",
                        nodeId,
                        nodeSplits.size()
                    );
                    parentComputeListener.acquireCompute().onFailure(nodeError);
                    return;
                }

                final Transport.Connection connection;
                try {
                    connection = transportService.getConnection(node);
                } catch (Exception e) {
                    if (allowPartial) {
                        LOGGER.warn(
                            "failed to connect to node [{}] ({}) for external source execution with {} splits; skipping (partial results)",
                            nodeId,
                            node.getName(),
                            nodeSplits.size(),
                            e
                        );
                        failedNodes.incrementAndGet();
                        parentComputeListener.acquireCompute().onResponse(DriverCompletionInfo.EMPTY);
                        continue;
                    }
                    LOGGER.warn(
                        "failed to connect to node [{}] ({}) for external source execution with {} splits",
                        nodeId,
                        node.getName(),
                        nodeSplits.size(),
                        e
                    );
                    parentComputeListener.acquireCompute().onFailure(e);
                    return;
                }

                sentAny = true;
                var childSessionId = computeService.newChildSession(sessionId);
                keepAlive.track();
                final AtomicBoolean nodeDone = new AtomicBoolean(false);
                final Runnable finishNode = () -> {
                    if (nodeDone.compareAndSet(false, true)) {
                        keepAlive.done();
                    }
                };
                ActionListener<Void> openExchangeListener = parentComputeListener.acquireAvoid().delegateFailureAndWrap((l, unused) -> {
                    l = ActionListener.runAfter(l, finishNode);
                    final Runnable onGroupFailure;
                    final CancellableTask groupTask;
                    if (allowPartial) {
                        try {
                            groupTask = computeService.createGroupTask(
                                parentTask,
                                () -> "compute group: external data-node [" + node.getName() + "], splits [" + nodeSplits.size() + "]"
                            );
                        } catch (TaskCancelledException e) {
                            l.onFailure(e);
                            return;
                        }
                        onGroupFailure = computeService.cancelQueryOnFailure(groupTask);
                        l = ActionListener.runAfter(l, () -> transportService.getTaskManager().unregister(groupTask));
                    } else {
                        groupTask = parentTask;
                        onGroupFailure = runOnTaskFailure;
                    }
                    // Mirror the indexed path (startComputeOnDataNodes): forward the inner
                    // ComputeListener's accumulated DriverCompletionInfo (driver + plan profiles)
                    // into a dedicated parentComputeListener.acquireCompute() slot.
                    final ActionListener<DriverCompletionInfo> profileSlot = parentComputeListener.acquireCompute();
                    final ActionListener<Void> outerL = l;
                    try (var computeListener = new ComputeListener(threadPool, onGroupFailure, ActionListener.wrap(info -> {
                        try {
                            profileSlot.onResponse(info);
                        } finally {
                            outerL.onResponse(null);
                        }
                    }, e -> {
                        try {
                            profileSlot.onFailure(e);
                        } finally {
                            outerL.onFailure(e);
                        }
                    }))) {
                        var dataNodeRequest = new DataNodeRequest(
                            childSessionId,
                            configuration,
                            "",
                            List.of(),
                            Map.of(),
                            dataNodePlan,
                            new String[0],
                            IndicesOptions.STRICT_EXPAND_OPEN,
                            queryPragmas.nodeLevelReduction(),
                            false,
                            false,
                            nodeSplits
                        );
                        transportService.sendChildRequest(
                            connection,
                            ComputeService.DATA_ACTION_NAME,
                            dataNodeRequest,
                            groupTask,
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(
                                computeListener.acquireCompute().map(DataNodeComputeResponse::completionInfo),
                                DataNodeComputeResponse::new,
                                searchExecutor
                            )
                        );
                        var remoteSink = exchangeService.newRemoteSink(groupTask, childSessionId, transportService, connection);
                        exchangeSource.addRemoteSink(
                            remoteSink,
                            allowPartial == false,
                            () -> {},
                            queryPragmas.concurrentExchangeClients(),
                            computeListener.acquireAvoid()
                        );
                    }
                });
                ActionListener<Void> openExchangeListenerWithNodeCompletion = ActionListener.wrap(r -> {
                    try {
                        openExchangeListener.onResponse(r);
                    } catch (Exception e) {
                        try {
                            openExchangeListener.onFailure(e);
                        } finally {
                            finishNode.run();
                        }
                    }
                }, e -> {
                    try {
                        openExchangeListener.onFailure(e);
                    } finally {
                        finishNode.run();
                    }
                });
                try {
                    ExchangeService.openExchange(
                        transportService,
                        connection,
                        childSessionId,
                        queryPragmas.exchangeBufferSize(),
                        searchExecutor,
                        openExchangeListenerWithNodeCompletion
                    );
                } catch (Exception e) {
                    openExchangeListenerWithNodeCompletion.onFailure(e);
                    return;
                }
            }
            if (sentAny == false) {
                if (failedNodes.get() > 0 && failedNodes.get() >= nodesWithSplits) {
                    parentComputeListener.acquireCompute()
                        .onFailure(
                            new IllegalStateException(
                                "all [" + failedNodes.get() + "] nodes assigned external splits failed; cannot serve partial results"
                            )
                        );
                } else {
                    parentComputeListener.acquireCompute().onResponse(DriverCompletionInfo.EMPTY);
                }
            }
        } finally {
            keepAlive.done();
        }
    }

    private static final Logger LOGGER = LogManager.getLogger(DataNodeComputeHandler.class);

    /**
     * Keeps an {@link ExchangeSourceHandler} from completing while external distribution is being wired up.
     * <p>
     * The external distribution path links sinks asynchronously (after {@code openExchange} completes).
     * We hold an "empty sink" reference across that async gap so the coordinator does not observe an
     * exchange that finishes before data-node tasks have registered their remote sinks.
     */
    private static final class ExchangeSourceLinkKeepAlive {
        private final Releasable keepAlive;
        private final AtomicInteger pending = new AtomicInteger(1);
        private final AtomicBoolean closed = new AtomicBoolean(false);

        ExchangeSourceLinkKeepAlive(ExchangeSourceHandler exchangeSource) {
            this.keepAlive = exchangeSource.addEmptySink();
        }

        void track() {
            pending.incrementAndGet();
        }

        void done() {
            if (pending.decrementAndGet() == 0) {
                close();
            }
        }

        private void close() {
            if (closed.compareAndSet(false, true)) {
                keepAlive.close();
            }
        }
    }

    private class DataNodeRequestExecutor {
        private final EsqlFlags flags;
        private final DataNodeRequest request;
        private final CancellableTask parentTask;
        private final ExchangeSinkHandler exchangeSink;
        private final ComputeListener computeListener;
        private final int maxConcurrentShards;
        private final ExchangeSink blockingSink; // block until we have completed on all shards or the coordinator has enough data
        private final boolean failFastOnShardFailure;
        private final Map<ShardId, Exception> shardLevelFailures;
        private final AcquiredSearchContexts searchContexts;
        private final PlanTimeProfile planTimeProfile;
        // null for the single-sink path; set for the per-shard sorted-merge path
        private final List<ExchangeSinkHandler> perShardSinks;
        private final AtomicInteger perShardSinkCounter;

        DataNodeRequestExecutor(
            EsqlFlags flags,
            DataNodeRequest request,
            CancellableTask parentTask,
            ExchangeSinkHandler exchangeSink,
            int maxConcurrentShards,
            boolean failFastOnShardFailure,
            Map<ShardId, Exception> shardLevelFailures,
            ComputeListener computeListener,
            AcquiredSearchContexts searchContexts
        ) {
            this.flags = flags;
            this.request = request;
            this.parentTask = parentTask;
            this.exchangeSink = exchangeSink;
            this.computeListener = computeListener;
            this.maxConcurrentShards = maxConcurrentShards;
            this.failFastOnShardFailure = failFastOnShardFailure;
            this.shardLevelFailures = shardLevelFailures;
            this.blockingSink = exchangeSink.createExchangeSink(() -> {});
            this.searchContexts = searchContexts;
            this.planTimeProfile = new PlanTimeProfile();
            this.perShardSinks = null;
            this.perShardSinkCounter = null;
        }

        DataNodeRequestExecutor(
            EsqlFlags flags,
            DataNodeRequest request,
            CancellableTask parentTask,
            List<ExchangeSinkHandler> perShardSinks,
            AtomicInteger perShardSinkCounter,
            boolean failFastOnShardFailure,
            Map<ShardId, Exception> shardLevelFailures,
            ComputeListener computeListener,
            AcquiredSearchContexts searchContexts
        ) {
            this.flags = flags;
            this.request = request;
            this.parentTask = parentTask;
            this.exchangeSink = null;
            this.blockingSink = null;
            // All shards run concurrently in a single batch so the sorted merge can start immediately.
            this.maxConcurrentShards = request.shards().size();
            this.perShardSinks = perShardSinks;
            this.perShardSinkCounter = perShardSinkCounter;
            this.failFastOnShardFailure = failFastOnShardFailure;
            this.shardLevelFailures = shardLevelFailures;
            this.computeListener = computeListener;
            this.searchContexts = searchContexts;
            this.planTimeProfile = new PlanTimeProfile();
        }

        void start() {
            runBatch(0);
        }

        private void runBatch(int startBatchIndex) {
            final Configuration configuration = request.configuration();
            final String clusterAlias = request.clusterAlias();
            final var sessionId = request.sessionId();
            final int endBatchIndex = Math.min(startBatchIndex + maxConcurrentShards, request.shards().size());
            final AtomicInteger pagesProduced = new AtomicInteger();
            List<DataNodeRequest.Shard> shards = request.shards().subList(startBatchIndex, endBatchIndex);
            ActionListener<DriverCompletionInfo> batchListener = new ActionListener<>() {
                final ActionListener<DriverCompletionInfo> ref = computeListener.acquireCompute();

                @Override
                public void onResponse(DriverCompletionInfo info) {
                    try {
                        onBatchCompleted(endBatchIndex);
                    } finally {
                        ref.onResponse(info);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (pagesProduced.get() == 0 && failFastOnShardFailure == false) {
                        for (DataNodeRequest.Shard shard : shards) {
                            addShardLevelFailure(shard.shardId(), e);
                        }
                        onResponse(DriverCompletionInfo.EMPTY);
                    } else {
                        // TODO: add these to fatal failures so we can continue processing other shards.
                        try {
                            if (perShardSinks != null) {
                                for (int i = 0; i < perShardSinks.size(); i++) {
                                    exchangeService.finishSinkHandler(request.sessionId() + "[s" + i + "]", e);
                                }
                            } else {
                                exchangeService.finishSinkHandler(request.sessionId(), e);
                            }
                        } finally {
                            ref.onFailure(e);
                        }
                    }
                }
            };
            acquireSearchContexts(
                clusterAlias,
                shards,
                configuration,
                request.aliasFilters(),
                ActionListener.wrap(acquiredSearchContexts -> {
                    assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH);
                    if (acquiredSearchContexts.isEmpty()) {
                        batchListener.onResponse(DriverCompletionInfo.EMPTY);
                        return;
                    }
                    final Supplier<ExchangeSink> sinkSupplier = (perShardSinks != null)
                        ? () -> perShardSinks.get(perShardSinkCounter.getAndIncrement()).createExchangeSink(pagesProduced::incrementAndGet)
                        : () -> exchangeSink.createExchangeSink(pagesProduced::incrementAndGet);
                    var computeContext = new ComputeContext(
                        sessionId,
                        ComputeService.DATA_DESCRIPTION,
                        clusterAlias,
                        flags,
                        acquiredSearchContexts,
                        configuration,
                        configuration.newFoldContext(),
                        null,
                        sinkSupplier,
                        null
                    );
                    computeService.runCompute(
                        parentTask,
                        computeContext,
                        request.plan(),
                        computeService.plannerSettings().get(),
                        LocalPhysicalOptimization.ENABLED,
                        planTimeProfile,
                        batchListener
                    );
                }, batchListener::onFailure)
            );
        }

        private void acquireSearchContexts(
            String clusterAlias,
            List<DataNodeRequest.Shard> shards,
            Configuration configuration,
            Map<Index, AliasFilter> aliasFilters,
            ActionListener<IndexedByShardId<ComputeSearchContext>> listener
        ) {
            final List<Tuple<IndexShard, SplitShardCountSummary>> targetShards = new ArrayList<>();
            for (DataNodeRequest.Shard shard : shards) {
                try {
                    var indexShard = searchService.getIndicesService()
                        .indexServiceSafe(shard.shardId().getIndex())
                        .getShard(shard.shardId().id());
                    targetShards.add(new Tuple<>(indexShard, shard.splitShardCountSummary()));
                } catch (Exception e) {
                    if (addShardLevelFailure(shard.shardId(), e) == false) {
                        listener.onFailure(e);
                        return;
                    }
                }
            }
            final var doAcquire = ActionRunnable.supply(listener, () -> {
                var newContexts = new ArrayList<SearchContext>();
                for (Tuple<IndexShard, SplitShardCountSummary> targetShard : targetShards) {
                    SearchContext context = null;
                    IndexShard indexShard = targetShard.v1();
                    try {
                        var aliasFilter = aliasFilters.getOrDefault(indexShard.shardId().getIndex(), AliasFilter.EMPTY);
                        var shardRequest = new ShardSearchRequest(
                            indexShard.shardId(),
                            configuration.absoluteStartedTimeInMillis(),
                            aliasFilter,
                            clusterAlias,
                            targetShard.v2()
                        );
                        // TODO: `searchService.createSearchContext` allows opening search contexts without limits,
                        // we need to limit the number of active search contexts here or in SearchService
                        context = searchService.createSearchContext(shardRequest, SearchService.NO_TIMEOUT);
                        context.preProcess();
                        newContexts.add(context);
                    } catch (RuntimeException e) {
                        IOUtils.close(context);
                        if (addShardLevelFailure(indexShard.shardId(), e) == false) {
                            IOUtils.closeWhileHandlingException(newContexts);
                            throw e;
                        }
                    }
                }
                return searchContexts.newSubRangeView(newContexts);
            });
            final AtomicBoolean waitedForRefreshes = new AtomicBoolean();
            try (RefCountingRunnable refs = new RefCountingRunnable(() -> {
                if (waitedForRefreshes.get()) {
                    searchExecutor.execute(doAcquire);
                } else {
                    doAcquire.run();
                }
            })) {
                for (Tuple<IndexShard, SplitShardCountSummary> targetShard : targetShards) {
                    final Releasable ref = refs.acquire();
                    targetShard.v1().ensureShardSearchActive(await -> {
                        try (ref) {
                            if (await) {
                                waitedForRefreshes.set(true);
                            }
                        }
                    });
                }
            }
        }

        private void onBatchCompleted(int lastBatchIndex) {
            if (perShardSinks != null) {
                // Sorted-merge path: all shards ran in a single batch; per-shard sinks complete
                // naturally when their drivers finish. The ExchangeSourceHandler detects completion
                // via completedSinks tracking — no blockingSink or additional listeners needed here.
                return;
            }
            if (lastBatchIndex < request.shards().size() && exchangeSink.isFinished() == false) {
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

        private boolean addShardLevelFailure(ShardId shardId, Exception e) {
            if (failFastOnShardFailure) {
                return false;
            }
            shardLevelFailures.put(shardId, e);
            return true;
        }

    }

    /**
     * Returns true when the node-level reduction plan contains a sorted-merge TopN — i.e. the plan
     * inserted by {@code MarkUnboundedSort} with a {@code MAX_VALUE} limit
     * that was subsequently marked as {@code SORTED} by {@link ComputeService#reductionPlan}.  In
     * that case each shard must write to its own {@link ExchangeSinkHandler} so that
     * {@link org.elasticsearch.compute.operator.topn.SortedMergeSourceOperator} can K-way merge
     * the already-sorted per-shard streams without buffering all rows in a single
     * {@link org.elasticsearch.compute.operator.topn.TopNQueue}.
     *
     * <p>We search the whole plan tree (not just the direct child of {@code ExchangeSinkExec})
     * because late-materialization can insert a {@code ProjectExec} between the sink and the
     * {@code TopNExec}.
     */
    private static boolean needsSortedMerge(PhysicalPlan nodeReducePlan) {
        if (nodeReducePlan instanceof ExchangeSinkExec == false) {
            return false;
        }
        return nodeReducePlan.collectFirstChildren(
            p -> p instanceof TopNExec topN
                && topN.inputOrdering() == TopNOperator.InputOrdering.SORTED
                && topN.unboundedSort()
                && topN.child() instanceof ExchangeSourceExec
        ).isEmpty() == false;
    }

    private void runComputeOnDataNode(
        CancellableTask task,
        String externalId,
        PhysicalPlan reducePlan,
        DataNodeRequest request,
        boolean failFastOnShardFailure,
        AcquiredSearchContexts searchContexts,
        PlannerSettings plannerSettings,
        PlanTimeProfile planTimeProfile,
        ActionListener<DataNodeComputeResponse> listener
    ) {
        final Map<ShardId, Exception> shardLevelFailures = new HashMap<>();
        try (
            ComputeListener computeListener = new ComputeListener(
                transportService.getThreadPool(),
                computeService.cancelQueryOnFailure(task),
                listener.map(profiles -> new DataNodeComputeResponse(profiles, shardLevelFailures))
            )
        ) {
            final boolean isSortedMerge = needsSortedMerge(reducePlan);
            var parentListener = computeListener.acquireAvoid();
            try {
                // run compute with target shards
                var externalSink = exchangeService.getSinkHandler(externalId);
                EsqlFlags flags = computeService.createFlags();

                final ExchangeSourceHandler exchangeSource;
                final DataNodeRequestExecutor dataNodeRequestExecutor;

                if (isSortedMerge) {
                    // Sorted merge path: each shard gets its own ExchangeSinkHandler so that
                    // SortedMergeSourceOperator can K-way merge per-shard sorted streams on the
                    // data node without buffering all data in a single TopNQueue(MAX_VALUE).
                    int numShards = request.shards().size();
                    List<ExchangeSinkHandler> perShardSinks = new ArrayList<>(numShards);
                    for (int i = 0; i < numShards; i++) {
                        perShardSinks.add(
                            exchangeService.createSinkHandler(request.sessionId() + "[s" + i + "]", request.pragmas().exchangeBufferSize())
                        );
                    }
                    task.addListener(() -> {
                        exchangeService.finishSinkHandler(externalId, new TaskCancelledException(task.getReasonCancelled()));
                        for (int i = 0; i < numShards; i++) {
                            exchangeService.finishSinkHandler(
                                request.sessionId() + "[s" + i + "]",
                                new TaskCancelledException(task.getReasonCancelled())
                            );
                        }
                    });
                    exchangeSource = new ExchangeSourceHandler(numShards, searchExecutor);
                    for (int i = 0; i < numShards; i++) {
                        final String shardSinkId = request.sessionId() + "[s" + i + "]";
                        final ExchangeSinkHandler shardSink = perShardSinks.get(i);
                        exchangeSource.addRemoteSink(
                            shardSink::fetchPageAsync,
                            true,
                            () -> {},
                            1,
                            ActionListener.running(() -> exchangeService.finishSinkHandler(shardSinkId, null))
                        );
                    }
                    AtomicInteger sinkCounter = new AtomicInteger(0);
                    dataNodeRequestExecutor = new DataNodeRequestExecutor(
                        flags,
                        request,
                        task,
                        perShardSinks,
                        sinkCounter,
                        failFastOnShardFailure,
                        shardLevelFailures,
                        computeListener,
                        searchContexts
                    );
                } else {
                    // Original single-sink path
                    var internalSink = exchangeService.createSinkHandler(request.sessionId(), request.pragmas().exchangeBufferSize());
                    task.addListener(() -> {
                        exchangeService.finishSinkHandler(externalId, new TaskCancelledException(task.getReasonCancelled()));
                        exchangeService.finishSinkHandler(request.sessionId(), new TaskCancelledException(task.getReasonCancelled()));
                    });
                    int maxConcurrentShards = request.pragmas().maxConcurrentShardsPerNode();
                    exchangeSource = new ExchangeSourceHandler(1, searchExecutor);
                    exchangeSource.addRemoteSink(internalSink::fetchPageAsync, true, () -> {}, 1, ActionListener.noop());
                    dataNodeRequestExecutor = new DataNodeRequestExecutor(
                        flags,
                        request,
                        task,
                        internalSink,
                        maxConcurrentShards,
                        failFastOnShardFailure,
                        shardLevelFailures,
                        computeListener,
                        searchContexts
                    );
                }

                dataNodeRequestExecutor.start();
                // run the node-level reduction
                var reductionListener = computeListener.acquireCompute();
                ExchangeSourceHandler exchangeSourceForContext = isSortedMerge ? exchangeSource : null;
                computeService.runCompute(
                    task,
                    new ComputeContext(
                        request.sessionId(),
                        ComputeService.REDUCE_DESCRIPTION,
                        request.clusterAlias(),
                        flags,
                        searchContexts.globalView(),
                        request.configuration(),
                        new FoldContext(request.pragmas().foldLimit().getBytes()),
                        exchangeSource::createExchangeSource,
                        () -> externalSink.createExchangeSink(() -> {}),
                        exchangeSourceForContext
                    ),
                    reducePlan,
                    plannerSettings,
                    // Local physical optimization is aimed at data nodes. For node-reduce-level reduction we precompute the final physical
                    // plan and pass it in reducePlan. We don't need any additional optimizations.
                    LocalPhysicalOptimization.DISABLED,
                    planTimeProfile,
                    ActionListener.wrap(resp -> {
                        // don't return until all pages are fetched
                        externalSink.addCompletionListener(ActionListener.running(() -> {
                            exchangeService.finishSinkHandler(externalId, null);
                            reductionListener.onResponse(resp);
                        }));
                    }, e -> {
                        LOGGER.debug("Error in node-level reduction", e);
                        exchangeService.finishSinkHandler(externalId, e);
                        reductionListener.onFailure(e);
                    })
                );
                parentListener.onResponse(null);
            } catch (Exception e) {
                exchangeService.finishSinkHandler(externalId, e);
                // best-effort cleanup; handlers may not exist if setup failed before creation
                int numShards = needsSortedMerge(reducePlan) ? request.shards().size() : 0;
                if (numShards > 0) {
                    for (int i = 0; i < numShards; i++) {
                        exchangeService.finishSinkHandler(request.sessionId() + "[s" + i + "]", e);
                    }
                } else {
                    exchangeService.finishSinkHandler(request.sessionId(), e);
                }
                parentListener.onFailure(e);
            }
        }
    }

    @Override
    public void messageReceived(DataNodeRequest request, TransportChannel channel, Task task) {
        ActionListener<DataNodeComputeResponse> listener = new ChannelActionListener<>(channel);
        Configuration configuration = request.configuration();
        PlanTimeProfile planTimeProfile = null;
        if (configuration.profile()) {
            planTimeProfile = new PlanTimeProfile();
        }

        if (request.externalSplits().isEmpty() == false && request.shards().isEmpty()) {
            handleExternalSourceRequest(request, (CancellableTask) task, listener, planTimeProfile);
            return;
        }

        ReductionPlan reductionPlan;
        if (request.plan() instanceof ExchangeSinkExec plan) {
            reductionPlan = ComputeService.reductionPlan(
                computeService.plannerSettings().get(),
                computeService.createFlags(),
                configuration,
                configuration.newFoldContext(),
                plan,
                request.runNodeLevelReduction(),
                request.reductionLateMaterialization(),
                planTimeProfile
            );
        } else {
            listener.onFailure(new IllegalStateException("expected exchange sink for a remote compute; got " + request.plan()));
            return;
        }
        final String sessionId = request.sessionId();
        request = new DataNodeRequest(
            sessionId + "[n]", // internal session
            request.configuration(),
            request.clusterAlias(),
            request.shards(),
            request.aliasFilters(),
            request.plan(),
            request.indices(),
            request.indicesOptions(),
            request.runNodeLevelReduction(),
            request.reductionLateMaterialization(),
            request.retainSearchContexts(),
            request.externalSplits()
        );
        // the sender doesn't support retry on shard failures, so we need to fail fast here.
        final boolean failFastOnShardFailures = supportShardLevelRetryFailure(channel.getVersion()) == false;
        var computeSearchContexts = new AcquiredSearchContexts(request.shards().size());
        runComputeOnDataNode(
            (CancellableTask) task,
            sessionId,
            reductionPlan.nodeReducePlan(),
            request.withPlan(reductionPlan.dataNodePlan()),
            failFastOnShardFailures,
            computeSearchContexts,
            computeService.plannerSettings().get(),
            planTimeProfile,
            ActionListener.releaseAfter(listener, computeSearchContexts)
        );
    }

    private void handleExternalSourceRequest(
        DataNodeRequest request,
        CancellableTask task,
        ActionListener<DataNodeComputeResponse> listener,
        PlanTimeProfile planTimeProfile
    ) {
        if (request.plan() instanceof ExchangeSinkExec == false) {
            listener.onFailure(new IllegalStateException("expected exchange sink for external compute; got " + request.plan()));
            return;
        }
        ExchangeSinkExec sinkExec = (ExchangeSinkExec) request.plan();
        Configuration configuration = request.configuration();
        final String sessionId = request.sessionId();
        EsqlFlags flags = computeService.createFlags();
        PlannerSettings plannerSettings = computeService.plannerSettings().get();

        // Run localPlan() to expand FragmentExec(ExternalRelation) -> ExternalSourceExec
        // This runs LocalLogicalPlanOptimizer, LocalMapper, and LocalPhysicalPlanOptimizer
        // (including filter pushdown via FormatReader.filterPushdownSupport())
        // Splits are injected before physical optimization so rules like PushAggregatesToExternalSource see them.
        PhysicalPlan planWithSplits = PlannerUtils.localPlan(
            plannerSettings,
            flags,
            configuration,
            configuration.newFoldContext(),
            sinkExec,
            SearchStats.EMPTY,
            computeService.formatReaderRegistry(),
            request.externalSplits(),
            planTimeProfile
        );

        try (
            ComputeListener computeListener = new ComputeListener(
                threadPool,
                computeService.cancelQueryOnFailure(task),
                listener.map(profiles -> new DataNodeComputeResponse(profiles, Map.of()))
            )
        ) {
            var parentListener = computeListener.acquireAvoid();
            final ActionListener<DriverCompletionInfo> driverCompletionListener = ActionListener.notifyOnce(
                computeListener.acquireCompute()
            );
            try {
                var externalSink = exchangeService.getSinkHandler(sessionId);
                String internalSessionId = sessionId + "[n]";
                task.addListener(
                    () -> { exchangeService.finishSinkHandler(sessionId, new TaskCancelledException(task.getReasonCancelled())); }
                );

                var computeContext = new ComputeContext(
                    internalSessionId,
                    ComputeService.DATA_DESCRIPTION,
                    request.clusterAlias(),
                    flags,
                    EmptyIndexedByShardId.instance(),
                    configuration,
                    configuration.newFoldContext(),
                    null,
                    () -> externalSink.createExchangeSink(() -> {}),
                    null
                );
                computeService.runCompute(
                    task,
                    computeContext,
                    planWithSplits,
                    plannerSettings,
                    LocalPhysicalOptimization.DISABLED,
                    planTimeProfile,
                    ActionListener.wrap(resp -> {
                        externalSink.addCompletionListener(ActionListener.running(() -> {
                            exchangeService.finishSinkHandler(sessionId, null);
                            driverCompletionListener.onResponse(resp);
                        }));
                    }, e -> {
                        LOGGER.warn(
                            "external source compute failed on data node [{}] with {} splits, session [{}]",
                            transportService.getLocalNode().getName(),
                            request.externalSplits().size(),
                            sessionId,
                            e
                        );
                        exchangeService.finishSinkHandler(sessionId, e);
                        driverCompletionListener.onFailure(e);
                    })
                );
                parentListener.onResponse(null);
            } catch (Exception e) {
                LOGGER.warn(
                    "failed to start external source compute on data node [{}], session [{}]",
                    transportService.getLocalNode().getName(),
                    sessionId,
                    e
                );
                exchangeService.finishSinkHandler(sessionId, e);
                driverCompletionListener.onFailure(e);
                parentListener.onFailure(e);
            }
        }
    }

    static boolean supportShardLevelRetryFailure(TransportVersion transportVersion) {
        return transportVersion.supports(ESQL_RETRY_ON_SHARD_LEVEL_FAILURE);
    }
}
