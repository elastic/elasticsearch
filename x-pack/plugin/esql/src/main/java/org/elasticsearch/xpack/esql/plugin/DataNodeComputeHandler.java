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
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
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
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlanConcurrencyCalculator;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;

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

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME;

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
    private final Executor esqlExecutor;
    private final ThreadPool threadPool;

    DataNodeComputeHandler(
        ComputeService computeService,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        SearchService searchService,
        TransportService transportService,
        ExchangeService exchangeService,
        Executor esqlExecutor
    ) {
        this.computeService = computeService;
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.searchService = searchService;
        this.transportService = transportService;
        this.exchangeService = exchangeService;
        this.esqlExecutor = esqlExecutor;
        this.threadPool = transportService.getThreadPool();
        transportService.registerRequestHandler(ComputeService.DATA_ACTION_NAME, esqlExecutor, DataNodeRequest::new, this);
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
            esqlExecutor,
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
                    esqlExecutor,
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
                                queryPragmas.nodeLevelReduction() && enableReduceNodeLateMaterialization
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
                                }), DataNodeComputeResponse::new, esqlExecutor)
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

    private static final Logger LOGGER = LogManager.getLogger(DataNodeComputeHandler.class);

    private class DataNodeRequestExecutor {
        private final EsqlFlags flags;
        private final DataNodeRequest request;
        private final CancellableTask parentTask;
        private final ExchangeSinkHandler exchangeSink;
        private final ComputeListener computeListener;
        private final int maxConcurrentShards;
        private final ExchangeSink blockingSink; // block until we have completed on all shards or the coordinator has enough data
        private final boolean singleShardPipeline;
        private final boolean failFastOnShardFailure;
        private final Map<ShardId, Exception> shardLevelFailures;
        private final AcquiredSearchContexts searchContexts;
        private final PlanTimeProfile planTimeProfile;

        DataNodeRequestExecutor(
            EsqlFlags flags,
            DataNodeRequest request,
            CancellableTask parentTask,
            ExchangeSinkHandler exchangeSink,
            int maxConcurrentShards,
            boolean failFastOnShardFailure,
            Map<ShardId, Exception> shardLevelFailures,
            boolean singleShardPipeline,
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
            this.singleShardPipeline = singleShardPipeline;
            this.blockingSink = exchangeSink.createExchangeSink(() -> {});
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
                            exchangeService.finishSinkHandler(request.sessionId(), e);
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
                    assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH, ESQL_WORKER_THREAD_POOL_NAME);
                    if (acquiredSearchContexts.isEmpty()) {
                        batchListener.onResponse(DriverCompletionInfo.EMPTY);
                        return;
                    }
                    if (singleShardPipeline) {
                        try (ComputeListener sub = new ComputeListener(threadPool, () -> {}, batchListener)) {
                            for (ComputeSearchContext searchContext : acquiredSearchContexts.iterable()) {
                                var computeContext = new ComputeContext(
                                    sessionId,
                                    "data",
                                    clusterAlias,
                                    flags,
                                    new IndexedByShardIdFromSingleton<>(searchContext, searchContext.index()),
                                    configuration,
                                    configuration.newFoldContext(),
                                    null,
                                    () -> exchangeSink.createExchangeSink(pagesProduced::incrementAndGet)
                                );
                                computeService.runCompute(
                                    parentTask,
                                    computeContext,
                                    request.plan(),
                                    computeService.plannerSettings().get(),
                                    planTimeProfile,
                                    sub.acquireCompute()
                                );
                            }
                        }
                    } else {
                        var computeContext = new ComputeContext(
                            sessionId,
                            ComputeService.DATA_DESCRIPTION,
                            clusterAlias,
                            flags,
                            acquiredSearchContexts,
                            configuration,
                            configuration.newFoldContext(),
                            null,
                            () -> exchangeSink.createExchangeSink(pagesProduced::incrementAndGet)
                        );
                        computeService.runCompute(
                            parentTask,
                            computeContext,
                            request.plan(),
                            computeService.plannerSettings().get(),
                            planTimeProfile,
                            batchListener
                        );
                    }
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
                    targetShards.add(new Tuple<>(indexShard, shard.reshardSplitShardCountSummary()));
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
                    esqlExecutor.execute(doAcquire);
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
            var parentListener = computeListener.acquireAvoid();
            try {
                // run compute with target shards
                var externalSink = exchangeService.getSinkHandler(externalId);
                var internalSink = exchangeService.createSinkHandler(request.sessionId(), request.pragmas().exchangeBufferSize());
                task.addListener(() -> {
                    exchangeService.finishSinkHandler(externalId, new TaskCancelledException(task.getReasonCancelled()));
                    exchangeService.finishSinkHandler(request.sessionId(), new TaskCancelledException(task.getReasonCancelled()));
                });
                EsqlFlags flags = computeService.createFlags();
                int maxConcurrentShards = request.pragmas().maxConcurrentShardsPerNode();
                final boolean sortedTimeSeriesSource = PlannerUtils.requiresSortedTimeSeriesSource(request.plan());
                if (sortedTimeSeriesSource) {
                    // each time-series pipeline uses 3 drivers
                    maxConcurrentShards = Math.clamp(Math.ceilDiv(request.pragmas().taskConcurrency(), 3), 1, maxConcurrentShards);
                }
                DataNodeRequestExecutor dataNodeRequestExecutor = new DataNodeRequestExecutor(
                    flags,
                    request,
                    task,
                    internalSink,
                    maxConcurrentShards,
                    failFastOnShardFailure,
                    shardLevelFailures,
                    sortedTimeSeriesSource,
                    computeListener,
                    searchContexts
                );
                dataNodeRequestExecutor.start();
                // run the node-level reduction
                var exchangeSource = new ExchangeSourceHandler(1, esqlExecutor);
                exchangeSource.addRemoteSink(internalSink::fetchPageAsync, true, () -> {}, 1, ActionListener.noop());
                var reductionListener = computeListener.acquireCompute();
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
                        () -> externalSink.createExchangeSink(() -> {})
                    ),
                    reducePlan,
                    plannerSettings,
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
                exchangeService.finishSinkHandler(request.sessionId(), e);
                parentListener.onFailure(e);
            }
        }
    }

    @Override
    public void messageReceived(DataNodeRequest request, TransportChannel channel, Task task) {
        ActionListener<DataNodeComputeResponse> listener = new ChannelActionListener<>(channel);
        ReductionPlan reductionPlan;
        Configuration configuration = request.configuration();
        // We can avoid synchronization (for the most part) since the array elements are never modified, and the array is only added to,
        // with its size being known before we start the computation.
        PlanTimeProfile planTimeProfile = null;
        if (configuration.profile()) {
            planTimeProfile = new PlanTimeProfile();
        }
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
            request.reductionLateMaterialization()
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

    static boolean supportShardLevelRetryFailure(TransportVersion transportVersion) {
        return transportVersion.supports(ESQL_RETRY_ON_SHARD_LEVEL_FAILURE);
    }
}
