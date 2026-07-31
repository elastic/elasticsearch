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
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
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
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
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
    private final ShardResultCache shardResultCache;
    /**
     * Resolved once: {@link Federation#FEDERATION_ENABLED} is node scoped and takes effect only after a restart, so
     * every external request on this node gets the same answer.
     */
    private final boolean federationAvailable;

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
        this.shardResultCache = new ShardResultCache(
            searchService.getIndicesService(),
            clusterService.getClusterSettings(),
            computeService.blockFactory()
        );
        this.federationAvailable = Federation.isAvailable(clusterService.getSettings());
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
                                // TODO: gate on EsqlCapabilities.Cap.REMOTE_FETCH plus request/connection transport versions
                                // when coordinator planning starts requesting retained contexts.
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
        private final ShardResultCacheSettings cacheSettings;
        /**
         * The shared part of the cache key, or {@code null} when this request may not use the cache at all. Non-null
         * implies {@link #maxConcurrentShards} is 1, which is what lets a batch's captured pages be attributed to one
         * shard.
         */
        @Nullable
        private final ShardResultCacheKey.QueryPart cacheQueryPart;

        DataNodeRequestExecutor(
            EsqlFlags flags,
            DataNodeRequest request,
            CancellableTask parentTask,
            ExchangeSinkHandler exchangeSink,
            int maxConcurrentShards,
            boolean failFastOnShardFailure,
            Map<ShardId, Exception> shardLevelFailures,
            ComputeListener computeListener,
            AcquiredSearchContexts searchContexts,
            ShardResultCacheSettings cacheSettings,
            @Nullable ShardResultCacheKey.QueryPart cacheQueryPart
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
            this.cacheSettings = cacheSettings;
            this.cacheQueryPart = cacheQueryPart;
            if (cacheQueryPart != null && maxConcurrentShards != 1) {
                throw new IllegalStateException(
                    "a cacheable request must run one shard per batch, but maxConcurrentShards=" + maxConcurrentShards
                );
            }
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
                    assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH);
                    if (acquiredSearchContexts.isEmpty()) {
                        batchListener.onResponse(DriverCompletionInfo.EMPTY);
                        return;
                    }
                    RetainedCacheProbe probe = probeCache(shards, acquiredSearchContexts);
                    if (probe != null && probe.probe().isHit()) {
                        if (serveFromCache(probe, pagesProduced, batchListener)) {
                            return;
                        }
                        // The entry could not be replayed, so this shard has to be computed after all, and there is no
                        // point capturing a value under a key that just failed to round-trip.
                        probe = null;
                    }
                    final ShardResultCapture capture = probe == null ? null : new ShardResultCapture(cacheSettings.maxValueSizeInBytes());
                    var computeContext = new ComputeContext(
                        sessionId,
                        ComputeService.DATA_DESCRIPTION,
                        clusterAlias,
                        flags,
                        acquiredSearchContexts,
                        configuration,
                        configuration.newFoldContext(),
                        null,
                        capture == null
                            ? () -> exchangeSink.createExchangeSink(pagesProduced::incrementAndGet)
                            : () -> capture.wrap(exchangeSink.createExchangeSink(pagesProduced::incrementAndGet)),
                        request.retainSearchContexts()
                    );
                    computeService.runCompute(
                        parentTask,
                        computeContext,
                        request.plan(),
                        computeService.plannerSettings().get(),
                        LocalPhysicalOptimization.ENABLED,
                        planTimeProfile,
                        capture == null ? batchListener : storeThen(probe, capture, batchListener)
                    );
                }, batchListener::onFailure)
            );
        }

        /**
         * A probe plus the searcher that keeps it storable. A shard's {@link SearchContext} is closed as soon as
         * the last operator holding its shard context releases it, which happens before this batch's completion listener
         * runs, so holding an {@link Engine.Searcher} is what guarantees the reader stays alive — and therefore
         * addressable by cache key — when the store finally happens. The compute context is carried for the same reason:
         * only it can say, once the drivers are done, whether building this shard's queries turned out to be cacheable.
         */
        private record RetainedCacheProbe(ShardResultCache.ShardProbe probe, Engine.Searcher searcher, ComputeSearchContext context)
            implements
                Releasable {
            @Override
            public void close() {
                searcher.close();
            }
        }

        /**
         * Probes the cache for this batch's single shard.
         *
         * @return {@code null} when this shard cannot use the cache, either because the request is not cacheable or
         *         because its key could not be completed
         */
        @Nullable
        private RetainedCacheProbe probeCache(
            List<DataNodeRequest.Shard> shards,
            IndexedByShardId<ComputeSearchContext> acquiredSearchContexts
        ) {
            if (cacheQueryPart == null) {
                return null;
            }
            assert shards.size() == 1 : "expected a single shard per batch, got " + shards.size();
            ComputeSearchContext context = acquiredSearchContexts.iterable().iterator().next();
            SearchContext searchContext = context.searchContext();
            /*
             * The same gate the DSL path applies, here catching whatever preProcess already consulted. It is only half
             * the gate: ES|QL builds this shard's queries later and against its own context, so the store path asks
             * that one too, in storeThen.
             */
            if (searchContext.getSearchExecutionContext().isCacheable() == false) {
                return null;
            }
            ShardResultCache.ShardProbe probe = shardResultCache.probe(cacheQueryPart, shards.getFirst(), searchContext);
            if (probe == null) {
                return null;
            }
            if (probe.isHit() == false && shardResultCache.admits(probe.shard(), cacheSettings) == false) {
                // A miss on a shard the admission policy would refuse: the miss is worth counting, the capture is not
                // worth paying for.
                return null;
            }
            return new RetainedCacheProbe(probe, probe.shard().acquireSearcher("shard-result-cache"), context);
        }

        /**
         * Replays a hit into the exchange in place of running any driver, and completes the batch with no driver
         * profiles: there were none, and reporting borrowed ones would misattribute another query's work.
         *
         * @return false when the entry could not be replayed, in which case nothing was written to the exchange and the
         *         caller must compute the shard normally
         */
        private boolean serveFromCache(
            RetainedCacheProbe probe,
            AtomicInteger pagesProduced,
            ActionListener<DriverCompletionInfo> batchListener
        ) {
            final List<Page> pages;
            try (probe) {
                // Deserializes everything before writing anything, so a failure here leaves the exchange untouched.
                pages = shardResultCache.replay(probe.probe().hit());
            } catch (Exception e) {
                LOGGER.warn("failed to replay a shard result cache entry; recomputing the shard", e);
                return false;
            }
            ExchangeSink sink = exchangeSink.createExchangeSink(pagesProduced::incrementAndGet);
            int handedOver = 0;
            try {
                for (Page page : pages) {
                    // The sink owns a page once it has been handed over, and releases it itself if the exchange is
                    // already finished. Anything not handed over is still ours.
                    sink.addPage(page);
                    handedOver++;
                }
            } finally {
                for (int i = handedOver; i < pages.size(); i++) {
                    pages.get(i).releaseBlocks();
                }
                sink.finish();
                /*
                 * A computed shard's search context is closed by the last operator to release its shard context. This
                 * shard has no operators, so closing it here is what keeps a run of hits from holding one context per
                 * shard open until the whole request ends. It is the same moment a computed batch would have closed
                 * it, so the node-reduce driver, which sees every context, is no worse off than on a miss.
                 */
                probe.context().close();
            }
            batchListener.onResponse(DriverCompletionInfo.EMPTY);
            return true;
        }

        /**
         * Wraps the batch listener so a batch that completed normally is stored. Failures skip the store: a partial
         * capture is indistinguishable from a complete one once it is bytes.
         */
        private ActionListener<DriverCompletionInfo> storeThen(
            RetainedCacheProbe probe,
            ShardResultCapture capture,
            ActionListener<DriverCompletionInfo> batchListener
        ) {
            return ActionListener.runAfter(batchListener.delegateFailureAndWrap((l, info) -> {
                /*
                 * The cacheability question can only be answered here. Building this shard's queries is what discovers
                 * a non-deterministic runtime field, and that happens on a context ES|QL made for the drivers, after
                 * the probe.
                 * Driver failures and cancellations route to onFailure, not here, so reaching this success branch means
                 * the computation completed normally and the capture is complete.
                 */
                if (probe.context().queryConstructionWasCacheable() == false) {
                    LOGGER.debug("not storing a shard result: building the shard's queries was not cacheable");
                } else if (shardLevelFailures.containsKey(probe.probe().shard().shardId())) {
                    /*
                     * A request that allows partial results turns a shard failure into a recorded failure and a
                     * successful batch, so a batch can complete having produced fewer rows than the shard holds.
                     */
                    LOGGER.debug("not storing a shard result: the shard failed");
                } else {
                    BytesReference value = capture.value();
                    if (value != null) {
                        shardResultCache.store(probe.probe(), value);
                    }
                }
                l.onResponse(info);
            }), probe::close);
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
                ShardResultCacheSettings cacheSettings = shardResultCache.settings();
                ShardResultCacheKey.QueryPart cacheQueryPart = cacheSettings.enabled() ? shardResultCache.queryPart(request, flags) : null;
                /*
                 * A batch's captured pages carry no shard attribution, so a cacheable request runs its shards one batch at
                 * a time to make the attribution unambiguous. This gives up the work stealing that lets one slow shard be
                 * helped by the drivers of another, which is why it applies only to the shapes the cache admits, where a
                 * shard's work is a scan and a hash rather than something a peer could usefully take over.
                 */
                int maxConcurrentShards = cacheQueryPart == null ? request.pragmas().maxConcurrentShardsPerNode() : 1;
                DataNodeRequestExecutor dataNodeRequestExecutor = new DataNodeRequestExecutor(
                    flags,
                    request,
                    task,
                    internalSink,
                    maxConcurrentShards,
                    failFastOnShardFailure,
                    shardLevelFailures,
                    computeListener,
                    searchContexts,
                    cacheSettings,
                    cacheQueryPart
                );
                dataNodeRequestExecutor.start();
                // run the node-level reduction
                var exchangeSource = new ExchangeSourceHandler(1, searchExecutor);
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
                        () -> externalSink.createExchangeSink(() -> {}),
                        request.retainSearchContexts()
                    ),
                    reducePlan,
                    plannerSettings,
                    // Local physical optimization is aimed at data nodes. For node-reduce-level reduction we precompute the final physical
                    // plan and pass it in reducePlan. We don't need any additional optimizations.
                    LocalPhysicalOptimization.DISABLED,
                    planTimeProfile,
                    ActionListener.wrap(resp -> {
                        // don't return until all pages are fetched; preserve the current thread context (which holds the
                        // reduction driver's warnings) because the completion listener may fire on a different thread
                        // (the transport thread that processes the coordinator's fetchPageAsync call)
                        externalSink.addCompletionListener(
                            ContextPreservingActionListener.wrapPreservingContext(ActionListener.running(() -> {
                                exchangeService.finishSinkHandler(externalId, null);
                                reductionListener.onResponse(resp);
                            }), threadPool.getThreadContext())
                        );
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
        final String nodeReduceSessionId = sessionId + "[n]";
        request = new DataNodeRequest(
            nodeReduceSessionId, // internal session
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
        ActionListener<DataNodeComputeResponse> responseListener;
        if (request.retainSearchContexts()) {
            final RetainedSearchContextsRegistry.Handle retainedSearchContexts;
            try {
                retainedSearchContexts = computeService.remoteFetchService()
                    .retainSearchContexts(nodeReduceSessionId, computeSearchContexts);
            } catch (Exception e) {
                computeSearchContexts.close();
                listener.onFailure(e);
                return;
            }
            /*
             * The compute holds its own lease on the retained contexts while its drivers run. Cancellation closes the
             * registration asynchronously, which rejects new fetch leases immediately, but must not release the
             * contexts out from under still-running drivers; that only happens once this lease is closed in the
             * response listener, after the compute has completed.
             */
            final RetainedSearchContextsRegistry.Handle computeLease;
            try {
                computeLease = computeService.remoteFetchService().acquireRetainedContexts(nodeReduceSessionId);
            } catch (Exception e) {
                retainedSearchContexts.close();
                listener.onFailure(e);
                return;
            }
            ((CancellableTask) task).addListener(retainedSearchContexts::close);
            responseListener = ActionListener.wrap(response -> {
                boolean success = false;
                try {
                    retainedSearchContexts.finishRegistration();
                    listener.onResponse(response);
                    success = true;
                } finally {
                    computeLease.close();
                    if (success == false) {
                        retainedSearchContexts.close();
                    }
                }
            }, e -> {
                try (retainedSearchContexts; computeLease) {
                    listener.onFailure(e);
                }
            });
        } else {
            responseListener = ActionListener.releaseAfter(listener, computeSearchContexts);
        }
        runComputeOnDataNode(
            (CancellableTask) task,
            sessionId,
            reductionPlan.nodeReducePlan(),
            request.withPlan(reductionPlan.dataNodePlan()),
            failFastOnShardFailures,
            computeSearchContexts,
            computeService.plannerSettings().get(),
            planTimeProfile,
            responseListener
        );
    }

    /**
     * Rejects an external request that never got as far as building a driver. The coordinator opened the exchange before
     * sending the request, so a sink handler is already registered here; it has to be failed explicitly or the
     * coordinator's exchange source only learns of the refusal through task cancellation and this node holds an
     * unfinished sink until the inactive-sinks reaper runs.
     */
    private void failWithoutStarting(DataNodeRequest request, ActionListener<DataNodeComputeResponse> listener, Exception failure) {
        exchangeService.finishSinkHandler(request.sessionId(), failure);
        listener.onFailure(failure);
    }

    private void handleExternalSourceRequest(
        DataNodeRequest request,
        CancellableTask task,
        ActionListener<DataNodeComputeResponse> listener,
        PlanTimeProfile planTimeProfile
    ) {
        // Federation gate for the whole external request, not just the operators it ends up building. The backstop in
        // LocalExecutionPlanner.planExternalSource only fires for a plan that still contains an ExternalSourceExec, and
        // localPlan() below can consume it: PushStatsToExternalSource answers an ungrouped COUNT/MIN/MAX from the split
        // stats the coordinator discovered and leaves a LocalSourceExec behind. Refusing on entry means a node without
        // federation serves no external data whatever the aggregate shape.
        if (federationAvailable == false) {
            failWithoutStarting(request, listener, Federation.notAvailableException());
            return;
        }
        if (request.plan() instanceof ExchangeSinkExec == false) {
            failWithoutStarting(
                request,
                listener,
                new IllegalStateException("expected exchange sink for external compute; got " + request.plan())
            );
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
        // Splits are injected before physical optimization so rules like PushStatsToExternalSource see them.
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
                    false
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
