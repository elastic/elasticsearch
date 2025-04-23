/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
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
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlanConcurrencyCalculator;
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
            PlannerUtils.canMatchFilter(dataNodePlan),
            clusterAlias,
            configuration.allowPartialResults(),
            maxConcurrentNodesPerCluster == null ? -1 : maxConcurrentNodesPerCluster,
            configuration.pragmas().unavailableShardResolutionAttempts()
        ) {
            @Override
            protected void sendRequest(
                DiscoveryNode node,
                List<ShardId> shardIds,
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
                                    () -> "compute group: data-node [" + node.getName() + "], " + shardIds + " [" + shardIds + "]"
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
                            final var remoteSink = exchangeService.newRemoteSink(groupTask, childSessionId, transportService, connection);
                            exchangeSource.addRemoteSink(
                                remoteSink,
                                configuration.allowPartialResults() == false,
                                pagesFetched::incrementAndGet,
                                queryPragmas.concurrentExchangeClients(),
                                computeListener.acquireAvoid()
                            );
                            final boolean sameNode = transportService.getLocalNode().getId().equals(connection.getNode().getId());
                            var dataNodeRequest = new DataNodeRequest(
                                childSessionId,
                                configuration,
                                clusterAlias,
                                shardIds,
                                aliasFilters,
                                dataNodePlan,
                                originalIndices.indices(),
                                originalIndices.indicesOptions(),
                                sameNode == false && queryPragmas.nodeLevelReduction()
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

    private class DataNodeRequestExecutor {
        private final DataNodeRequest request;
        private final CancellableTask parentTask;
        private final ExchangeSinkHandler exchangeSink;
        private final ComputeListener computeListener;
        private final int maxConcurrentShards;
        private final ExchangeSink blockingSink; // block until we have completed on all shards or the coordinator has enough data
        private final boolean failFastOnShardFailure;
        private final Map<ShardId, Exception> shardLevelFailures;

        DataNodeRequestExecutor(
            DataNodeRequest request,
            CancellableTask parentTask,
            ExchangeSinkHandler exchangeSink,
            int maxConcurrentShards,
            boolean failFastOnShardFailure,
            Map<ShardId, Exception> shardLevelFailures,
            ComputeListener computeListener
        ) {
            this.request = request;
            this.parentTask = parentTask;
            this.exchangeSink = exchangeSink;
            this.computeListener = computeListener;
            this.maxConcurrentShards = maxConcurrentShards;
            this.failFastOnShardFailure = failFastOnShardFailure;
            this.shardLevelFailures = shardLevelFailures;
            this.blockingSink = exchangeSink.createExchangeSink(() -> {});
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
            final AtomicInteger pagesProduced = new AtomicInteger();
            List<ShardId> shardIds = request.shardIds().subList(startBatchIndex, endBatchIndex);
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
                        for (ShardId shardId : shardIds) {
                            addShardLevelFailure(shardId, e);
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
            acquireSearchContexts(clusterAlias, shardIds, configuration, request.aliasFilters(), ActionListener.wrap(searchContexts -> {
                assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH, ESQL_WORKER_THREAD_POOL_NAME);
                if (searchContexts.isEmpty()) {
                    batchListener.onResponse(DriverCompletionInfo.EMPTY);
                    return;
                }
                var computeContext = new ComputeContext(
                    sessionId,
                    "data",
                    clusterAlias,
                    searchContexts,
                    configuration,
                    configuration.newFoldContext(),
                    null,
                    () -> exchangeSink.createExchangeSink(pagesProduced::incrementAndGet)
                );
                computeService.runCompute(parentTask, computeContext, request.plan(), batchListener);
            }, batchListener::onFailure));
        }

        private void acquireSearchContexts(
            String clusterAlias,
            List<ShardId> shardIds,
            Configuration configuration,
            Map<Index, AliasFilter> aliasFilters,
            ActionListener<List<SearchContext>> listener
        ) {
            final List<IndexShard> targetShards = new ArrayList<>();
            for (ShardId shardId : shardIds) {
                try {
                    var indexShard = searchService.getIndicesService().indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
                    targetShards.add(indexShard);
                } catch (Exception e) {
                    if (addShardLevelFailure(shardId, e) == false) {
                        listener.onFailure(e);
                        return;
                    }
                }
            }
            final var doAcquire = ActionRunnable.supply(listener, () -> {
                final List<SearchContext> searchContexts = new ArrayList<>(targetShards.size());
                SearchContext context = null;
                for (IndexShard shard : targetShards) {
                    try {
                        var aliasFilter = aliasFilters.getOrDefault(shard.shardId().getIndex(), AliasFilter.EMPTY);
                        var shardRequest = new ShardSearchRequest(
                            shard.shardId(),
                            configuration.absoluteStartedTimeInMillis(),
                            aliasFilter,
                            clusterAlias
                        );
                        // TODO: `searchService.createSearchContext` allows opening search contexts without limits,
                        // we need to limit the number of active search contexts here or in SearchService
                        context = searchService.createSearchContext(shardRequest, SearchService.NO_TIMEOUT);
                        context.preProcess();
                        searchContexts.add(context);
                    } catch (Exception e) {
                        if (addShardLevelFailure(shard.shardId(), e)) {
                            IOUtils.close(context);
                        } else {
                            IOUtils.closeWhileHandlingException(context, () -> IOUtils.close(searchContexts));
                            throw e;
                        }
                    }
                }
                return searchContexts;
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
                var internalSink = exchangeService.createSinkHandler(request.sessionId(), request.pragmas().exchangeBufferSize());
                DataNodeRequestExecutor dataNodeRequestExecutor = new DataNodeRequestExecutor(
                    request,
                    task,
                    internalSink,
                    request.configuration().pragmas().maxConcurrentShardsPerNode(),
                    failFastOnShardFailure,
                    shardLevelFailures,
                    computeListener
                );
                dataNodeRequestExecutor.start();
                // run the node-level reduction
                var externalSink = exchangeService.getSinkHandler(externalId);
                task.addListener(
                    () -> exchangeService.finishSinkHandler(externalId, new TaskCancelledException(task.getReasonCancelled()))
                );
                var exchangeSource = new ExchangeSourceHandler(1, esqlExecutor);
                exchangeSource.addRemoteSink(internalSink::fetchPageAsync, true, () -> {}, 1, ActionListener.noop());
                var reductionListener = computeListener.acquireCompute();
                computeService.runCompute(
                    task,
                    new ComputeContext(
                        request.sessionId(),
                        "node_reduce",
                        request.clusterAlias(),
                        List.of(),
                        request.configuration(),
                        new FoldContext(request.pragmas().foldLimit().getBytes()),
                        exchangeSource::createExchangeSource,
                        () -> externalSink.createExchangeSink(() -> {})
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
    }

    @Override
    public void messageReceived(DataNodeRequest request, TransportChannel channel, Task task) {
        final ActionListener<DataNodeComputeResponse> listener = new ChannelActionListener<>(channel);
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
        // the sender doesn't support retry on shard failures, so we need to fail fast here.
        final boolean failFastOnShardFailures = supportShardLevelRetryFailure(channel.getVersion()) == false;
        runComputeOnDataNode((CancellableTask) task, sessionId, reductionPlan, request, failFastOnShardFailures, listener);
    }

    static boolean supportShardLevelRetryFailure(TransportVersion transportVersion) {
        return transportVersion.onOrAfter(TransportVersions.ESQL_RETRY_ON_SHARD_LEVEL_FAILURE)
            || transportVersion.isPatchFrom(TransportVersions.ESQL_RETRY_ON_SHARD_LEVEL_FAILURE_BACKPORT_8_19);
    }
}
