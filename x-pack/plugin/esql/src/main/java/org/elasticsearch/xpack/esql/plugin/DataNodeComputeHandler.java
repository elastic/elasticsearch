/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.compute.operator.exchange.LocalExchange;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.SliceSelection;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.datasources.DatasetConfigResolver;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceAuthorizer;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceDatasets;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

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
    /**
     * Resolved once: {@link Federation#FEDERATION_ENABLED} is node scoped and takes effect only after a restart, so
     * every external request on this node gets the same answer.
     */
    private final boolean federationAvailable;
    /**
     * Decides whether a received plan may read the datasets it names, before anything is planned or rebound; see
     * {@link #messageReceived}.
     */
    private final ExternalSourceAuthorizer externalSourceAuthorizer;
    /** {@link ExternalSourceSettings#WORKER_READY_TIMEOUT}, tracked live; see {@link #waitUntilReady}. */
    private final AtomicReference<TimeValue> workerReadyTimeout = new AtomicReference<>();
    /**
     * Per declining worker id, when this coordinator last logged at INFO that it routed around that worker. A node joining
     * under dashboard load declines hundreds of requests within a second, each of which is one DEBUG line; the operator-facing
     * INFO is kept to one per node per {@link #DECLINE_INFO_INTERVAL_MILLIS}. Entries are not evicted: one per node id that
     * ever declined to this coordinator, which node churn bounds.
     */
    private static final long DECLINE_INFO_INTERVAL_MILLIS = TimeValue.timeValueMinutes(1).millis();
    private final Map<String, Long> lastDeclineInfoMillis = new ConcurrentHashMap<>();

    DataNodeComputeHandler(
        ComputeService computeService,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        SearchService searchService,
        TransportService transportService,
        ExchangeService exchangeService,
        Executor searchExecutor,
        Client client
    ) {
        this.computeService = computeService;
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.searchService = searchService;
        this.transportService = transportService;
        this.exchangeService = exchangeService;
        this.searchExecutor = searchExecutor;
        this.threadPool = transportService.getThreadPool();
        this.federationAvailable = Federation.isAvailable(clusterService.getSettings());
        this.externalSourceAuthorizer = new ExternalSourceAuthorizer(
            client,
            threadPool.getThreadContext(),
            searchExecutor,
            Build.current().isSnapshot()
        );
        // Seeded from the node settings when federation is unregistered (the key is unknown to ClusterSettings then) and
        // watched otherwise, like the other dynamic external-source settings in EsqlPlugin.
        clusterService.getClusterSettings()
            .initializeAndWatchIfRegistered(ExternalSourceSettings.WORKER_READY_TIMEOUT, workerReadyTimeout::set);
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
            PlannerUtils.sliceSelection(dataNodePlan),
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
                        try (var computeListener = new ComputeListener(onGroupFailure, l.map(ignored -> nodeResponseRef.get()))) {
                            final boolean sameNodeAsCoordinator = transportService.getLocalNode()
                                .getId()
                                .equals(connection.getNode().getId());
                            boolean enableReduceNodeLateMaterialization = EsqlCapabilities.Cap.ENABLE_REDUCE_NODE_LATE_MATERIALIZATION
                                .isEnabled();
                            var dataNodeRequest = new DataNodeRequest(
                                childSessionId,
                                computeService.queryId(parentTask),
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
                                sameNodeAsCoordinator && queryPragmas.singleNodeOptimizations() && Strings.isEmpty(clusterAlias)
                            );
                            ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
                            transportService.sendChildRequest(
                                connection,
                                ComputeService.DATA_ACTION_NAME,
                                dataNodeRequest,
                                groupTask,
                                TransportRequestOptions.EMPTY,
                                new ActionListenerResponseHandler<>(computeListener.acquireCompute().map(r -> {
                                    nodeResponseRef.set(r);
                                    return r.completionInfo();
                                }), in -> new DataNodeComputeResponse(in, threadContext), searchExecutor)
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

        final var keepAlive = new ExchangeSourceLinkKeepAlive(exchangeSource);
        try {
            ExternalDispatchResolution beforeReassign = resolveExternalAssignments(
                distributionPlan.nodeAssignments(),
                nodeId -> clusterService.state().nodes().get(nodeId),
                transportService::getConnection
            );
            ExternalDispatchResolution resolution = reassignUnreachableSplits(beforeReassign);
            if (resolution.resolved().isEmpty()) {
                if (resolution.unresolved().isEmpty() == false) {
                    LOGGER.warn(
                        "external splits on [{}] unreachable nodes (0 reachable): [{}]",
                        resolution.unresolved().size(),
                        unresolvedNodeSummary(resolution.unresolved())
                    );
                    parentComputeListener.acquireCompute().onFailure(allExternalWorkersFailed(resolution.unresolved()));
                } else {
                    parentComputeListener.acquireCompute().onResponse(DriverCompletionInfo.EMPTY);
                }
                return;
            }
            if (beforeReassign.unresolved().isEmpty() == false) {
                LOGGER.debug(
                    () -> Strings.format(
                        "reassigned external splits from [%s] unreachable nodes onto [%s] reachable nodes: [%s]",
                        beforeReassign.unresolved().size(),
                        resolution.resolved().size(),
                        unresolvedNodeSummary(beforeReassign.unresolved())
                    )
                );
            }

            // The version the splits were planned under, or at least as new: the state only moves forward on this node, and a
            // worker that has applied a lower version and cannot bind a dataset of the plan is behind rather than missing it.
            final long requiredClusterStateVersion = clusterService.state().version();
            final List<ExternalDispatchResolution.ResolvedExternalNode> workers = resolution.resolved();
            for (int i = 0; i < workers.size(); i++) {
                ExternalDispatchResolution.ResolvedExternalNode target = workers.get(i);
                // Should this worker not have applied the cluster state its splits were planned from, they walk the other
                // resolved workers in turn and the last of them waits for that state; a lone worker waits at once (see
                // ExternalSplitWalk and DataNodeRequest).
                final int targetIndex = i;
                DiscoveryNode node = target.node();
                Transport.Connection connection = target.connection();
                List<ExternalSplit> nodeSplits = target.splits();

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
                    // The node the splits currently run on: the target, or the worker of the latest hop of their walk.
                    final AtomicReference<DiscoveryNode> currentNode = new AtomicReference<>(node);
                    final Runnable onGroupFailure;
                    final CancellableTask groupTask;
                    if (allowPartial) {
                        try {
                            groupTask = computeService.createGroupTask(
                                parentTask,
                                () -> "compute group: external data-node ["
                                    + currentNode.get().getName()
                                    + "], splits ["
                                    + nodeSplits.size()
                                    + "]"
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
                    try (var computeListener = new ComputeListener(onGroupFailure, ActionListener.wrap(info -> {
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
                        final ExternalSplitWalk walk = new ExternalSplitWalk(
                            sessionId,
                            computeService.queryId(parentTask),
                            workers,
                            targetIndex,
                            configuration,
                            dataNodePlan,
                            requiredClusterStateVersion,
                            groupTask,
                            exchangeSource,
                            allowPartial,
                            computeListener,
                            currentNode
                        );
                        ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
                        transportService.sendChildRequest(
                            connection,
                            ComputeService.DATA_ACTION_NAME,
                            walk.request(childSessionId, 0),
                            groupTask,
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(
                                walk.attemptListener(0),
                                in -> new DataNodeComputeResponse(in, threadContext),
                                searchExecutor
                            )
                        );
                        var remoteSink = exchangeService.newRemoteSink(groupTask, childSessionId, transportService, connection);
                        exchangeSource.addRemoteSink(
                            remoteSink,
                            allowPartial == false,
                            walk::pageFetched,
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
        } finally {
            keepAlive.done();
        }
    }

    /**
     * Result of looking up each assigned worker at dispatch. {@code unresolved} is every
     * assignment whose node is gone from cluster state or whose connection cannot be opened.
     */
    record ExternalDispatchResolution(List<ResolvedExternalNode> resolved, List<UnresolvedExternalNode> unresolved) {
        record ResolvedExternalNode(DiscoveryNode node, Transport.Connection connection, List<ExternalSplit> splits) {}

        record UnresolvedExternalNode(String nodeId, List<ExternalSplit> splits, Exception error) {}
    }

    /**
     * Looks up a transport connection for a resolved worker. Exists so
     * {@link TransportService#getConnection} can throw and so tests can inject failures.
     */
    @FunctionalInterface
    interface ExternalNodeConnectionLookup {
        Transport.Connection get(DiscoveryNode node) throws Exception;
    }

    /**
     * Resolves each assignment to a live node and connection. An empty assignment that
     * connects is kept as a target so later reassignment can place orphans on it. An
     * empty assignment that fails to resolve is dropped: it has no splits to move.
     * Non-empty failures stay on {@code unresolved}; this step does not move splits.
     */
    static ExternalDispatchResolution resolveExternalAssignments(
        Map<String, List<ExternalSplit>> nodeAssignments,
        Function<String, DiscoveryNode> nodes,
        ExternalNodeConnectionLookup connections
    ) {
        List<ExternalDispatchResolution.ResolvedExternalNode> resolved = new ArrayList<>();
        List<ExternalDispatchResolution.UnresolvedExternalNode> unresolved = new ArrayList<>();
        for (Map.Entry<String, List<ExternalSplit>> entry : nodeAssignments.entrySet()) {
            String nodeId = entry.getKey();
            List<ExternalSplit> nodeSplits = entry.getValue();
            DiscoveryNode node = nodes.apply(nodeId);
            if (node == null) {
                if (nodeSplits.isEmpty() == false) {
                    unresolved.add(
                        new ExternalDispatchResolution.UnresolvedExternalNode(
                            nodeId,
                            nodeSplits,
                            new IllegalStateException(
                                "node [" + nodeId + "] assigned [" + nodeSplits.size() + "] external splits not found in cluster state"
                            )
                        )
                    );
                }
                continue;
            }
            try {
                resolved.add(new ExternalDispatchResolution.ResolvedExternalNode(node, connections.get(node), nodeSplits));
            } catch (Exception e) {
                if (nodeSplits.isEmpty() == false) {
                    unresolved.add(new ExternalDispatchResolution.UnresolvedExternalNode(nodeId, nodeSplits, e));
                }
            }
        }
        return new ExternalDispatchResolution(List.copyOf(resolved), List.copyOf(unresolved));
    }

    /**
     * Moves splits from unresolved workers onto resolved workers, appending round-robin
     * in resolved-node order. Resolved workers that still have no splits after that are
     * dropped so the dispatcher only sends work. When no worker resolved, the input is
     * returned unchanged so the dispatcher can fail the query.
     */
    static ExternalDispatchResolution reassignUnreachableSplits(ExternalDispatchResolution resolution) {
        if (resolution.resolved().isEmpty()) {
            return resolution;
        }
        if (resolution.unresolved().isEmpty()) {
            return withoutEmptyResolved(resolution);
        }
        List<List<ExternalSplit>> expanded = new ArrayList<>(resolution.resolved().size());
        for (ExternalDispatchResolution.ResolvedExternalNode resolved : resolution.resolved()) {
            expanded.add(new ArrayList<>(resolved.splits()));
        }
        int next = 0;
        for (ExternalDispatchResolution.UnresolvedExternalNode unresolved : resolution.unresolved()) {
            for (ExternalSplit split : unresolved.splits()) {
                expanded.get(next % expanded.size()).add(split);
                next++;
            }
        }
        List<ExternalDispatchResolution.ResolvedExternalNode> reassigned = new ArrayList<>(resolution.resolved().size());
        for (int i = 0; i < resolution.resolved().size(); i++) {
            ExternalDispatchResolution.ResolvedExternalNode original = resolution.resolved().get(i);
            reassigned.add(
                new ExternalDispatchResolution.ResolvedExternalNode(original.node(), original.connection(), List.copyOf(expanded.get(i)))
            );
        }
        return withoutEmptyResolved(new ExternalDispatchResolution(List.copyOf(reassigned), List.of()));
    }

    private static ExternalDispatchResolution withoutEmptyResolved(ExternalDispatchResolution resolution) {
        List<ExternalDispatchResolution.ResolvedExternalNode> kept = new ArrayList<>(resolution.resolved().size());
        for (ExternalDispatchResolution.ResolvedExternalNode resolved : resolution.resolved()) {
            if (resolved.splits().isEmpty() == false) {
                kept.add(resolved);
            }
        }
        if (kept.size() == resolution.resolved().size()) {
            return resolution;
        }
        return new ExternalDispatchResolution(List.copyOf(kept), resolution.unresolved());
    }

    /**
     * Failure when every assigned worker with splits was unreachable. The first
     * per-node error is the cause; the rest are suppressed. The message does not
     * mention partial results: none of the work can run.
     */
    static IllegalStateException allExternalWorkersFailed(List<ExternalDispatchResolution.UnresolvedExternalNode> unresolved) {
        ExternalDispatchResolution.UnresolvedExternalNode first = unresolved.getFirst();
        IllegalStateException failure = new IllegalStateException(
            "all [" + unresolved.size() + "] nodes assigned external splits failed",
            first.error()
        );
        for (int i = 1; i < unresolved.size(); i++) {
            failure.addSuppressed(unresolved.get(i).error());
        }
        return failure;
    }

    private static String unresolvedNodeSummary(List<ExternalDispatchResolution.UnresolvedExternalNode> unresolved) {
        StringBuilder summary = new StringBuilder();
        for (ExternalDispatchResolution.UnresolvedExternalNode node : unresolved) {
            if (summary.isEmpty() == false) {
                summary.append(',');
            }
            summary.append(node.nodeId()).append('=').append(node.splits().size());
        }
        return summary.toString();
    }

    /**
     * The worker attempt {@code hop} of a walk runs on: {@code hop} places after the walk's original target
     * ({@code resolved.get(index)}) in resolved order, wrapping around, so a walk visits every other worker once.
     * "Next after", not "first other": the distribution strategies offset concurrent leaves (UNION, FORK) so they do not
     * all start on the first node, and one lagging node must not undo that by sending every leaf's declined splits to the
     * same worker. {@code null} once the walk is exhausted, and for a lone worker, which waits for the state instead.
     * The strategies keep no locality or per-node statistics, so any resolved worker can run any split.
     */
    @Nullable
    static ExternalDispatchResolution.ResolvedExternalNode nextWorker(
        List<ExternalDispatchResolution.ResolvedExternalNode> resolved,
        int index,
        int hop
    ) {
        if (hop <= 0 || hop >= resolved.size()) {
            return null;
        }
        return resolved.get((index + hop) % resolved.size());
    }

    /**
     * Whether attempt {@code hop} of a walk over {@code workers} resolved workers asks the worker to decline rather than
     * wait when it has not applied the cluster state the splits were planned from: every attempt but the walk's last.
     * The last worker waits because, with every other one having declined, waiting is cheaper than failing; a lone
     * worker's only attempt is its last.
     */
    static boolean declineIfNotReady(int hop, int workers) {
        return hop < workers - 1;
    }

    /**
     * Whether a worker's failure is its decline of the splits for not having applied the cluster state they were planned
     * from, as opposed to a real failure. Decided on the typed condition, which survives the node boundary in the
     * exception metadata ({@link ExternalException#hasCondition}), never on message text.
     */
    static boolean isWorkerNotReadyFailure(Exception failure) {
        return ExternalException.hasCondition(failure, ExternalException.Condition.WORKER_NOT_READY);
    }

    private static DataNodeRequest externalRequest(
        String sessionId,
        TaskId queryId,
        Configuration configuration,
        ExchangeSinkExec dataNodePlan,
        List<ExternalSplit> splits,
        boolean declineIfNotReady,
        long requiredClusterStateVersion
    ) {
        return new DataNodeRequest(
            sessionId,
            queryId,
            configuration,
            "",
            List.of(),
            Map.of(),
            dataNodePlan,
            new String[0],
            IndicesOptions.STRICT_EXPAND_OPEN,
            configuration.pragmas().nodeLevelReduction(),
            false,
            false,
            splits,
            declineIfNotReady,
            requiredClusterStateVersion
        );
    }

    /**
     * The walk one worker's external splits take across the resolved workers. Attempt 0 is the original target; each
     * worker that declines the splits, for not having applied the cluster state they were planned from, hands them on
     * to the next in resolved order ({@link #nextWorker}), under the same group task and into the same exchange source,
     * until a worker runs them or the last of the walk waits for the state ({@link #declineIfNotReady}). A worker's
     * connection is the one captured at resolution; if it has gone since, the hop fails and so does the query, with a
     * retryable status: a walk passes declines, not failures. A hop's exchange is opened with a plain request, not a
     * child of the task, so a hop that races a cancellation can still open an exchange that is only reaped later, as
     * any attempt's can.
     *
     * <p>Two slots of the inner {@link ComputeListener} are acquired up front, while it is still open: the response slot,
     * completed by whichever attempt answers last, and the walk ref, a spare slot held until then. The listener's refs can
     * be acquired after its try block closes as long as one is outstanding, which is what the walk ref is for: every hop
     * acquires its own sink slot. The two also hold the {@code ExchangeSourceLinkKeepAlive} ({@code finishNode} runs when
     * the inner listener completes), so the exchange source stays open across the hops; without that, a decliner's
     * empty-finished sink could be the source's last and finish it before the next hop's sink is added.
     */
    private final class ExternalSplitWalk {
        private final String sessionId;
        private final TaskId queryId;
        private final List<ExternalDispatchResolution.ResolvedExternalNode> workers;
        private final int targetIndex;
        private final Configuration configuration;
        private final ExchangeSinkExec dataNodePlan;
        private final long requiredClusterStateVersion;
        private final CancellableTask groupTask;
        private final ExchangeSourceHandler exchangeSource;
        private final boolean allowPartial;
        private final ComputeListener computeListener;
        /** The node the splits currently run on, for the group task's description. */
        private final AtomicReference<DiscoveryNode> currentNode;
        private final AtomicLong pagesFetched = new AtomicLong();
        private final ActionListener<DriverCompletionInfo> response;
        @Nullable
        private final ActionListener<Void> walkRef;
        /** Set once the walk's outcome is decided; a decline that arrives after that must not hop on a closed listener. */
        private final AtomicBoolean ended = new AtomicBoolean();

        ExternalSplitWalk(
            String sessionId,
            TaskId queryId,
            List<ExternalDispatchResolution.ResolvedExternalNode> workers,
            int targetIndex,
            Configuration configuration,
            ExchangeSinkExec dataNodePlan,
            long requiredClusterStateVersion,
            CancellableTask groupTask,
            ExchangeSourceHandler exchangeSource,
            boolean allowPartial,
            ComputeListener computeListener,
            AtomicReference<DiscoveryNode> currentNode
        ) {
            this.sessionId = sessionId;
            this.queryId = queryId;
            this.workers = workers;
            this.targetIndex = targetIndex;
            this.configuration = configuration;
            this.dataNodePlan = dataNodePlan;
            this.requiredClusterStateVersion = requiredClusterStateVersion;
            this.groupTask = groupTask;
            this.exchangeSource = exchangeSource;
            this.allowPartial = allowPartial;
            this.computeListener = computeListener;
            this.currentNode = currentNode;
            // Each completed exactly once, whichever step of whichever attempt ends the walk, and a step that throws after
            // the attempt's response handler was registered must not complete them a second time.
            this.response = ActionListener.notifyOnce(computeListener.acquireCompute());
            this.walkRef = workers.size() < 2 ? null : ActionListener.notifyOnce(computeListener.acquireAvoid());
        }

        private ExternalDispatchResolution.ResolvedExternalNode worker(int hop) {
            return workers.get((targetIndex + hop) % workers.size());
        }

        private List<ExternalSplit> splits() {
            return worker(0).splits();
        }

        DataNodeRequest request(String childSessionId, int hop) {
            return externalRequest(
                childSessionId,
                queryId,
                configuration,
                dataNodePlan,
                splits(),
                declineIfNotReady(hop, workers.size()),
                requiredClusterStateVersion
            );
        }

        void pageFetched() {
            pagesFetched.incrementAndGet();
        }

        /**
         * The response listener of attempt {@code attempt}. The decline is decided here, on a plain listener, before any
         * {@link ComputeListener} slot sees it: a failure delivered to a slot cancels the query ({@code acquireAvoid} runs
         * the group failure hook), which would end the query the hop is meant to save. A decline that comes with pages
         * already fetched is a misbehaving worker and fails the query rather than walking on. A throw out of the hop
         * (a listener that cannot be acquired, say) fails the walk rather than escaping the transport thread with the
         * slots uncompleted, which would hang the query.
         */
        ActionListener<DataNodeComputeResponse> attemptListener(int attempt) {
            return ActionListener.wrap(r -> succeed(r.completionInfo()), e -> {
                if (ended.get() == false
                    && nextWorker(workers, targetIndex, attempt + 1) != null
                    && pagesFetched.get() == 0
                    && isWorkerNotReadyFailure(e)) {
                    try {
                        hop(attempt + 1);
                    } catch (Exception hopFailure) {
                        hopFailure.addSuppressed(e);
                        fail(hopFailure);
                    }
                    return;
                }
                fail(e);
            });
        }

        private void succeed(DriverCompletionInfo info) {
            try {
                response.onResponse(info);
            } finally {
                endWalk();
            }
        }

        private void fail(Exception e) {
            try {
                response.onFailure(e);
            } finally {
                endWalk();
            }
        }

        private void endWalk() {
            ended.set(true);
            if (walkRef != null) {
                walkRef.onResponse(null);
            }
        }

        /** Sends the splits the worker of attempt {@code attempt - 1} declined to the worker of attempt {@code attempt}. */
        private void hop(int attempt) {
            final DiscoveryNode declined = worker(attempt - 1).node();
            final ExternalDispatchResolution.ResolvedExternalNode next = worker(attempt);
            currentNode.set(next.node());
            // This hop's sink slot, acquired while the walk ref keeps the inner ComputeListener open, and completed exactly
            // once: by the remote sink, or here when nothing was sent and no sink was added.
            final ActionListener<Void> sink = ActionListener.notifyOnce(computeListener.acquireAvoid());
            final var queryPragmas = configuration.pragmas();
            final String childSessionId = computeService.newChildSession(sessionId);
            final ActionListener<Void> opened = ActionListener.wrap(unused -> {
                ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
                transportService.sendChildRequest(
                    next.connection(),
                    ComputeService.DATA_ACTION_NAME,
                    request(childSessionId, attempt),
                    groupTask,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(
                        attemptListener(attempt),
                        in -> new DataNodeComputeResponse(in, threadContext),
                        searchExecutor
                    )
                );
                var remoteSink = exchangeService.newRemoteSink(groupTask, childSessionId, transportService, next.connection());
                exchangeSource.addRemoteSink(
                    remoteSink,
                    allowPartial == false,
                    this::pageFetched,
                    queryPragmas.concurrentExchangeClients(),
                    sink
                );
            }, e -> {
                try {
                    fail(e);
                } finally {
                    sink.onResponse(null);
                }
            });
            try {
                logDeclinedWorker(declined, next.node(), splits().size(), attempt, workers.size() - 1, childSessionId);
                ExchangeService.openExchange(
                    transportService,
                    next.connection(),
                    childSessionId,
                    queryPragmas.exchangeBufferSize(),
                    searchExecutor,
                    opened
                );
            } catch (Exception e) {
                opened.onFailure(e);
            }
        }
    }

    /**
     * One DEBUG line per hop, and an INFO line at most once per declining node per minute: a node joining under query load
     * declines every request it gets for a second or so, and the operator needs the fact, not the volume.
     */
    private void logDeclinedWorker(DiscoveryNode declined, DiscoveryNode next, int splits, int hop, int hops, String sessionId) {
        LOGGER.debug(
            "external worker [{}] has not applied the cluster state this query was planned from; "
                + "rerouting [{}] splits to [{}] on hop [{}/{}], session [{}]",
            declined.getName(),
            splits,
            next.getName(),
            hop,
            hops,
            sessionId
        );
        final long now = threadPool.relativeTimeInMillis();
        final boolean[] logInfo = new boolean[1];
        lastDeclineInfoMillis.compute(declined.getId(), (id, last) -> {
            if (last == null || now - last >= DECLINE_INFO_INTERVAL_MILLIS) {
                logInfo[0] = true;
                return now;
            }
            return last;
        });
        if (logInfo[0]) {
            LOGGER.info(
                "external worker [{}] declined external splits because it has not yet applied the cluster state the query was planned "
                    + "from; routing its splits to other workers (reported at most once a minute per node)",
                declined.getName()
            );
        }
    }

    private static final Logger LOGGER = LogManager.getLogger(DataNodeComputeHandler.class);
    /** {@link ClusterStateObserver} takes a log4j logger; same name as {@link #LOGGER}, so it is configured together with it. */
    private static final org.apache.logging.log4j.Logger OBSERVER_LOGGER = org.apache.logging.log4j.LogManager.getLogger(
        DataNodeComputeHandler.class
    );

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
        private final LocalExchange internalExchange;
        private final ExchangeSinkHandler externalSink;
        private final String externalId;
        private final ComputeListener computeListener;
        private final int maxConcurrentShards;
        private final ExchangeSink blockingSink; // block until we have completed on all shards or the coordinator has enough data
        private final boolean failFastOnShardFailure;
        private final boolean singleNodeOptimizations;
        private final Map<ShardId, Exception> shardLevelFailures;
        private final AcquiredSearchContexts searchContexts;
        private final PlanTimeProfile planTimeProfile;

        DataNodeRequestExecutor(
            EsqlFlags flags,
            DataNodeRequest request,
            CancellableTask parentTask,
            LocalExchange internalExchange,
            ExchangeSinkHandler externalSink,
            String externalId,
            int maxConcurrentShards,
            boolean failFastOnShardFailure,
            boolean singleNodeOptimizations,
            Map<ShardId, Exception> shardLevelFailures,
            ComputeListener computeListener,
            AcquiredSearchContexts searchContexts
        ) {
            assert (internalExchange == null) != (externalSink == null) : "exactly one exchange must be provided";
            this.flags = flags;
            this.request = request;
            this.parentTask = parentTask;
            this.internalExchange = internalExchange;
            this.externalSink = externalSink;
            this.externalId = externalId;
            this.computeListener = computeListener;
            this.maxConcurrentShards = maxConcurrentShards;
            this.failFastOnShardFailure = failFastOnShardFailure;
            this.singleNodeOptimizations = singleNodeOptimizations;
            this.shardLevelFailures = shardLevelFailures;
            this.blockingSink = exchangeSink(() -> {});
            this.searchContexts = searchContexts;
            this.planTimeProfile = new PlanTimeProfile();
        }

        void start() {
            runBatch(0);
        }

        private ExchangeSink exchangeSink(Runnable onPageAdded) {
            return internalExchange != null ? internalExchange.exchangeSink(onPageAdded) : externalSink.createExchangeSink(onPageAdded);
        }

        private boolean exchangeFinished() {
            return internalExchange != null ? internalExchange.isFinished() : externalSink.isFinished();
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
                            if (internalExchange != null) {
                                internalExchange.finish(true);
                            } else {
                                exchangeService.finishSinkHandler(externalId, e);
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
                PlannerUtils.sliceSelection(request.plan()),
                ActionListener.wrap(acquiredSearchContexts -> {
                    assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH);
                    if (acquiredSearchContexts.isEmpty()) {
                        batchListener.onResponse(DriverCompletionInfo.EMPTY);
                        return;
                    }
                    var computeContext = new ComputeContext(
                        sessionId,
                        request.queryId(),
                        ComputeService.DATA_DESCRIPTION,
                        clusterAlias,
                        flags,
                        acquiredSearchContexts,
                        configuration,
                        configuration.newFoldContext(),
                        null,
                        () -> exchangeSink(pagesProduced::incrementAndGet),
                        singleNodeOptimizations
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
            SliceSelection slices,
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
                        // The slices the plan reads, for the field mappers that build its queries.
                        context.getSearchExecutionContext().setSliceSelection(slices);
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
            if (lastBatchIndex < request.shards().size() && exchangeFinished() == false) {
                runBatch(lastBatchIndex);
            } else {
                // don't return until all pages have been consumed
                if (internalExchange != null) {
                    internalExchange.addCompletionListener(computeListener.acquireAvoid());
                } else {
                    externalSink.addCompletionListener(
                        ActionListener.runBefore(computeListener.acquireAvoid(), () -> exchangeService.finishSinkHandler(externalId, null))
                    );
                }
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
        ReductionPlan reductionPlan,
        DataNodeRequest request,
        boolean failFastOnShardFailure,
        AcquiredSearchContexts searchContexts,
        PlannerSettings plannerSettings,
        PlanTimeProfile planTimeProfile,
        ActionListener<DataNodeComputeResponse> listener
    ) {
        final Map<ShardId, Exception> shardLevelFailures = new HashMap<>();
        // Resolve before the compute listener. A sources-finished exchange completes this request with an empty
        // result; opening the listener first would also complete it when that early return skips every acquire.
        final ExchangeSinkHandler externalSink = sinkToRun(externalId, listener);
        if (externalSink == null) {
            return;
        }
        try (
            ComputeListener computeListener = new ComputeListener(
                computeService.cancelQueryOnFailure(task),
                listener.map(profiles -> new DataNodeComputeResponse(profiles, shardLevelFailures))
            )
        ) {
            var parentListener = computeListener.acquireAvoid();
            final boolean runNodeReduce = reductionPlan.isPassThrough() == false;
            final LocalExchange internalExchange = runNodeReduce ? new LocalExchange(request.pragmas().exchangeBufferSize()) : null;
            try {
                assert request.singleNodeOptimizations() == false
                    || task.getParentTaskId().getNodeId().equals(transportService.getLocalNode().getId())
                    : "single node optimizations enabled but wrong parent task: " + task + " vs " + transportService.getLocalNode().getId();
                // run compute with target shards
                task.addListener(() -> {
                    exchangeService.finishSinkHandler(externalId, new TaskCancelledException(task.getReasonCancelled()));
                    if (internalExchange != null) {
                        internalExchange.finish(true);
                    }
                });
                EsqlFlags flags = computeService.createFlags();
                int maxConcurrentShards = request.pragmas().maxConcurrentShardsPerNode();
                DataNodeRequestExecutor dataNodeRequestExecutor = new DataNodeRequestExecutor(
                    flags,
                    request,
                    task,
                    internalExchange,
                    runNodeReduce ? null : externalSink,
                    externalId,
                    maxConcurrentShards,
                    failFastOnShardFailure,
                    request.singleNodeOptimizations(),
                    shardLevelFailures,
                    computeListener,
                    searchContexts
                );
                dataNodeRequestExecutor.start();
                if (runNodeReduce == false) {
                    parentListener.onResponse(null);
                    return;
                }
                // run the node-level reduction
                var reductionListener = computeListener.acquireCompute();
                computeService.runCompute(
                    task,
                    new ComputeContext(
                        request.sessionId(),
                        request.queryId(),
                        ComputeService.REDUCE_DESCRIPTION,
                        request.clusterAlias(),
                        flags,
                        searchContexts.globalView(),
                        request.configuration(),
                        new FoldContext(request.pragmas().foldLimit().getBytes()),
                        internalExchange::exchangeSource,
                        () -> externalSink.createExchangeSink(() -> {}),
                        request.singleNodeOptimizations()
                    ),
                    reductionPlan.nodeReducePlan(),
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
                if (internalExchange != null) {
                    internalExchange.finish(true);
                }
                parentListener.onFailure(e);
            }
        }
    }

    /**
     * The query of a data-node request: its {@link DataNodeRequest#queryId()}, or its parent task if the sender does not
     * send one.
     */
    static TaskId queryId(DataNodeRequest request, Task task) {
        return request.queryId().isSet() ? request.queryId() : task.getParentTaskId();
    }

    /**
     * A plan that names an external dataset is authorized for the request's credential before any branch below runs,
     * so neither the planning nor the rebind ({@link DatasetConfigResolver}) ever sees a dataset the caller may not
     * read. The security filter authorized this action on the wire-supplied {@code indices()} only, which says nothing
     * about the datasets inside the plan; see {@link ExternalSourceAuthorizer}. Both request shapes a coordinator
     * sends, external splits without shards and shards with a plan that also reads a dataset, are authorized by the
     * same rule; a plan with an external node but neither shards nor splits has no legitimate sender and is refused,
     * as is any external node under a cross-cluster-access authentication. A plan without an external node, the
     * common case, pays nothing: no round-trip, no thread hop.
     * <p>
     * An external-only request (splits, no shards) is also checked for this node's readiness to serve it. Shards are
     * routed through the routing table, which lists a shard on a node only after that node applied the state and reported
     * it started; external splits are routed by node role, which the coordinator sees as soon as it applies the join, so
     * a node that accepts transport requests before it has applied any cluster state ({@code Node.start()} accepts
     * requests before the initial join) receives splits it cannot serve: its state has no dataset, no encryption key and
     * default settings. Likewise a node behind a dataset change the coordinator planned from. Three sites decide "not
     * ready", with one outcome ({@link #notReady}): the entry check on {@link ClusterState#clusterRecovered} below, the
     * authorizer refusing a dataset while this node's state is older than the coordinator's, and the rebind refusing one
     * for the same reason ({@link #planAndRunExternal}).
     */
    @Override
    public void messageReceived(DataNodeRequest request, TransportChannel channel, Task task) {
        receive(request, channel, task, null);
    }

    /**
     * {@link #messageReceived} with the request's wait budget: {@code null} until the request first waits for cluster
     * state, then the one {@link ReadyDeadline} every later wait of the same request draws on.
     */
    private void receive(DataNodeRequest request, TransportChannel channel, Task task, @Nullable ReadyDeadline deadline) {
        ActionListener<DataNodeComputeResponse> channelListener = new ChannelActionListener<>(channel);
        // Old coordinators receive warnings as transport response headers rather than the ESQL_DRIVER_WARNINGS
        // wire field. Emit them here — before the channel serialises its ThreadContext — so they are included.
        final ActionListener<DataNodeComputeResponse> listener = channel.getVersion().supports(DriverCompletionInfo.ESQL_DRIVER_WARNINGS)
            ? channelListener
            : channelListener.map(resp -> {
                resp.completionInfo().warnings().forEach(HeaderWarning::addWarning);
                return resp;
            });
        final CancellableTask cancellableTask = (CancellableTask) task;
        final TransportVersion channelVersion = channel.getVersion();
        if (request.isExternalOnly()) {
            // Before the dataset fast path (an unnamed snapshot-only EXTERNAL source has no dataset and would skip the
            // authorizer) and before the authorizer, whose local action would otherwise park on the not-recovered block
            // for up to its master timeout. The block is this node's own, cleared by the first state it applies; it says
            // nothing about shards, so it is the right signal for a node that holds none.
            final ClusterState state = clusterService.state();
            if (state.clusterRecovered() == false) {
                notReady(
                    request,
                    cancellableTask,
                    listener,
                    state,
                    ClusterState::clusterRecovered,
                    "no cluster state applied yet",
                    deadline,
                    d -> receive(request, channel, task, d)
                );
                return;
            }
        }
        ExternalSourceDatasets datasets = ExternalSourceDatasets.of(request.plan());
        if (datasets.isEmpty()) {
            startCompute(request, channelVersion, cancellableTask, listener, deadline);
            return;
        }
        final Exception refusal;
        if (request.externalSplits().isEmpty() && request.shards().isEmpty()) {
            refusal = ExternalSourceAuthorizer.unexpectedShape("a data node request with neither shards nor external splits");
        } else if (externalSourceAuthorizer.isCrossClusterAccess()) {
            refusal = ExternalSourceAuthorizer.notLocal();
        } else {
            refusal = null;
        }
        if (refusal != null) {
            failWithoutStarting(request, listener, refusal);
            return;
        }
        // A throw out of startCompute lands in failWithoutStarting; the channel must not be answered a second time if
        // the compute had already been started and answered it.
        authorizeAndStart(request, channelVersion, cancellableTask, datasets, ActionListener.notifyOnce(listener), deadline);
    }

    private void authorizeAndStart(
        DataNodeRequest request,
        TransportVersion channelVersion,
        CancellableTask task,
        ExternalSourceDatasets datasets,
        ActionListener<DataNodeComputeResponse> listener,
        @Nullable ReadyDeadline deadline
    ) {
        // Sampled before the round-trip. The authorizer answers from this node's applied state, which is this one or a
        // newer one; comparing a state sampled afterwards could call a refusal computed on the older state "not lag".
        final ClusterState state = clusterService.state();
        externalSourceAuthorizer.authorize(
            datasets,
            new TaskId(clusterService.localNode().getId(), task.getId()),
            ActionListener.wrap(ignored -> startCompute(request, channelVersion, task, listener, deadline), e -> {
                if (request.isExternalOnly() && isDatasetRefusalWhileBehind(e, request, state)) {
                    // The authorizer resolves names against this node's own project metadata, so a dataset registered
                    // in a state this node has not applied yet is refused here, before the rebind ever runs.
                    notReady(
                        request,
                        task,
                        listener,
                        state,
                        s -> s.version() >= request.requiredClusterStateVersion(),
                        "behind the cluster state the query was planned from",
                        deadline,
                        d -> authorizeAndStart(request, channelVersion, task, datasets, listener, d)
                    );
                    return;
                }
                failWithoutStarting(request, listener, e);
            })
        );
    }

    private void startCompute(
        DataNodeRequest request,
        TransportVersion channelVersion,
        CancellableTask task,
        ActionListener<DataNodeComputeResponse> listener,
        @Nullable ReadyDeadline deadline
    ) {
        Configuration configuration = request.configuration();
        PlanTimeProfile planTimeProfile = null;
        if (configuration.profile()) {
            planTimeProfile = new PlanTimeProfile();
        }

        if (request.externalSplits().isEmpty() == false && request.shards().isEmpty()) {
            handleExternalSourceRequest(request, task, listener, planTimeProfile, deadline);
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
            queryId(request, task),
            request.configuration(),
            request.clusterAlias(),
            request.shards(),
            request.aliasFilters(),
            request.plan(),
            request.indices(),
            request.indicesOptions(),
            request.runNodeLevelReduction(),
            request.reductionLateMaterialization(),
            request.singleNodeOptimizations(),
            request.externalSplits(),
            request.declineIfNotReady(),
            request.requiredClusterStateVersion()
        );
        // the sender doesn't support retry on shard failures, so we need to fail fast here.
        final boolean failFastOnShardFailures = supportShardLevelRetryFailure(channelVersion) == false;
        var computeSearchContexts = new AcquiredSearchContexts(request.shards().size());
        runComputeOnDataNode(
            task,
            sessionId,
            reductionPlan,
            request.withPlan(reductionPlan.dataNodePlan()),
            failFastOnShardFailures,
            computeSearchContexts,
            computeService.plannerSettings().get(),
            planTimeProfile,
            ActionListener.releaseAfter(listener, computeSearchContexts)
        );
    }

    /**
     * Resolves the sink the coordinator opened before this request. When the coordinator has since finished
     * that exchange, completes {@code listener} with an empty result so a {@link ResourceNotFoundException}
     * cannot replace the query's own error. Returns {@code null} when {@code listener} has already been completed.
     */
    private ExchangeSinkHandler sinkToRun(String sessionId, ActionListener<DataNodeComputeResponse> listener) {
        final ExchangeSinkHandler sink;
        try {
            sink = exchangeService.getSinkHandlerIfOpen(sessionId);
        } catch (ResourceNotFoundException e) {
            listener.onFailure(e);
            return null;
        }
        if (sink == null) {
            listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of()));
            return null;
        }
        return sink;
    }

    /**
     * Rejects a request that never got as far as building a driver. The coordinator opened the exchange before
     * sending the request, so a sink handler is already registered here; it has to be failed explicitly or the
     * coordinator's exchange source only learns of the refusal through task cancellation and this node holds an
     * unfinished sink until the inactive-sinks reaper runs. A no-op on the sink side when none is registered.
     */
    private void failWithoutStarting(DataNodeRequest request, ActionListener<?> listener, Exception failure) {
        exchangeService.finishSinkHandler(request.sessionId(), failure);
        listener.onFailure(failure);
    }

    /**
     * This node cannot serve the request's external splits yet: it has not applied the cluster state they were planned
     * from, so the datasets, credentials or settings the plan relies on are not here. What happens is the coordinator's
     * call, carried in the request: with another worker still to try it asked for a decline ({@link #decline}), otherwise
     * this node waits for the state, bounded ({@link #waitUntilReady}). Both are the read-path precedents: a shard copy that
     * cannot serve fails fast and the search moves on, a lone primary parks the request on the cluster state.
     *
     * @param observed the state the decision was made on; the wait observes from it, so a state applied in between is
     *                 not missed
     * @param ready    satisfied by a state this node can serve the request from
     * @param deadline the request's wait budget so far, {@code null} if it has not waited yet; one budget covers every
     *                 wait of a request, since a node that has just applied its first state can find itself behind a
     *                 dataset change right after, and the bound promised to the operator is one, not one per step
     * @param retry    re-runs the step that found this node not ready, on the search pool, once {@code ready} holds,
     *                 handing it the budget to carry on
     */
    private void notReady(
        DataNodeRequest request,
        CancellableTask task,
        ActionListener<DataNodeComputeResponse> listener,
        ClusterState observed,
        Predicate<ClusterState> ready,
        String why,
        @Nullable ReadyDeadline deadline,
        Consumer<ReadyDeadline> retry
    ) {
        if (task.isCancelled()) {
            failWithoutStarting(request, listener, new TaskCancelledException(task.getReasonCancelled()));
            return;
        }
        if (request.declineIfNotReady()) {
            final ExchangeSinkHandler sink = sinkToRun(request.sessionId(), listener);
            if (sink != null) {
                decline(request, sink, listener, why);
            }
        } else {
            waitUntilReady(request, task, listener, observed, ready, why, deadline != null ? deadline : new ReadyDeadline(), retry);
        }
    }

    /**
     * The one bound on all the waits of a request, taken from {@link ExternalSourceSettings#WORKER_READY_TIMEOUT} when the
     * request first waits; a later wait of the same request gets what is left.
     */
    private final class ReadyDeadline {
        private final TimeValue total = workerReadyTimeout.get();
        private final long deadlineMillis = threadPool.relativeTimeInMillis() + total.millis();

        TimeValue remaining() {
            return TimeValue.timeValueMillis(Math.max(0L, deadlineMillis - threadPool.relativeTimeInMillis()));
        }
    }

    /**
     * Refuses the splits so the coordinator hands them to another worker. The sink the coordinator opened is finished
     * <em>empty</em>, not failed: a failed sink aborts the coordinator's whole exchange source when partial results are off,
     * and the coordinator must keep reading from the other workers. Finishing an empty sink completes it at once, so the
     * handler is removed and the response sent before this returns; a coordinator fetch arriving later gets
     * {@code finished=true}. Only the transport response carries the reason, as the typed
     * {@link ExternalException.Condition#WORKER_NOT_READY}, which the coordinator routes on.
     */
    private void decline(DataNodeRequest request, ExchangeSinkHandler sink, ActionListener<DataNodeComputeResponse> listener, String why) {
        final String sessionId = request.sessionId();
        LOGGER.debug(
            "declining [{}] external splits on [{}], session [{}]: {}",
            request.externalSplits().size(),
            transportService.getLocalNode().getName(),
            sessionId,
            why
        );
        sink.addCompletionListener(ActionListener.running(() -> {
            exchangeService.finishSinkHandler(sessionId, null);
            listener.onFailure(workerNotReady(""));
        }));
        sink.createExchangeSink(() -> {}).finish();
    }

    /**
     * Parks the request until this node has applied a state that satisfies {@code ready}, then re-runs {@code retry} on the
     * search pool: the observer delivers a new state on the cluster applier thread (or on the caller's, when the sampled
     * state already qualifies), and neither may run the request. Bounded by {@link ExternalSourceSettings#WORKER_READY_TIMEOUT},
     * after which the request fails with the retryable {@link ExternalException.Condition#WORKER_NOT_READY}; the caller's
     * own retry is then the right thing. A cancellation or the node closing during the wait fails it the same way the
     * request would have failed before starting. Exactly one of the four outcomes runs ({@link Predicates#once}); not
     * {@code notifyOnce}, because the ready outcome continues the request rather than completing the listener.
     */
    private void waitUntilReady(
        DataNodeRequest request,
        CancellableTask task,
        ActionListener<DataNodeComputeResponse> listener,
        ClusterState observed,
        Predicate<ClusterState> ready,
        String why,
        ReadyDeadline deadline,
        Consumer<ReadyDeadline> retry
    ) {
        final TimeValue timeout = deadline.remaining();
        final ActionListener<DataNodeComputeResponse> retryListener = listener.delegateResponse(
            (l, e) -> failWithoutStarting(request, l, e)
        );
        if (timeout.millis() <= 0) {
            // A spent budget (or a setting of 0) is not consulted by the observer, which times out before sampling the
            // state. The state may have arrived since the decision was made: a refusal computed on an older sample must
            // not fail a request the current state can serve.
            if (ready.test(clusterService.state())) {
                searchExecutor.execute(ActionRunnable.wrap(retryListener, l -> retry.accept(deadline)));
            } else {
                failWithoutStarting(request, listener, workerNotReady("waited [" + deadline.total + "]"));
            }
            return;
        }
        LOGGER.debug(
            "waiting up to [{}] for the cluster state before running [{}] external splits on [{}], session [{}]: {}",
            timeout,
            request.externalSplits().size(),
            transportService.getLocalNode().getName(),
            request.sessionId(),
            why
        );
        final ClusterStateObserver observer = new ClusterStateObserver(
            observed,
            clusterService,
            timeout,
            OBSERVER_LOGGER,
            threadPool.getThreadContext()
        );
        final BooleanSupplier first = Predicates.once();
        final Runnable onTimeout = () -> {
            if (first.getAsBoolean()) {
                failWithoutStarting(request, listener, workerNotReady("waited [" + deadline.total + "]"));
            }
        };
        try {
            // The observer registers its listener, and schedules its own timeout, from a task on the cluster applier
            // thread: the very thread that is busy on a node that lags. A bound that cannot fire while that thread is busy
            // is no bound, so the timeout is also scheduled here, off the applier. Whichever of the two fires first wins;
            // the observer's own timeout is kept so it unregisters its listener.
            final Scheduler.Cancellable timeoutGuard = threadPool.schedule(onTimeout, timeout, threadPool.generic());
            task.addListener(() -> {
                if (first.getAsBoolean()) {
                    timeoutGuard.cancel();
                    failWithoutStarting(request, listener, new TaskCancelledException(task.getReasonCancelled()));
                }
            });
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    if (first.getAsBoolean()) {
                        timeoutGuard.cancel();
                        // wrap, not run: the continuation answers the listener itself, run would answer it again with null.
                        searchExecutor.execute(ActionRunnable.wrap(retryListener, l -> retry.accept(deadline)));
                    }
                }

                @Override
                public void onClusterServiceClose() {
                    if (first.getAsBoolean()) {
                        timeoutGuard.cancel();
                        failWithoutStarting(request, listener, new NodeClosedException(clusterService.localNode()));
                    }
                }

                @Override
                public void onTimeout(TimeValue elapsed) {
                    onTimeout.run();
                }
            }, state -> task.isCancelled() || ready.test(state));
        } catch (Exception e) {
            // A node shutting down rejects the scheduling; at the authorizer site a throw would otherwise escape its
            // listener without answering the channel.
            if (first.getAsBoolean()) {
                failWithoutStarting(request, listener, e);
            }
        }
    }

    /** The retryable refusal of a worker that has not applied the cluster state the query was planned from. */
    static ExternalUnavailableException workerNotReady(String detail) {
        return new ExternalUnavailableException(ExternalException.Condition.WORKER_NOT_READY, detail, "");
    }

    /**
     * Whether a dataset refusal was decided on a state older than the one the coordinator planned from, in which case the
     * dataset may be registered in a state this node has not applied yet and the refusal must not be surfaced as is.
     * Consulted only on such a refusal, so an unrelated one-state lag costs nothing. Compares the <em>applied</em> state:
     * applied versions never go backwards on a node, unlike accepted ones across terms. A version of {@code 0} is an old
     * coordinator that sent none; a fresh node's initial state is version 0 too, so it is never "at or above".
     * <p>
     * A refusal is any {@link ElasticsearchStatusException} with status 400 out of the authorizer or the rebind: that is
     * the type and status both raise their binding refusals with ({@link DatasetConfigResolver#notRegistered}, a changed
     * definition, a split outside the resource) and neither raises anything else of that type (a block, a timeout, a
     * cancellation or a rejection come as their own types). A 400 that is not a binding refusal, on a node that is behind,
     * costs at most a hop per such worker of the walk, or one wait, after which the same 400 is reported.
     */
    static boolean isDatasetRefusalWhileBehind(Exception failure, DataNodeRequest request, ClusterState applied) {
        return request.requiredClusterStateVersion() > 0
            && applied.version() < request.requiredClusterStateVersion()
            && failure instanceof ElasticsearchStatusException statusException
            && statusException.status() == RestStatus.BAD_REQUEST;
    }

    /**
     * Diagnoses an old coordinator placing external scan work on an index node during a rolling
     * upgrade. Deliberately does not reject: rejecting without reassignment would fail the query
     * or return partial results, and rolling-upgrade tests run with assertions enabled.
     * A coordinator's own self-assignment is suppressed so it does not duplicate the coordinator warning.
     */
    static void warnIfRemoteIndexWorker(DiscoveryNode localNode, String parentNodeId) {
        if (localNode.hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName()) == false) {
            return;
        }
        // An unset parent task carries the empty node id of TaskId.EMPTY_TASK_ID, which names no coordinator
        // and so cannot be attributed to an old one.
        if (parentNodeId.isEmpty() || parentNodeId.equals(localNode.getId())) {
            return;
        }
        LOGGER.warn(
            "index node [{}] received external ES|QL scan work from coordinator [{}]; expected only during a mixed-version transition",
            localNode.getId(),
            parentNodeId
        );
    }

    private void handleExternalSourceRequest(
        DataNodeRequest request,
        CancellableTask task,
        ActionListener<DataNodeComputeResponse> listener,
        PlanTimeProfile planTimeProfile,
        @Nullable ReadyDeadline deadline
    ) {
        warnIfRemoteIndexWorker(clusterService.localNode(), task.getParentTaskId().getNodeId());
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
        final String sessionId = request.sessionId();
        final ExchangeSinkHandler externalSink = sinkToRun(sessionId, listener);
        if (externalSink == null) {
            return;
        }
        // Cancellation during local planning has to drop the sink the coordinator already opened. Registered
        // before localPlan so a cancel in that window does not wait for the inactive-sink reaper.
        task.addListener(() -> exchangeService.finishSinkHandler(sessionId, new TaskCancelledException(task.getReasonCancelled())));
        planAndRunExternal(request, task, listener, planTimeProfile, externalSink, deadline);
    }

    /**
     * The part of {@link #handleExternalSourceRequest} after the sink is resolved and the cancellation listener registered:
     * local planning, which rebinds the plan's datasets to this node's definitions, and the compute. Re-entered by itself
     * when the rebind refused a dataset while this node was behind the coordinator's cluster state and the coordinator
     * asked for a wait; by then a newer state has been applied.
     */
    private void planAndRunExternal(
        DataNodeRequest request,
        CancellableTask task,
        ActionListener<DataNodeComputeResponse> listener,
        PlanTimeProfile planTimeProfile,
        ExchangeSinkHandler externalSink,
        @Nullable ReadyDeadline deadline
    ) {
        ExchangeSinkExec sinkExec = (ExchangeSinkExec) request.plan();
        Configuration configuration = request.configuration();
        final String sessionId = request.sessionId();
        EsqlFlags flags = computeService.createFlags();
        PlannerSettings plannerSettings = computeService.plannerSettings().get();
        if (task.isCancelled()) {
            listener.onFailure(new TaskCancelledException(task.getReasonCancelled()));
            return;
        }

        // Run localPlan() to expand FragmentExec(ExternalRelation) -> ExternalSourceExec
        // This runs LocalLogicalPlanOptimizer, LocalMapper, and LocalPhysicalPlanOptimizer
        // (including filter pushdown via FormatReader.filterPushdownSupport())
        // Splits are injected before physical optimization so rules like PushStatsToExternalSource see them, and
        // every external source is rebound to this node's own dataset definition (DatasetConfigResolver): the plan
        // came over the wire and does not get to choose what is read or with which credentials.
        // Kept outside the ComputeListener below: a throw before any acquire, followed by close(), would
        // complete the request successfully and leave the sink registered when sourcesFinished was never sent.
        // One state object serves the rebind and, on a refusal, the version comparison: a second sample could be newer
        // and call a refusal computed on the older state "not lag".
        final ClusterState state = clusterService.state();
        final PhysicalPlan planWithSplits;
        try {
            planWithSplits = PlannerUtils.localPlan(
                plannerSettings,
                flags,
                configuration,
                configuration.newFoldContext(),
                sinkExec,
                SearchStats.EMPTY,
                computeService.formatReaderRegistry(),
                request.externalSplits(),
                planTimeProfile,
                DatasetConfigResolver.forProduction(projectResolver.getProjectMetadata(state))
            );
        } catch (Exception e) {
            if (isDatasetRefusalWhileBehind(e, request, state)) {
                // A definition changed between the coordinator's planning and this node's state, or a dataset the
                // authorizer let through is gone from this node's older state. Not the WARN below: under query load every
                // request of the second a node lags would log a stack trace for a condition that resolves itself.
                LOGGER.debug("external source plan refused on a cluster state older than the coordinator's, session [{}]", sessionId, e);
                notReady(
                    request,
                    task,
                    listener,
                    state,
                    s -> s.version() >= request.requiredClusterStateVersion(),
                    "behind the cluster state the query was planned from",
                    deadline,
                    d -> planAndRunExternal(request, task, listener, planTimeProfile, externalSink, d)
                );
                return;
            }
            LOGGER.warn(
                "failed to plan external source compute on data node [{}], session [{}]",
                transportService.getLocalNode().getName(),
                sessionId,
                e
            );
            failWithoutStarting(request, listener, e);
            return;
        }
        if (task.isCancelled()) {
            listener.onFailure(new TaskCancelledException(task.getReasonCancelled()));
            return;
        }

        try (
            ComputeListener computeListener = new ComputeListener(
                computeService.cancelQueryOnFailure(task),
                listener.map(profiles -> new DataNodeComputeResponse(profiles, Map.of()))
            )
        ) {
            var parentListener = computeListener.acquireAvoid();
            final ActionListener<DriverCompletionInfo> driverCompletionListener = ActionListener.notifyOnce(
                computeListener.acquireCompute()
            );
            try {
                String internalSessionId = sessionId + "[n]";
                var computeContext = new ComputeContext(
                    internalSessionId,
                    queryId(request, task),
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
                        // don't return until all pages are fetched
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
