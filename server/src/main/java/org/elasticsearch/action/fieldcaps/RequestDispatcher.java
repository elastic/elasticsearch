/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.SearchShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.CoordinatorRewriteContextProvider;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Dispatches child field-caps requests to old/new data nodes in the local cluster that have shards of the requesting indices.
 * <p>
 * <b>Second resolution.</b> When all initially resolved copies of a shard have become unavailable (e.g. the only copy was on a node
 * that relocated it), the dispatcher resolves its routing once more from the applied cluster state. Any untried copy it finds is queued
 * and sent in the same round. A shard with no active searchable copy ({@link Routing#NO_ACTIVE_COPY}) or a deleted index
 * ({@link Routing#GONE}) is terminal: it fails at once, whatever the remaining budget.
 * <p>
 * <b>Unit rule.</b> The retry unit is the shard when every shard matters (an index filter or {@code include_empty_fields=false}), and the
 * index otherwise. For the index unit, one failure that is not an unavailable copy (e.g. a circuit-breaker exception) keeps the entire
 * index out of the second resolution. A terminal shard is recorded as resolved only when the shard is the unit; on the index unit it
 * leaves the index eligible, so that a sibling shard can still take copies from a later resolution.
 * <p>
 * <b>Wait.</b> When every unit that underwent a second resolution is stalled ({@link Routing#NONE_YET}), the dispatcher parks and waits
 * for the cluster state to change. The wait ends as soon as one parked unit is no longer {@code NONE_YET}: either an untried copy has
 * appeared, the index was deleted, or the shard has no active copy. An unrelated state change that moves nothing for the parked shards
 * does not end the wait. The cluster state observer holds a static {@link ObserverCallback}, which only has a clearable reference to
 * the wait. The reference is cleared when the wait ends, so the observer does not pin the wait, the dispatcher or the request until it
 * unregisters.
 * <p>
 * <b>Budget.</b> One deadline per dispatch, started at the first stalled shard, funded by the
 * {@link TransportFieldCapabilitiesAction#ROUTING_WAIT_TIMEOUT_SETTING} setting. When the timer fires, it marks the budget as spent
 * ({@code routingBudgetSpent}), so a later round cannot park again even if the cached clock still shows time left. At zero, the second
 * resolution still runs and sends every copy it finds, but no wait is started.
 * <p>
 * <b>Terminal path.</b> If the executor rejects the hand-off after a wait, the dispatch ends via {@link #terminate}: it fails every
 * remaining index with its recorded failure and completes on the terminal executor (a forced-execution view of the action's own runner).
 * If the forced task is refused too (the executor is shut down), it falls back to {@code generic} and then to the calling thread.
 */
final class RequestDispatcher {
    static final Logger LOGGER = LogManager.getLogger(RequestDispatcher.class);

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final ClusterState clusterState;
    private final ProjectId projectId;
    private final FieldCapabilitiesRequest fieldCapsRequest;
    private final Task parentTask;
    private final OriginalIndices originalIndices;
    private final long nowInMillis;

    private final boolean hasFilter;
    private final boolean shardIsUnit;
    private final TimeValue routingWaitTimeout;
    private final Executor executor;
    private final Executor terminalExecutor;
    private final Consumer<FieldCapabilitiesIndexResponse> onIndexResponse;
    private final BiConsumer<String, Exception> onIndexFailure;
    private final Runnable onComplete;

    private final AtomicInteger pendingRequests = new AtomicInteger();
    private final AtomicInteger executionRound = new AtomicInteger();
    private final AtomicInteger routingWaits = new AtomicInteger();
    private final AtomicBoolean terminated = new AtomicBoolean();
    private final AtomicBoolean cancellationListenerRegistered = new AtomicBoolean();
    private final AtomicReference<RoutingWait> currentWait = new AtomicReference<>();
    private final Map<String, IndexSelector> indexSelectors;
    private final Map<String, DiscoveryNode> laterNodes = ConcurrentCollections.newConcurrentMap();
    // written only by the round loop; volatile because the test accessor reads it from another thread
    private volatile long routingDeadlineMillis = -1L;
    private volatile boolean routingBudgetSpent;

    RequestDispatcher(
        ClusterService clusterService,
        TransportService transportService,
        ProjectResolver projectResolver,
        CoordinatorRewriteContextProvider coordinatorRewriteContextProvider,
        Task parentTask,
        FieldCapabilitiesRequest fieldCapsRequest,
        OriginalIndices originalIndices,
        long nowInMillis,
        String[] indices,
        Executor executor,
        Executor terminalExecutor,
        TimeValue routingWaitTimeout,
        Consumer<FieldCapabilitiesIndexResponse> onIndexResponse,
        BiConsumer<String, Exception> onIndexFailure,
        Runnable onComplete,
        @Nullable FieldCapsCache cache
    ) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.threadPool = transportService.getThreadPool();
        this.fieldCapsRequest = fieldCapsRequest;
        this.parentTask = parentTask;
        this.originalIndices = originalIndices;
        this.nowInMillis = nowInMillis;
        this.clusterState = clusterService.state();
        this.hasFilter = fieldCapsRequest.indexFilter() != null && fieldCapsRequest.indexFilter() instanceof MatchAllQueryBuilder == false;
        this.shardIsUnit = hasFilter || fieldCapsRequest.includeEmptyFields() == false;
        this.executor = executor;
        this.terminalExecutor = terminalExecutor;
        this.routingWaitTimeout = routingWaitTimeout;
        this.onIndexFailure = onIndexFailure;
        this.onComplete = new RunOnce(onComplete);
        this.indexSelectors = ConcurrentCollections.newConcurrentMap();

        ProjectState project = projectResolver.getProjectState(clusterState);
        this.projectId = project.projectId();
        String[] fields = fieldCapsRequest.fields();
        String[] filters = fieldCapsRequest.filters();
        final Map<String, FieldCapsCache.Key> cacheKeys;
        if (cache != null) {
            if (fields.length > 1) {
                fields = fieldCapsRequest.fields().clone();
                Arrays.sort(fields);
            }
            if (filters.length > 1) {
                filters = fieldCapsRequest.filters().clone();
                Arrays.sort(filters);
            }
            cacheKeys = new HashMap<>(indices.length);
            this.onIndexResponse = resp -> {
                if (resp.canMatch()) {
                    FieldCapsCache.Key key = cacheKeys.remove(resp.getIndexName());
                    if (key != null) {
                        cache.put(key, resp);
                    }
                }
                onIndexResponse.accept(resp);
            };
        } else {
            cacheKeys = null;
            this.onIndexResponse = onIndexResponse;
        }
        for (String index : indices) {
            final List<SearchShardRouting> shardIts;
            try {
                shardIts = clusterService.operationRouting().searchShards(project, new String[] { index }, null, null);
            } catch (Exception e) {
                onIndexFailure.accept(index, e);
                continue;
            }
            final IndexSelector indexResult = new IndexSelector(
                fieldCapsRequest.clusterAlias(),
                shardIts,
                fieldCapsRequest.indexFilter(),
                nowInMillis,
                coordinatorRewriteContextProvider
            );
            if (indexResult.nodeToShards.isEmpty() && indexResult.unmatchedShardIds.isEmpty()) {
                onIndexFailure.accept(index, new NoShardAvailableActionException(null, "index [" + index + "] has no active shard copy"));
                continue;
            }
            if (cacheKeys != null) {
                IndexMetadata imd = project.metadata().index(index);
                FieldCapsCache.Key key = new FieldCapsCache.Key(
                    imd.getIndexUUID(),
                    imd.getSettingsVersion(),
                    imd.getMappingVersion(),
                    fields,
                    filters
                );
                FieldCapabilitiesIndexResponse cached = cache.get(key);
                if (cached != null) {
                    onIndexResponse.accept(cached);
                    continue;
                }
                cacheKeys.put(index, key);
            }
            this.indexSelectors.put(index, indexResult);
        }
    }

    void start() {
        if (indexSelectors.isEmpty()) {
            onComplete.run();
            return;
        }
        execute();
    }

    private void execute() {
        executor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                // If we get rejected, mark pending indices as failed and complete
                final List<String> failedIndices = new ArrayList<>(indexSelectors.keySet());
                for (String failedIndex : failedIndices) {
                    final IndexSelector removed = indexSelectors.remove(failedIndex);
                    assert removed != null;
                    onIndexFailure.accept(failedIndex, e);
                }
                onComplete.run();
            }

            @Override
            protected void doRun() {
                innerExecute();
            }
        });
    }

    private void innerExecute() {
        if (terminated.get()) {
            return;
        }
        if (parentTask instanceof CancellableTask cancellableTask && cancellableTask.isCancelled()) {
            onComplete.run();
            return;
        }
        final Map<String, List<ShardId>> nodeToSelectedShards = new HashMap<>();
        assert pendingRequests.get() == 0 : "pending requests = " + pendingRequests;
        final List<String> failedIndices = new ArrayList<>();
        final List<ParkedIndex> stalledIndices = new ArrayList<>();
        boolean stallSeen = false;
        ClusterState applied = null;
        for (Map.Entry<String, IndexSelector> e : indexSelectors.entrySet()) {
            final String index = e.getKey();
            final IndexSelector indexSelector = e.getValue();
            List<ShardRouting> selectedShards = indexSelector.nextTarget(hasFilter);
            if (selectedShards.isEmpty()) {
                if (applied == null) {
                    applied = clusterService.state();
                }
                final Resolution resolution = indexSelector.resolveAgain(applied, projectId, shardIsUnit, laterNodes);
                stallSeen |= resolution.stalled().isEmpty() == false;
                if (LOGGER.isDebugEnabled() && resolution.result() != Exhausted.NOT_ELIGIBLE) {
                    LOGGER.debug(
                        "second resolution for index [{}]: result=[{}], stalled shards={}",
                        index,
                        resolution.result(),
                        resolution.stalled().keySet()
                    );
                }
                switch (resolution.result()) {
                    case QUEUED -> selectedShards = indexSelector.nextTarget(hasFilter);
                    case STALLED -> {
                        stalledIndices.add(new ParkedIndex(index, resolution.stalled()));
                        continue;
                    }
                    case TERMINAL, NOT_ELIGIBLE -> {
                        failedIndices.add(index);
                        continue;
                    }
                }
            }
            assert selectedShards.isEmpty() == false;
            pendingRequests.addAndGet(selectedShards.size());
            for (ShardRouting shard : selectedShards) {
                nodeToSelectedShards.computeIfAbsent(shard.currentNodeId(), n -> new ArrayList<>()).add(shard.shardId());
            }
        }
        for (String failedIndex : failedIndices) {
            final IndexSelector indexSelector = indexSelectors.remove(failedIndex);
            assert indexSelector != null;
            final Exception failure = indexSelector.getFailure();
            if (failure != null) {
                onIndexFailure.accept(failedIndex, failure);
            }
        }
        if (stallSeen && routingDeadlineMillis < 0L) {
            routingDeadlineMillis = threadPool.relativeTimeInMillis() + routingWaitTimeout.millis();
        }
        if (nodeToSelectedShards.isEmpty() == false) {
            for (Map.Entry<String, List<ShardId>> e : nodeToSelectedShards.entrySet()) {
                sendRequestToNode(e.getKey(), e.getValue());
            }
        } else if (stalledIndices.isEmpty()) {
            onComplete.run();
        } else {
            final long remainingMillis = routingBudgetSpent ? 0L : routingDeadlineMillis - threadPool.relativeTimeInMillis();
            if (remainingMillis <= 0L) {
                for (ParkedIndex parked : stalledIndices) {
                    final IndexSelector removed = indexSelectors.remove(parked.index());
                    if (removed != null) {
                        onIndexFailure.accept(parked.index(), removed.getFailure());
                    }
                }
                onComplete.run();
            } else {
                pendingRequests.set(1);
                new RoutingWait(stalledIndices).start(applied, TimeValue.timeValueMillis(remainingMillis));
            }
        }
    }

    // for testing
    int executionRound() {
        return executionRound.get();
    }

    // for testing
    int routingWaits() {
        return routingWaits.get();
    }

    // for testing: the time left before the routing budget expires, or -1 if the deadline has not been set
    long routingDeadlineRemainingMillis() {
        return routingDeadlineMillis < 0L ? -1L : routingDeadlineMillis - threadPool.relativeTimeInMillis();
    }

    private void sendRequestToNode(String nodeId, List<ShardId> shardIds) {
        final DiscoveryNode node = laterNodes.getOrDefault(nodeId, clusterState.nodes().get(nodeId));
        assert node != null;
        LOGGER.debug("round {} sends field caps node request to node {} for shardIds {}", executionRound, node, shardIds);
        final ActionListener<FieldCapabilitiesNodeResponse> listener = ActionListener.wrap(
            response -> onRequestResponse(shardIds, response),
            failure -> onRequestFailure(shardIds, failure)
        );
        final FieldCapabilitiesNodeRequest nodeRequest = new FieldCapabilitiesNodeRequest(
            shardIds,
            fieldCapsRequest.fields(),
            fieldCapsRequest.filters(),
            fieldCapsRequest.types(),
            originalIndices,
            fieldCapsRequest.indexFilter(),
            nowInMillis,
            fieldCapsRequest.runtimeFields(),
            fieldCapsRequest.includeEmptyFields(),
            fieldCapsRequest.includeTimeSeriesStats()
        );
        transportService.sendChildRequest(
            node,
            TransportFieldCapabilitiesAction.ACTION_NODE_NAME,
            nodeRequest,
            parentTask,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(
                ActionListener.runAfter(listener, () -> afterRequestsCompleted(shardIds.size())),
                FieldCapabilitiesNodeResponse::new,
                executor
            )
        );
    }

    private void afterRequestsCompleted(int numRequests) {
        if (pendingRequests.addAndGet(-numRequests) == 0) {
            // Here we only retry after all pending requests have responded to avoid exploding network requests
            // when the cluster is unstable or overloaded as an eager retry approach can add more load to the cluster.
            executionRound.incrementAndGet();
            execute();
        }
    }

    private void onRequestResponse(List<ShardId> requestedShardIds, FieldCapabilitiesNodeResponse nodeResponse) {
        for (FieldCapabilitiesIndexResponse indexResponse : nodeResponse.getIndexResponses()) {
            if (indexResponse.canMatch()) {
                if (fieldCapsRequest.includeEmptyFields() == false) {
                    // we accept all the responses because they may vary from node to node if we exclude empty fields
                    onIndexResponse.accept(indexResponse);
                } else if (indexSelectors.remove(indexResponse.getIndexName()) != null) {
                    onIndexResponse.accept(indexResponse);
                }
            }
        }
        for (ShardId unmatchedShardId : nodeResponse.getUnmatchedShardIds()) {
            final IndexSelector indexSelector = indexSelectors.get(unmatchedShardId.getIndexName());
            if (indexSelector != null) {
                indexSelector.addUnmatchedShardId(unmatchedShardId);
            }
        }
        for (Map.Entry<ShardId, Exception> e : nodeResponse.getFailures().entrySet()) {
            final IndexSelector indexSelector = indexSelectors.get(e.getKey().getIndexName());
            if (indexSelector != null) {
                indexSelector.setFailure(e.getKey(), e.getValue());
            }
        }
        if (shardIsUnit) {
            for (ShardId shardId : requestedShardIds) {
                if (nodeResponse.getFailures().containsKey(shardId) == false
                    && nodeResponse.getUnmatchedShardIds().contains(shardId) == false) {
                    final IndexSelector indexSelector = indexSelectors.get(shardId.getIndexName());
                    if (indexSelector != null) {
                        indexSelector.markAnswered(shardId);
                    }
                }
            }
        }
    }

    private void onRequestFailure(List<ShardId> shardIds, Exception e) {
        for (ShardId shardId : shardIds) {
            final IndexSelector indexSelector = indexSelectors.get(shardId.getIndexName());
            if (indexSelector != null) {
                indexSelector.setFailure(shardId, e);
            }
        }
    }

    private static FieldCapsCache.Key cacheKey(IndexMetadata indexMetadata, String[] fields, String[] filters) {
        return new FieldCapsCache.Key(
            indexMetadata.getIndexUUID(),
            indexMetadata.getSettingsVersion(),
            indexMetadata.getMappingVersion(),
            fields,
            filters
        );
    }

    /** Returns whether the failure means that this particular shard copy is currently unavailable. */
    static boolean isCopyUnavailable(Exception e) {
        return TransportActions.isShardNotAvailableException(e)
            || ExceptionsHelper.unwrap(e, ConnectTransportException.class, NodeClosedException.class) != null;
    }

    /** Classification of routing for a shard after its initially resolved copies are exhausted. */
    enum Routing {
        UNTRIED_COPY,
        NONE_YET,
        NO_ACTIVE_COPY,
        GONE
    }

    /**
     * Finds the copies that a newer cluster state offers for an exhausted shard. This method is safe to call from a cluster-state
     * predicate: it only uses null-returning routing accessors, does not consult thread context, and does not throw for missing routing.
     */
    static Routing untriedCopies(
        ClusterState state,
        ProjectId projectId,
        ShardId shardId,
        Map<String, Boolean> tried,
        List<ShardRouting> into
    ) {
        final RoutingTable routingTable = state.globalRoutingTable().routingTables().get(projectId);
        if (routingTable == null) {
            return Routing.GONE;
        }
        final IndexRoutingTable indexRoutingTable = routingTable.index(shardId.getIndex());
        if (indexRoutingTable == null) {
            return Routing.GONE;
        }
        final IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId.id());
        if (shardRoutingTable == null) {
            return Routing.GONE;
        }
        boolean hasActiveSearchableCopy = false;
        for (ShardRouting shardRouting : shardRoutingTable.activeShards()) {
            if (shardRouting.isSearchable()) {
                hasActiveSearchableCopy = true;
                addIfUntried(state, tried, into, shardRouting);
            }
        }
        if (hasActiveSearchableCopy == false) {
            return Routing.NO_ACTIVE_COPY;
        }
        for (ShardRouting shardRouting : shardRoutingTable.getAllInitializingShards()) {
            if (shardRouting.isSearchable()) {
                addIfUntried(state, tried, into, shardRouting);
            }
        }
        return into.isEmpty() ? Routing.NONE_YET : Routing.UNTRIED_COPY;
    }

    /** Adds a reachable routing when its allocation was not tried in the same state, or became active since it was tried. */
    private static void addIfUntried(ClusterState state, Map<String, Boolean> tried, List<ShardRouting> into, ShardRouting shardRouting) {
        if (state.nodes().get(shardRouting.currentNodeId()) == null) {
            return;
        }
        final Boolean activeWhenTried = tried.get(shardRouting.allocationId().getId());
        if (activeWhenTried == null || activeWhenTried == false && shardRouting.active()) {
            into.add(shardRouting);
        }
    }

    /** Result of trying to resolve all eligible exhausted units of an index. */
    private enum Exhausted {
        NOT_ELIGIBLE,
        QUEUED,
        STALLED,
        TERMINAL
    }

    /** The result of resolving an index's exhausted units, with the shards that answered NONE_YET. */
    private record Resolution(Exhausted result, Map<ShardId, Map<String, Boolean>> stalled) {}

    /** Immutable routing predicate input for one index parked between dispatcher rounds. */
    private record ParkedIndex(String index, Map<ShardId, Map<String, Boolean>> stalled) {}

    /** Why a routing wait ended. Logged, and TIMEOUT marks the budget as spent. */
    private enum End {
        NEW_STATE,
        TIMEOUT,
        CANCELLED,
        SERVICE_CLOSED,
        START_FAILED
    }

    private static class IndexSelector {
        private final Map<String, List<ShardRouting>> nodeToShards = new HashMap<>();
        private final Set<ShardId> unmatchedShardIds = new HashSet<>();
        private final Map<ShardId, Exception> failures = new HashMap<>();
        /** Copies handed out by nextTarget: allocation id -> whether the routing showed the copy active when it was tried. */
        private final Map<ShardId, Map<String, Boolean>> tried = new HashMap<>();
        /** Shards with a failure that is not an unavailable copy. They are never resolved again. */
        private final Set<ShardId> failedOtherwise = new HashSet<>();
        /** Shards a node answered for, with a response or as unmatched. Kept only when the shard is the unit. */
        private final Set<ShardId> answered = new HashSet<>();
        /**
         * Shards whose second resolution is done and that are not resolved a third time. On the shard unit: a shard that was given
         * copies, or was found terminal. On the index unit: every failed shard of the index, once the index was given copies; a
         * terminal shard alone does not enter the set, so the index stays eligible.
         */
        private final Set<ShardId> resolvedAgain = new HashSet<>();

        IndexSelector(
            String clusterAlias,
            List<SearchShardRouting> shards,
            QueryBuilder indexFilter,
            long nowInMillis,
            CoordinatorRewriteContextProvider coordinatorRewriteContextProvider
        ) {
            for (SearchShardRouting searchShardRouting : shards) {
                boolean canMatch = true;
                final ShardId shardId = searchShardRouting.shardId();
                if (indexFilter != null && indexFilter instanceof MatchAllQueryBuilder == false) {
                    var coordinatorRewriteContext = coordinatorRewriteContextProvider.getCoordinatorRewriteContext(shardId.getIndex());
                    if (coordinatorRewriteContext != null) {
                        var shardRequest = new ShardSearchRequest(
                            shardId,
                            nowInMillis,
                            AliasFilter.EMPTY,
                            clusterAlias,
                            searchShardRouting.splitShardCountSummary()
                        );
                        shardRequest.source(new SearchSourceBuilder().query(indexFilter));
                        try {
                            canMatch = SearchService.queryStillMatchesAfterRewrite(shardRequest, coordinatorRewriteContext);
                        } catch (Exception e) {
                            // treat as if shard is still a potential match
                        }
                    }
                }
                if (canMatch) {
                    for (ShardRouting shard : searchShardRouting) {
                        nodeToShards.computeIfAbsent(shard.currentNodeId(), node -> new ArrayList<>()).add(shard);
                    }
                } else {
                    unmatchedShardIds.add(shardId);
                }
            }
        }

        synchronized Exception getFailure() {
            Exception first = null;
            for (Exception e : failures.values()) {
                first = useOrSuppressIfDifferent(first, e);
            }
            return first;
        }

        static Exception useOrSuppressIfDifferent(Exception first, Exception second) {
            if (first == null) {
                return second;
            } else if (ExceptionsHelper.unwrap(first) != ExceptionsHelper.unwrap(second)) {
                first.addSuppressed(second);
            }
            return first;
        }

        synchronized void setFailure(ShardId shardId, Exception failure) {
            assert unmatchedShardIds.contains(shardId) == false : "Shard " + shardId + " was unmatched already";
            failures.compute(shardId, (k, curr) -> useOrSuppressIfDifferent(curr, failure));
            if (isCopyUnavailable(failure) == false) {
                failedOtherwise.add(shardId);
            }
        }

        synchronized void addUnmatchedShardId(ShardId shardId) {
            final boolean added = unmatchedShardIds.add(shardId);
            assert added : "Shard " + shardId + " was unmatched already";
            failures.remove(shardId);
            answered.add(shardId);
        }

        synchronized void markAnswered(ShardId shardId) {
            answered.add(shardId);
        }

        synchronized List<ShardRouting> nextTarget(boolean withQueryFilter) {
            if (nodeToShards.isEmpty()) {
                return Collections.emptyList();
            }
            final Iterator<Map.Entry<String, List<ShardRouting>>> nodeIt = nodeToShards.entrySet().iterator();
            if (withQueryFilter) {
                // If an index filter is specified, then we must reach out to all of an index's shards to check
                // if one of them can match. Otherwise, for efficiency we just reach out to one of its shards.
                final List<ShardRouting> selectedShards = new ArrayList<>();
                final Set<ShardId> selectedShardIds = new HashSet<>();
                while (nodeIt.hasNext()) {
                    final List<ShardRouting> shards = nodeIt.next().getValue();
                    final Iterator<ShardRouting> shardIt = shards.iterator();
                    while (shardIt.hasNext()) {
                        final ShardRouting shard = shardIt.next();
                        if (unmatchedShardIds.contains(shard.shardId())) {
                            shardIt.remove();
                            continue;
                        }
                        if (selectedShardIds.add(shard.shardId())) {
                            shardIt.remove();
                            selectedShards.add(shard);
                        }
                    }
                    if (shards.isEmpty()) {
                        nodeIt.remove();
                    }
                }
                recordTried(selectedShards);
                return selectedShards;
            } else {
                assert unmatchedShardIds.isEmpty();
                final Map.Entry<String, List<ShardRouting>> node = nodeIt.next();
                nodeIt.remove();
                recordTried(node.getValue());
                return node.getValue();
            }
        }

        private void recordTried(List<ShardRouting> selectedShards) {
            for (ShardRouting shard : selectedShards) {
                tried.computeIfAbsent(shard.shardId(), ignored -> new HashMap<>()).put(shard.allocationId().getId(), shard.active());
            }
        }

        /**
         * Resolves exhausted units against the applied state and immediately queues every untried copy it finds.
         */
        synchronized Resolution resolveAgain(
            ClusterState applied,
            ProjectId projectId,
            boolean shardIsUnit,
            Map<String, DiscoveryNode> laterNodes
        ) {
            final List<ShardId> candidates = new ArrayList<>();
            if (shardIsUnit) {
                for (ShardId shardId : failures.keySet()) {
                    if (answered.contains(shardId) == false
                        && failedOtherwise.contains(shardId) == false
                        && resolvedAgain.contains(shardId) == false) {
                        candidates.add(shardId);
                    }
                }
            } else {
                if (failedOtherwise.isEmpty() == false || resolvedAgain.isEmpty() == false) {
                    return new Resolution(Exhausted.NOT_ELIGIBLE, Map.of());
                }
                candidates.addAll(failures.keySet());
            }
            if (candidates.isEmpty()) {
                return new Resolution(Exhausted.NOT_ELIGIBLE, Map.of());
            }

            final Map<ShardId, Map<String, Boolean>> newlyStalled = new HashMap<>();
            final Map<ShardId, List<ShardRouting>> found = new HashMap<>();
            int terminal = 0;
            for (ShardId shardId : candidates) {
                final List<ShardRouting> copies = new ArrayList<>();
                final Map<String, Boolean> triedCopies = tried.getOrDefault(shardId, Map.of());
                final Routing routing = untriedCopies(applied, projectId, shardId, triedCopies, copies);
                LOGGER.debug("second resolution of shard {}: [{}]", shardId, routing);
                switch (routing) {
                    case UNTRIED_COPY -> found.put(shardId, copies);
                    case NONE_YET -> newlyStalled.put(shardId, Map.copyOf(triedCopies));
                    case NO_ACTIVE_COPY, GONE -> {
                        if (shardIsUnit) {
                            resolvedAgain.add(shardId);
                        }
                        terminal++;
                    }
                }
            }

            if (found.isEmpty() == false) {
                for (List<ShardRouting> copies : found.values()) {
                    for (ShardRouting copy : copies) {
                        nodeToShards.computeIfAbsent(copy.currentNodeId(), ignored -> new ArrayList<>()).add(copy);
                        laterNodes.put(copy.currentNodeId(), applied.nodes().get(copy.currentNodeId()));
                    }
                }
                if (shardIsUnit) {
                    resolvedAgain.addAll(found.keySet());
                    return new Resolution(Exhausted.QUEUED, Map.copyOf(newlyStalled));
                } else {
                    resolvedAgain.addAll(candidates);
                    return new Resolution(Exhausted.QUEUED, Map.of());
                }
            }
            if (newlyStalled.isEmpty() == false) {
                return new Resolution(Exhausted.STALLED, Map.copyOf(newlyStalled));
            }
            assert terminal == candidates.size();
            return new Resolution(Exhausted.TERMINAL, Map.of());
        }
    }

    /**
     * Waits until routing for one parked unit changes, while keeping the wait itself as the round's only pending request.
     */
    private final class RoutingWait {
        private final List<ParkedIndex> parked;
        private final AtomicBoolean done = new AtomicBoolean();
        private final ObserverCallback callback = new ObserverCallback(this);
        private final AbstractRunnable resume;
        private final Supplier<ThreadContext.StoredContext> requestContext;
        private volatile Scheduler.Cancellable timer;

        private RoutingWait(List<ParkedIndex> parked) {
            this.parked = List.copyOf(parked);
            this.requestContext = threadPool.getThreadContext().newRestorableContext(false);
            final AbstractRunnable unwrappedResume = new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    try (ThreadContext.StoredContext ignored = requestContext.get()) {
                        terminate(e);
                    }
                }

                @Override
                protected void doRun() {
                    afterRequestsCompleted(1);
                }
            };
            // AbstractRunnable: preserveContext wraps an AbstractRunnable in an AbstractRunnable.
            this.resume = (AbstractRunnable) threadPool.getThreadContext().preserveContext(unwrappedResume);
        }

        void start(ClusterState resolvedOn, TimeValue remaining) {
            routingWaits.incrementAndGet();
            currentWait.set(this);
            if (parentTask instanceof CancellableTask cancellableTask) {
                if (cancellationListenerRegistered.compareAndSet(false, true)) {
                    cancellableTask.addListener(() -> {
                        final RoutingWait wait = currentWait.get();
                        if (wait != null) {
                            wait.finish(End.CANCELLED);
                        }
                    });
                }
                if (cancellableTask.isCancelled()) {
                    finish(End.CANCELLED);
                    return;
                }
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "waiting up to [{}] for newer field caps routing for indices {}",
                    remaining,
                    parked.stream().map(ParkedIndex::index).toList()
                );
            }
            try {
                timer = threadPool.schedule(() -> finish(End.TIMEOUT), remaining, threadPool.generic());
                if (done.get()) {
                    timer.cancel();
                }
                final ClusterStateObserver observer = new ClusterStateObserver(
                    resolvedOn,
                    clusterService,
                    remaining,
                    LOGGER,
                    threadPool.getThreadContext()
                );
                observer.waitForNextChange(callback, callback);
            } catch (Exception e) {
                finish(End.START_FAILED);
            }
        }

        private boolean wakes(ClusterState state) {
            for (ParkedIndex parkedIndex : parked) {
                boolean anyUntried = false;
                boolean anyStalled = false;
                for (Map.Entry<ShardId, Map<String, Boolean>> stalledShard : parkedIndex.stalled().entrySet()) {
                    final Routing routing = untriedCopies(
                        state,
                        projectId,
                        stalledShard.getKey(),
                        stalledShard.getValue(),
                        new ArrayList<>()
                    );
                    anyUntried |= routing == Routing.UNTRIED_COPY;
                    anyStalled |= routing == Routing.NONE_YET;
                    if (shardIsUnit && routing != Routing.NONE_YET) {
                        return true;
                    }
                }
                if (shardIsUnit == false && (anyUntried || anyStalled == false)) {
                    return true;
                }
            }
            return false;
        }

        void finish(End cause) {
            if (done.compareAndSet(false, true)) {
                if (cause == End.TIMEOUT) {
                    routingBudgetSpent = true;
                }
                LOGGER.debug("routing wait ended: [{}]", cause);
                callback.wait.set(null);
                currentWait.compareAndSet(this, null);
                final Scheduler.Cancellable localTimer = timer;
                if (localTimer != null) {
                    localTimer.cancel();
                }
                executor.execute(resume);
            }
        }
    }

    /** Observer callback that does not retain an ended wait until the observer unregisters itself. */
    private static final class ObserverCallback implements ClusterStateObserver.Listener, Predicate<ClusterState> {
        private final AtomicReference<RoutingWait> wait;

        private ObserverCallback(RoutingWait wait) {
            this.wait = new AtomicReference<>(wait);
        }

        @Override
        public void onNewClusterState(ClusterState state) {
            finish(End.NEW_STATE);
        }

        @Override
        public void onClusterServiceClose() {
            finish(End.SERVICE_CLOSED);
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            finish(End.TIMEOUT);
        }

        @Override
        public boolean test(ClusterState state) {
            final RoutingWait routingWait = wait.get();
            if (routingWait == null) {
                return true;
            }
            try {
                return routingWait.wakes(state);
            } catch (Exception e) {
                return true;
            }
        }

        private void finish(End cause) {
            final RoutingWait routingWait = wait.get();
            if (routingWait != null) {
                routingWait.finish(cause);
            }
        }
    }

    /**
     * Ends the dispatch after a rejected wait hand-off. No node request is outstanding while waiting, so this path can safely complete
     * without releasing the pending wait slot or starting another round.
     */
    private void terminate(Exception rejection) {
        if (terminated.compareAndSet(false, true) == false) {
            return;
        }
        LOGGER.warn("terminating field caps dispatch after a rejected routing-wait hand-off", rejection);
        final Runnable body = new RunOnce(() -> {
            final List<String> failedIndices = new ArrayList<>(indexSelectors.keySet());
            for (String failedIndex : failedIndices) {
                final IndexSelector removed = indexSelectors.remove(failedIndex);
                if (removed != null) {
                    final Exception failure = removed.getFailure();
                    onIndexFailure.accept(failedIndex, failure == null ? rejection : failure);
                }
            }
            onComplete.run();
        });
        terminalExecutor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                // Forced task refused: the executor is shut down. Try generic, then inline as last resort.
                try {
                    threadPool.generic().execute(body);
                } catch (Exception ignored) {
                    body.run();
                }
            }

            @Override
            protected void doRun() {
                body.run();
            }
        });
    }
}
