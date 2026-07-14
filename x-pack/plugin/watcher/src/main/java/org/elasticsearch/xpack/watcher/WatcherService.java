/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.NotMultiProjectCapable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.upgrade.UpgradeField;
import org.elasticsearch.xpack.core.watcher.WatcherState;
import org.elasticsearch.xpack.core.watcher.execution.TriggeredWatchStoreField;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.execution.ExecutionService;
import org.elasticsearch.xpack.watcher.execution.TriggeredWatch;
import org.elasticsearch.xpack.watcher.execution.TriggeredWatchStore;
import org.elasticsearch.xpack.watcher.history.HistoryStore;
import org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.watch.WatchParser;
import org.elasticsearch.xpack.watcher.watch.WatchStoreUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;
import static org.elasticsearch.xpack.core.watcher.support.Exceptions.illegalState;
import static org.elasticsearch.xpack.core.watcher.watch.Watch.INDEX;

public class WatcherService implements WatcherEventConsumer {

    private static final String LIFECYCLE_THREADPOOL_NAME = "watcher-lifecycle";
    private static final Logger logger = LogManager.getLogger(WatcherService.class);

    private final TriggerService triggerService;
    private final TriggeredWatchStore triggeredWatchStore;
    private final ExecutionService executionService;
    private final TimeValue scrollTimeout;
    private final int scrollSize;
    private final WatchParser parser;
    private final Client client;
    private final TimeValue defaultSearchTimeout;
    private final ExecutorService executor;
    private final Map<String, Watch> pendingWatches = new HashMap<>();
    private final AtomicReference<WatcherState> state = new AtomicReference<>(WatcherState.STARTED);
    private final AtomicReference<DesiredState> desiredState = new AtomicReference<>();
    private final AtomicBoolean reconciliationScheduled = new AtomicBoolean();
    private List<ShardRouting> appliedShardRoutings;

    /** The latest lifecycle outcome requested by {@link WatcherLifeCycleService}. */
    private sealed interface DesiredState permits Running, Paused, Stopped, Shutdown {}

    private record Running(ClusterState clusterState, List<ShardRouting> affectedShardRoutings, String reason) implements DesiredState {}

    private record Paused(String reason) implements DesiredState {}

    private record Stopped(String reason) implements DesiredState {}

    private record Shutdown() implements DesiredState {}

    private enum ReconcileResult {
        COMPLETE,
        WAITING_FOR_STOP
    }

    WatcherService(
        Settings settings,
        TriggerService triggerService,
        TriggeredWatchStore triggeredWatchStore,
        ExecutionService executionService,
        WatchParser parser,
        Client client,
        ExecutorService executor
    ) {
        this.triggerService = triggerService;
        this.triggeredWatchStore = triggeredWatchStore;
        this.executionService = executionService;
        this.scrollTimeout = settings.getAsTime("xpack.watcher.watch.scroll.timeout", TimeValue.timeValueSeconds(30));
        this.scrollSize = settings.getAsInt("xpack.watcher.watch.scroll.size", 100);
        this.defaultSearchTimeout = settings.getAsTime("xpack.watcher.internal.ops.search.default_timeout", TimeValue.timeValueSeconds(30));
        this.parser = parser;
        this.client = ClientHelper.clientWithOrigin(client, WATCHER_ORIGIN);
        this.executor = executor;
    }

    WatcherService(
        Settings settings,
        TriggerService triggerService,
        TriggeredWatchStore triggeredWatchStore,
        ExecutionService executionService,
        WatchParser parser,
        Client client
    ) {
        this(
            settings,
            triggerService,
            triggeredWatchStore,
            executionService,
            parser,
            client,
            EsExecutors.newFixed(
                LIFECYCLE_THREADPOOL_NAME,
                1,
                1000,
                daemonThreadFactory(settings, LIFECYCLE_THREADPOOL_NAME),
                client.threadPool().getThreadContext(),
                EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
            )
        );
    }

    /**
     * Ensure that watcher can be reloaded, by checking if all indices are marked as up and ready in the cluster state
     * @param state The current cluster state
     * @return true if everything is good to go, so that the service can be started
     */
    public boolean validate(ClusterState state) {
        @NotMultiProjectCapable(description = "Watcher is not available in serverless")
        ProjectId projectId = ProjectId.DEFAULT;
        IndexMetadata watcherIndexMetadata = WatchStoreUtils.getConcreteIndex(Watch.INDEX, state.metadata());
        IndexMetadata triggeredWatchesIndexMetadata = WatchStoreUtils.getConcreteIndex(
            TriggeredWatchStoreField.INDEX_NAME,
            state.metadata()
        );
        boolean isIndexInternalFormatWatchIndex = watcherIndexMetadata == null
            || UpgradeField.checkInternalIndexFormat(watcherIndexMetadata);
        boolean isIndexInternalFormatTriggeredWatchIndex = triggeredWatchesIndexMetadata == null
            || UpgradeField.checkInternalIndexFormat(triggeredWatchesIndexMetadata);
        if (isIndexInternalFormatTriggeredWatchIndex == false || isIndexInternalFormatWatchIndex == false) {
            logger.warn(
                "not starting watcher, upgrade API run required: .watches[{}], .triggered_watches[{}]",
                isIndexInternalFormatWatchIndex,
                isIndexInternalFormatTriggeredWatchIndex
            );
            return false;
        }

        try {
            boolean storesValid = TriggeredWatchStore.validate(state) && HistoryStore.validate(state);
            if (storesValid == false) {
                return false;
            }

            return watcherIndexMetadata == null
                || (watcherIndexMetadata.getState() == IndexMetadata.State.OPEN
                    && state.routingTable(projectId).index(watcherIndexMetadata.getIndex()).allPrimaryShardsActive());
        } catch (IllegalStateException e) {
            logger.warn("Validation error: cannot start watcher", e);
            return false;
        }
    }

    /** Requests that Watcher stop after its current executions finish. */
    void setDesiredStopped(String reason) {
        setDesiredState(new Stopped(reason));
    }

    /** Requests terminal shutdown and then drains the lifecycle executor. */
    void setDesiredShutdown() {
        if (setDesiredState(new Shutdown())) {
            stopExecutor();
        }
    }

    void stopExecutor() {
        ThreadPool.terminate(executor, 10L, TimeUnit.SECONDS);
    }

    /** Requests that Watcher run using the routing information in {@code state}. */
    void setDesiredRunning(ClusterState state, List<ShardRouting> affectedShardRoutings, String reason) {
        final DesiredState currentDesiredState = desiredState.get();
        if (currentDesiredState instanceof Running running && running.affectedShardRoutings().equals(affectedShardRoutings)) {
            return;
        }
        setDesiredState(new Running(state, List.copyOf(affectedShardRoutings), reason));
    }

    /**
     * Reconciles the current Watcher state with the latest requested state. All mutations of the trigger and execution services made
     * during start, reload, pause, and stop are serialized on the lifecycle executor.
     */
    private void reconcile() {
        assert ThreadPool.assertCurrentThreadPool(LIFECYCLE_THREADPOOL_NAME)
            : "reconcile must run on the single threaded [" + LIFECYCLE_THREADPOOL_NAME + "] thread pool";
        DesiredState reconciledState = null;
        ReconcileResult result;
        try {
            while (true) {
                reconciledState = desiredState.get();
                if (reconciledState == null) {
                    return;
                }
                result = switch (reconciledState) {
                    case Running running -> reconcileRunning(running);
                    case Paused paused -> reconcilePaused(paused);
                    case Stopped stopped -> reconcileStopped(stopped);
                    case Shutdown ignored -> reconcileShutdown();
                };
                if (result == ReconcileResult.WAITING_FOR_STOP || desiredState.get() == reconciledState) {
                    return;
                }
            }
        } finally {
            reconciliationScheduled.set(false);
            if (desiredState.get() != reconciledState) {
                scheduleReconciliation();
            }
        }
    }

    private ReconcileResult reconcileRunning(Running running) {
        if (state.get() == WatcherState.STOPPING) {
            return ReconcileResult.WAITING_FOR_STOP;
        }

        final boolean starting = state.get() != WatcherState.STARTED;
        if (starting == false && running.affectedShardRoutings().equals(appliedShardRoutings)) {
            return ReconcileResult.COMPLETE;
        }
        if (starting) {
            validateTransitionAndApplyNewState(WatcherState.STARTING);
        } else {
            final boolean hasValidWatcherTemplates = WatcherIndexTemplateRegistry.validate(running.clusterState());
            if (hasValidWatcherTemplates == false) {
                logger.warn("missing watcher index templates");
            }
            triggerService.pauseExecution();
            final int cancelledTaskCount = executionService.clearExecutionsAndQueue(() -> {});
            logger.info("reloading watcher, reason [{}], cancelled [{}] queued tasks", running.reason(), cancelledTaskCount);
        }

        try {
            final Collection<Watch> watches = loadWatches(running.clusterState());
            final Collection<TriggeredWatch> triggeredWatches;
            if (starting && desiredState.get() == running) {
                triggeredWatches = triggeredWatchStore.findTriggeredWatches(watches, running.clusterState());
            } else {
                triggeredWatches = Collections.emptyList();
            }

            if (desiredState.get() != running) {
                return ReconcileResult.COMPLETE;
            }

            executionService.unPause();
            triggerService.start(watches);
            addPendingWatches(running.clusterState());
            if (triggeredWatches.isEmpty() == false) {
                executionService.executeTriggeredWatches(triggeredWatches);
            }
            appliedShardRoutings = running.affectedShardRoutings();
            validateTransitionAndApplyNewState(WatcherState.STARTED);
            logger.debug("watch service has been reloaded, reason [{}]", running.reason());
        } catch (Exception e) {
            logger.error(starting ? "error starting watcher" : "error reloading watcher", e);
            if (desiredState.get() == running) {
                if (starting) {
                    validateTransitionAndApplyNewState(WatcherState.STOPPED);
                }
                desiredState.compareAndSet(running, null);
            }
        }
        return ReconcileResult.COMPLETE;
    }

    private ReconcileResult reconcilePaused(Paused paused) {
        if (state.get() == WatcherState.STOPPING) {
            return ReconcileResult.WAITING_FOR_STOP;
        }
        triggerService.pauseExecution();
        final int cancelledTaskCount = executionService.pause(() -> {});
        appliedShardRoutings = null;
        validateTransitionAndApplyNewState(WatcherState.STARTED);
        logger.info("paused watch execution, reason [{}], cancelled [{}] queued tasks", paused.reason(), cancelledTaskCount);
        return ReconcileResult.COMPLETE;
    }

    private ReconcileResult reconcileStopped(Stopped stopped) {
        appliedShardRoutings = null;
        if (state.get() == WatcherState.STOPPED) {
            return ReconcileResult.COMPLETE;
        }
        if (state.get() == WatcherState.STOPPING) {
            return ReconcileResult.WAITING_FOR_STOP;
        }
        validateTransitionAndApplyNewState(WatcherState.STOPPING);
        logger.info("stopping watch service, reason [{}]", stopped.reason());
        triggerService.pauseExecution();
        executionService.pause(() -> notifyStopComplete(stopped));
        return ReconcileResult.WAITING_FOR_STOP;
    }

    private void notifyStopComplete(Stopped stopped) {
        try {
            executor.execute(wrapWatcherService(() -> completeStop(stopped), e -> logger.error("error stopping watcher", e)));
        } catch (RuntimeException e) {
            if (executor.isShutdown() == false) {
                throw e;
            }
            logger.debug("watcher lifecycle executor shut down before stop completion was processed");
        }
    }

    private void completeStop(Stopped stopped) {
        state.compareAndSet(WatcherState.STOPPING, WatcherState.STOPPED);
        logger.info("watcher has stopped");
        if (desiredState.get() != stopped) {
            scheduleReconciliation();
        }
    }

    private ReconcileResult reconcileShutdown() {
        validateTransitionAndApplyNewState(WatcherState.STOPPING);
        appliedShardRoutings = null;
        logger.info("stopping watch service, reason [shutdown initiated]");
        triggerService.stop();
        executionService.pause(() -> {
            validateTransitionAndApplyNewState(WatcherState.STOPPED);
            logger.info("watcher has stopped and shutdown");
        });
        return ReconcileResult.COMPLETE;
    }

    private boolean setDesiredState(DesiredState newState) {
        while (true) {
            final DesiredState currentState = desiredState.get();
            if (currentState instanceof Shutdown || newState.equals(currentState)) {
                return false;
            }
            if (desiredState.compareAndSet(currentState, newState)) {
                scheduleReconciliation();
                return true;
            }
        }
    }

    private void scheduleReconciliation() {
        if (reconciliationScheduled.compareAndSet(false, true)) {
            executor.execute(wrapWatcherService(this::reconcile, e -> {
                reconciliationScheduled.set(false);
                logger.error("error reconciling watcher lifecycle", e);
            }));
        }
    }

    /** Requests that scheduled execution pause while manual execution remains available. */
    void setDesiredPaused(String reason) {
        setDesiredState(new Paused(reason));
    }

    /** Returns Watcher's reconciled lifecycle state. */
    public WatcherState getState() {
        return state.get();
    }

    /**
     * This reads all watches from the .watches index/alias and puts them into memory for a short period of time,
     * before they are fed into the trigger service.
     */
    private Collection<Watch> loadWatches(ClusterState clusterState) {
        IndexMetadata indexMetadata = WatchStoreUtils.getConcreteIndex(INDEX, clusterState.metadata());
        // no index exists, all good, we can start
        if (indexMetadata == null) {
            return Collections.emptyList();
        }

        SearchResponse response = null;
        List<Watch> watches = new ArrayList<>();
        try {
            refreshWatches(indexMetadata);

            final Map<ShardId, ShardAllocationConfiguration> shardConfigs = shardAllocationConfigs(clusterState, indexMetadata);
            if (shardConfigs == null) {
                return List.of(); // no shard configs means the index is not yet ready, so we can't load watches yet'
            }

            SearchRequest searchRequest = new SearchRequest(INDEX).scroll(scrollTimeout)
                .preference(Preference.ONLY_LOCAL.toString())
                .source(new SearchSourceBuilder().size(scrollSize).sort(SortBuilders.fieldSort("_doc")).seqNoAndPrimaryTerm(true));
            response = client.search(searchRequest).actionGet(defaultSearchTimeout);

            if (response.getTotalShards() != response.getSuccessfulShards()) {
                throw new ElasticsearchException("Partial response while loading watches");
            }

            while (response.getHits().getHits().length != 0) {
                for (SearchHit hit : response.getHits()) {
                    final ShardAllocationConfiguration shardConfig = shardConfigs.get(hit.getShard().getShardId());
                    if (shardConfig == null || shardConfig.hostsWatch(hit.getId()) == false) {
                        continue;
                    }
                    String id = hit.getId();
                    try {
                        Watch watch = parser.parse(id, true, hit.getSourceRef(), XContentType.JSON, hit.getSeqNo(), hit.getPrimaryTerm());
                        if (watch.status().state().isActive()) {
                            watches.add(watch);
                        }
                    } catch (Exception e) {
                        logger.error(() -> "couldn't load watch [" + id + "], ignoring it...", e);
                    }
                }
                SearchScrollRequest request = new SearchScrollRequest(response.getScrollId());
                request.scroll(scrollTimeout);
                response.decRef();
                response = client.searchScroll(request).actionGet(defaultSearchTimeout);
            }
        } finally {
            if (response != null) {
                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                clearScrollRequest.addScrollId(response.getScrollId());
                response.decRef();
                client.clearScroll(clearScrollRequest).actionGet(scrollTimeout);
            }
        }

        logger.debug("Loaded [{}] watches for execution", watches.size());

        return watches;
    }

    private static Map<ShardId, ShardAllocationConfiguration> shardAllocationConfigs(ClusterState state, IndexMetadata indexMetadata) {
        // find out local shards
        final RoutingNode routingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        // yes, this can happen, if the state is not recovered
        if (routingNode == null) {
            return null;
        }

        final String watchIndexName = indexMetadata.getIndex().getName();
        final List<ShardRouting> localShards = routingNode.shardsWithState(watchIndexName, RELOCATING, STARTED).toList();

        @NotMultiProjectCapable(description = "Watcher is not available in serverless")
        final IndexRoutingTable indexRoutingTable = state.routingTable(ProjectId.DEFAULT).index(watchIndexName);
        return ShardAllocationConfiguration.forLocalShards(localShards, indexRoutingTable);
    }

    // visible for testing
    void addPendingWatches(ClusterState state) {
        final IndexMetadata indexMetadata = WatchStoreUtils.getConcreteIndex(INDEX, state.metadata());
        if (indexMetadata == null) {
            // no index means there is nothing to schedule against; drop anything that was buffered
            synchronized (pendingWatches) {
                pendingWatches.clear();
            }
            return;
        }
        final Map<ShardId, ShardAllocationConfiguration> shardConfigs = shardAllocationConfigs(state, indexMetadata);
        if (shardConfigs == null) {
            // routing not yet recovered on this node — keep pending watches around for the next reload
            return;
        }
        final int numShards = indexMetadata.getNumberOfShards();
        synchronized (pendingWatches) {
            for (Watch pendingWatch : pendingWatches.values()) {
                final ShardAllocationConfiguration shardConfig = ShardAllocationConfiguration.findShardConfig(
                    shardConfigs,
                    pendingWatch.id(),
                    numShards
                );
                if (shardConfig == null || shardConfig.hostsWatch(pendingWatch.id()) == false) {
                    continue;
                }
                if (pendingWatch.status().state().isActive()) {
                    /// We ignore the return value deliberately. If the engine pauses during this operation,
                    /// the [#loadWatches(ClusterState)] will bring them back
                    triggerService.add(pendingWatch);
                }
            }
            pendingWatches.clear();
        }
    }

    /// Atomically tries to schedule an active watch on the trigger engine and, only if the engine refused (it is
    /// paused between `pauseExecution` and `start`), retains the watch in the pending-watches map so the next reload
    /// picks it up. When the engine accepts the watch immediately, no pending entry is needed — the next reload will
    /// reload it from the index search anyway. Both branches happen under the same lock in [WatcherService], so a
    /// concurrent [#onWatchRemoved] cannot interleave between the engine call and the pending update and leave the
    /// two views inconsistent.
    @Override
    public void onWatchAdded(Watch watch) {
        synchronized (pendingWatches) {
            if (triggerService.add(watch) == false) {
                pendingWatches.put(watch.id(), watch);
            }
        }
    }

    /// Atomically removes a watch from the pending-watches map and the trigger engine under the same lock as
    /// [#onWatchAdded]. This prevents a concurrent `postIndex` from resurrecting a deleted watch by sneaking an add
    /// in between the two halves of the removal.
    @Override
    public void onWatchRemoved(String watchId) {
        synchronized (pendingWatches) {
            pendingWatches.remove(watchId);
            triggerService.remove(watchId);
        }
    }

    // visible for testing
    Map<String, Watch> pendingWatches() {
        return Collections.unmodifiableMap(pendingWatches);
    }

    // Non private for unit testing purposes
    void refreshWatches(IndexMetadata indexMetadata) {
        BroadcastResponse refreshResponse = client.admin()
            .indices()
            .refresh(new RefreshRequest(INDEX))
            .actionGet(TimeValue.timeValueSeconds(5));
        if (refreshResponse.getSuccessfulShards() < indexMetadata.getNumberOfShards()) {
            throw illegalState("not all required shards have been refreshed");
        }
    }

    /**
     * Wraps an abstract runnable to easier supply onFailure and doRun methods via lambdas
     * This ensures that the uncaught exception handler in the executing threadpool does not get called
     *
     * @param run                 The code to be executed in the runnable
     * @param exceptionConsumer   The exception handling code to be executed, if the runnable fails
     * @return                    The AbstractRunnable instance to pass to the executor
     */
    private static AbstractRunnable wrapWatcherService(Runnable run, Consumer<Exception> exceptionConsumer) {
        return new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                exceptionConsumer.accept(e);
            }

            @Override
            protected void doRun() throws Exception {
                run.run();
            }
        };
    }

    /**
     * Validate state transition when assertions are enabled, and apply the new state
     *
     * @param newState The new state to transition to
     */
    private void validateTransitionAndApplyNewState(WatcherState newState) {
        assert newState == state.get() || switch (newState) {
            case STARTED -> WatcherState.STARTING == state.get();
            case STOPPED -> WatcherState.STOPPING == state.get();
            case STOPPING -> WatcherState.STARTED == state.get() || WatcherState.STARTING == state.get();
            case STARTING -> WatcherState.STOPPED == state.get();
        } : "Unexpected transition from state " + state.get() + " to state " + newState;
        state.set(newState);
    }
}
