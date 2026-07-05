/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.plan.RecoveryPlannerService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * The source recovery accepts recovery requests from other peer shards and start the recovery process from this
 * source shard to the target shard.
 */
public class PeerRecoverySourceService extends AbstractLifecycleComponent implements IndexEventListener, ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(PeerRecoverySourceService.class);

    /// Maximum number of outgoing peer recoveries a node may run concurrently as a source.
    /// Requests that arrive when all slots are occupied are queued in FIFO order and started as slots free up.
    ///
    /// TODO: register this setting in `BUILT_IN_CLUSTER_SETTINGS` before we start elasticsearch-team#2805
    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING = Setting.intSetting(
        "indices.recovery.max_concurrent_outgoing_recoveries",
        // Throttling handled by master allocation for now.
        Integer.MAX_VALUE,
        1,
        Property.NodeScope,
        Property.Dynamic
    );

    public static class Actions {
        public static final String START_RECOVERY = "internal:index/shard/recovery/start_recovery";
        public static final String REESTABLISH_RECOVERY = "internal:index/shard/recovery/reestablish_recovery";
    }

    private final TransportService transportService;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final RecoverySettings recoverySettings;
    private final RecoveryPlannerService recoveryPlannerService;

    // visible for testing
    final OngoingRecoveries ongoingRecoveries;

    public PeerRecoverySourceService(
        TransportService transportService,
        IndicesService indicesService,
        ClusterService clusterService,
        RecoverySettings recoverySettings,
        RecoveryPlannerService recoveryPlannerService,
        CompositeRecoverySchedulingListener schedulingListeners
    ) {
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.recoverySettings = recoverySettings;
        this.recoveryPlannerService = recoveryPlannerService;
        this.ongoingRecoveries = new OngoingRecoveries(schedulingListeners);
        clusterService.getClusterSettings()
            .initializeAndWatchIfRegistered(
                INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING,
                ongoingRecoveries::updateMaxConcurrentOutgoingRecoveries
            );
        // When the target node wants to start a peer recovery it sends a START_RECOVERY request to the source
        // node. Upon receiving START_RECOVERY, the source node will initiate the peer recovery.
        transportService.registerRequestHandler(
            Actions.START_RECOVERY,
            transportService.getThreadPool().generic(),
            StartRecoveryRequest::new,
            (request, channel, task) -> recover(request, task, new ChannelActionListener<>(channel))
        );
        // When the target node's START_RECOVERY request has failed due to a network disconnection, it will
        // send a REESTABLISH_RECOVERY. This attempts to reconnect to an existing recovery process taking
        // place on the source node. If the recovery process no longer exists, then the REESTABLISH_RECOVERY
        // action will fail and the target node will send a new START_RECOVERY request.
        transportService.registerRequestHandler(
            Actions.REESTABLISH_RECOVERY,
            transportService.getThreadPool().generic(),
            ReestablishRecoveryRequest::new,
            (request, channel, task) -> reestablish(request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected void doStart() {
        final ClusterService clusterService = indicesService.clusterService();
        if (DiscoveryNode.canContainData(clusterService.getSettings())) {
            clusterService.addListener(this);
        }
        this.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                ongoingRecoveries.cancelAllPendingRecoveries();
            }
        });
    }

    @Override
    protected void doStop() {
        final ClusterService clusterService = indicesService.clusterService();
        if (DiscoveryNode.canContainData(clusterService.getSettings())) {
            // Drained by the `beforeStop()` listener registered in `doStart()`, which runs before the lifecycle
            // transitions to STOPPED, preventing `onRecoveryComplete()` from racing to promote a queued recovery
            // against a stopped lifecycle. Any new incoming recovery would also fail at `indexServiceSafe()` since
            // `IndicesService.stop()` runs before this service closes.
            assert ongoingRecoveries.queuedRecoveryCount() == 0 : "pending recoveries queue should already be drained";
            ongoingRecoveries.awaitEmpty();
            indicesService.clusterService().removeListener(this);
        }
    }

    @Override
    protected void doClose() {}

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            ongoingRecoveries.cancel(indexShard);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                ongoingRecoveries.cancelOnNodeLeft(removedNode);
            }
        }
    }

    private void recover(StartRecoveryRequest request, Task task, ActionListener<RecoveryResponse> listener) {
        PeerRecoverySourceClusterStateDelay.ensureClusterStateVersion(
            request.clusterStateVersion(),
            clusterService,
            transportService.getThreadPool().generic(),
            transportService.getThreadPool().getThreadContext(),
            listener,
            new Consumer<>() {
                @Override
                public void accept(ActionListener<RecoveryResponse> l) {
                    recoverWithFreshClusterState(request, task, l);
                }

                @Override
                public String toString() {
                    return "recovery [" + request + "]";
                }
            }
        );
    }

    private void recoverWithFreshClusterState(StartRecoveryRequest request, Task task, ActionListener<RecoveryResponse> listener) {
        final IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        final IndexShard shard = indexService.getShard(request.shardId().id());

        final ShardRouting routingEntry = shard.routingEntry();

        if (routingEntry.primary() == false || routingEntry.active() == false) {
            throw new DelayRecoveryException("source shard [" + routingEntry + "] is not an active primary");
        }

        if (request.isPrimaryRelocation()
            && (routingEntry.relocating() == false || routingEntry.relocatingNodeId().equals(request.targetNode().getId()) == false)) {
            logger.debug(
                "delaying recovery of {} as source shard is not marked yet as relocating to {}",
                request.shardId(),
                request.targetNode()
            );
            throw new DelayRecoveryException("source shard is not marked yet as relocating to [" + request.targetNode() + "]");
        }
        ongoingRecoveries.enqueueRecovery(request, task, shard, listener);
    }

    private void reestablish(ReestablishRecoveryRequest request, ActionListener<RecoveryResponse> listener) {
        final IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        final IndexShard shard = indexService.getShard(request.shardId().id());

        logger.trace(
            "[{}][{}] reestablishing recovery {}",
            request.shardId().getIndex().getName(),
            request.shardId().id(),
            request.recoveryId()
        );
        ongoingRecoveries.reestablishRecovery(request, shard, listener);
    }

    final class OngoingRecoveries {

        private int maxConcurrentOutgoingRecoveries;

        private final CompositeRecoverySchedulingListener schedulingListeners;

        private final Map<IndexShard, ShardRecoveryContext> activeRecoveries = new HashMap<>();

        private final Map<DiscoveryNode, Collection<RemoteRecoveryTargetHandler>> nodeToHandlers = new HashMap<>();

        private final Deque<PendingRecovery> pendingRecoveries = new ArrayDeque<>();

        private int activeRecoveryHandlerCount = 0;

        OngoingRecoveries(CompositeRecoverySchedulingListener schedulingListeners) {
            this.schedulingListeners = schedulingListeners;
        }

        // visible for testing
        synchronized int activeRecoveryCount() {
            return activeRecoveryHandlerCount;
        }

        // visible for testing
        synchronized int queuedRecoveryCount() {
            return pendingRecoveries.size();
        }

        /// Always enqueues first to preserve FIFO ordering across all recoveries.
        /// Attempts recoveries for pending items (if slots are available) after enqueuing.
        void enqueueRecovery(StartRecoveryRequest request, Task task, IndexShard shard, ActionListener<RecoveryResponse> listener) {
            synchronized (this) {
                assert lifecycle.started();
                ensureNoDuplicateAllocationId(request.targetAllocationId());
                // TODO: consider capping the queue depth and rejecting with DelayRecoveryException once exceeded.
                final var subscribableListener = new SubscribableListener<RecoveryResponse>();
                subscribableListener.addListener(listener);
                pendingRecoveries.add(new PendingRecovery(request, task, shard, subscribableListener));
                shard.recoveryStats().sourceRecoveryQueued();
            }
            schedulingListeners.onRecoveryQueued(RecoverySource.Type.PEER, RecoveryRole.SOURCE);
            startRecoveriesUpToLimit();
        }

        private RecoverySourceHandler addNewRecovery(StartRecoveryRequest request, Task task, IndexShard shard) {
            assert Thread.holdsLock(this);
            assert lifecycle.started();
            assert activeRecoveryHandlerCount < maxConcurrentOutgoingRecoveries;
            final ShardRecoveryContext shardContext = activeRecoveries.computeIfAbsent(shard, s -> new ShardRecoveryContext());
            final Tuple<RecoverySourceHandler, RemoteRecoveryTargetHandler> handlers = shardContext.addNewRecovery(request, task, shard);
            final RemoteRecoveryTargetHandler recoveryTargetHandler = handlers.v2();
            nodeToHandlers.computeIfAbsent(recoveryTargetHandler.targetNode(), k -> new HashSet<>()).add(recoveryTargetHandler);
            activeRecoveryHandlerCount++;
            return handlers.v1();
        }

        void cancelOnNodeLeft(DiscoveryNode node) {
            final List<PendingRecovery> cancelled;
            synchronized (this) {
                final Collection<RemoteRecoveryTargetHandler> handlers = nodeToHandlers.get(node);
                if (handlers != null) {
                    for (RemoteRecoveryTargetHandler handler : handlers) {
                        handler.cancel();
                    }
                }
                cancelled = removePendingRecoveries(pendingRecovery -> pendingRecovery.request().targetNode().equals(node));
                for (PendingRecovery cancelledRecovery : cancelled) {
                    cancelledRecovery.shard().recoveryStats().sourceQueuedRecoveryDiscarded();
                }
            }
            for (PendingRecovery cancelledRecovery : cancelled) {
                cancelledRecovery.listener()
                    .onFailure(
                        new DelayRecoveryException(
                            Strings.format(
                                "cancelled pending recovery for shard %s, with target node %s: target node left during queued recovery",
                                cancelledRecovery.shard().shardId(),
                                cancelledRecovery.request().targetNode()
                            )
                        )
                    );
                schedulingListeners.onQueuedRecoveryDiscarded(RecoverySource.Type.PEER, RecoveryRole.SOURCE);
            }
        }

        synchronized void reestablishRecovery(
            ReestablishRecoveryRequest request,
            IndexShard shard,
            ActionListener<RecoveryResponse> listener
        ) {
            assert lifecycle.started();
            final ShardRecoveryContext shardContext = activeRecoveries.get(shard);
            if (shardContext != null && shardContext.reestablishRecovery(request, listener)) {
                return;
            }
            // The recovery is not in active handlers. Check if it's pending.
            for (PendingRecovery pending : pendingRecoveries) {
                if (pending.request().recoveryId() == request.recoveryId()
                    && pending.request().targetAllocationId().equals(request.targetAllocationId())) {
                    pending.listener().addListener(listener);
                    return;
                }
            }
            if (shardContext == null) {
                // caller will retry
                throw new PeerRecoveryNotFound(request.recoveryId(), request.shardId(), request.targetAllocationId());
            } else {
                // TODO: legacy behavior, but should we consider also throwing PeerRecoveryNotFound here?
                throw new ResourceNotFoundException("Cannot reestablish recovery, recovery id [" + request.recoveryId() + "] not found.");
            }
        }

        /// Called when an active recovery completes (successfully or not).
        /// Frees the throttling slot and starts any queued recoveries that now fit within the limit.
        void onRecoveryComplete(IndexShard shard, RecoverySourceHandler handler) {
            synchronized (this) {
                remove(shard, handler);
                // Update the recovery stats inside the lock to ensure consistency, and to avoid briefly showing negative counters to users.
                shard.recoveryStats().sourceRecoveryCompleted();
            }
            schedulingListeners.onRecoveryCompleted(RecoverySource.Type.PEER, RecoveryRole.SOURCE);
            startRecoveriesUpToLimit();
        }

        void updateMaxConcurrentOutgoingRecoveries(int newMax) {
            final int oldMax;
            synchronized (this) {
                oldMax = maxConcurrentOutgoingRecoveries;
                maxConcurrentOutgoingRecoveries = newMax;
            }
            if (oldMax < newMax) {
                startRecoveriesUpToLimit();
            }
        }

        /// Dequeues and starts pending recoveries up to the max concurrency limit.
        /// Acquires the lock once per dequeued recovery and triggers recovery in same loop, outside the lock.
        void startRecoveriesUpToLimit() {
            while (true) {
                final PendingRecovery nextRecovery;
                final RecoverySourceHandler nextHandler;
                synchronized (this) {
                    if (activeRecoveryHandlerCount >= maxConcurrentOutgoingRecoveries || pendingRecoveries.isEmpty()) {
                        break;
                    }
                    nextRecovery = pendingRecoveries.poll();
                    nextHandler = addNewRecovery(nextRecovery.request(), nextRecovery.task(), nextRecovery.shard());
                    nextRecovery.shard().recoveryStats().sourceRecoveryDequeuedAndStarted();
                }
                schedulingListeners.onRecoveryDequeuedAndStarted(RecoverySource.Type.PEER, RecoveryRole.SOURCE);
                logger.trace(
                    "[{}][{}] starting queued recovery to {}",
                    nextRecovery.request().shardId().getIndex().getName(),
                    nextRecovery.request().shardId().id(),
                    nextRecovery.request().targetNode()
                );
                final ActionListener<RecoveryResponse> wrappedListener = ActionListener.runAfter(
                    nextRecovery.listener(),
                    () -> onRecoveryComplete(nextRecovery.shard(), nextHandler)
                );
                // The generic executor has an unbounded queue and the threadpool shuts down after this service is stopped
                // (and drains the queue), so the `execute` call cannot throw `EsRejectedExecutionException` here.
                transportService.getThreadPool().generic().execute(() -> nextHandler.recoverToTarget(wrappedListener));
            }
        }

        void cancelAllPendingRecoveries() {
            final List<PendingRecovery> cancelled;
            synchronized (this) {
                cancelled = removePendingRecoveries(ignored -> true);
                for (PendingRecovery cancelledRecovery : cancelled) {
                    cancelledRecovery.shard().recoveryStats().sourceQueuedRecoveryDiscarded();
                }
            }
            for (PendingRecovery cancelledRecovery : cancelled) {
                cancelledRecovery.listener()
                    .onFailure(
                        new DelayRecoveryException(
                            Strings.format(
                                "cancelled pending recovery for shard %s, with target node %s: source node is closing",
                                cancelledRecovery.shard().shardId(),
                                cancelledRecovery.request().targetNode()
                            )
                        )
                    );
                schedulingListeners.onQueuedRecoveryDiscarded(RecoverySource.Type.PEER, RecoveryRole.SOURCE);
            }
        }

        private synchronized void remove(IndexShard shard, RecoverySourceHandler handler) {
            final ShardRecoveryContext shardRecoveryContext = activeRecoveries.get(shard);
            assert shardRecoveryContext != null : "Shard was not registered [" + shard + "]";
            final RemoteRecoveryTargetHandler removed = shardRecoveryContext.recoveryHandlers.remove(handler);
            assert removed != null : "Handler was not registered [" + handler + "]";
            if (removed != null) {
                activeRecoveryHandlerCount--;
                removed.cancel();
                assert nodeToHandlers.getOrDefault(removed.targetNode(), Collections.emptySet()).contains(removed)
                    : "Remote recovery was not properly tracked [" + removed + "]";
                nodeToHandlers.computeIfPresent(removed.targetNode(), (k, handlersForNode) -> {
                    handlersForNode.remove(removed);
                    if (handlersForNode.isEmpty()) {
                        return null;
                    }
                    return handlersForNode;
                });
            }
            if (shardRecoveryContext.recoveryHandlers.isEmpty()) {
                activeRecoveries.remove(shard);
            }
        }

        private void cancel(IndexShard shard) {
            final List<PendingRecovery> cancelled;
            synchronized (this) {
                final ShardRecoveryContext shardRecoveryContext = activeRecoveries.get(shard);
                if (shardRecoveryContext != null) {
                    final List<Exception> failures = new ArrayList<>();
                    for (RecoverySourceHandler handlers : shardRecoveryContext.recoveryHandlers.keySet()) {
                        try {
                            handlers.cancel("shard is closed");
                        } catch (Exception ex) {
                            failures.add(ex);
                        }
                    }
                    ExceptionsHelper.maybeThrowRuntimeAndSuppress(failures);
                }
                cancelled = removePendingRecoveries(pendingRecovery -> pendingRecovery.shard() == shard);
                for (PendingRecovery cancelledRecovery : cancelled) {
                    cancelledRecovery.shard().recoveryStats().sourceQueuedRecoveryDiscarded();
                }
            }
            for (PendingRecovery cancelledRecovery : cancelled) {
                cancelledRecovery.listener()
                    .onFailure(
                        new DelayRecoveryException(
                            Strings.format(
                                "cancelled pending recovery for shard %s, with target node %s: index shard closed",
                                cancelledRecovery.shard().shardId(),
                                cancelledRecovery.request().targetNode()
                            )
                        )
                    );
                schedulingListeners.onQueuedRecoveryDiscarded(RecoverySource.Type.PEER, RecoveryRole.SOURCE);
            }
        }

        private void awaitEmpty() {
            assert lifecycle.stoppedOrClosed();
            if (isEmpty()) {
                return;
            }
            final CountDownLatch emptyLatch = new CountDownLatch(1);
            final RecoverySchedulingListener listener = new RecoverySchedulingListener() {
                private void checkEmpty() {
                    if (isEmpty()) {
                        emptyLatch.countDown();
                    }
                }

                @Override
                public void onRecoveryCompleted(RecoverySource.Type type, RecoveryRole role) {
                    checkEmpty();
                }

                @Override
                public void onQueuedRecoveryDiscarded(RecoverySource.Type type, RecoveryRole role) {
                    checkEmpty();
                }
            };
            schedulingListeners.addListener(listener);
            try {
                // Force a check in case we became empty while registering
                if (isEmpty()) {
                    return;
                }
                emptyLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while waiting for recoveries to complete", e);
            } finally {
                schedulingListeners.removeListener(listener);
            }
        }

        private synchronized boolean isEmpty() {
            return activeRecoveries.isEmpty() && pendingRecoveries.isEmpty();
        }

        private void ensureNoDuplicateAllocationId(String targetAllocationId) {
            assert Thread.holdsLock(this);
            for (PendingRecovery pending : pendingRecoveries) {
                if (pending.request().targetAllocationId().equals(targetAllocationId)) {
                    throw new DelayRecoveryException(
                        "recovery with same target already registered, waiting for "
                            + "previous recovery attempt to be cancelled or completed"
                    );
                }
            }
            for (ShardRecoveryContext ctx : activeRecoveries.values()) {
                for (RecoverySourceHandler h : ctx.recoveryHandlers.keySet()) {
                    if (h.getRequest().targetAllocationId().equals(targetAllocationId)) {
                        throw new DelayRecoveryException(
                            "recovery with same target already registered, waiting for "
                                + "previous recovery attempt to be cancelled or completed"
                        );
                    }
                }
            }
        }

        private List<PendingRecovery> removePendingRecoveries(Predicate<PendingRecovery> predicate) {
            assert Thread.holdsLock(this);
            final List<PendingRecovery> cancelled = new ArrayList<>();
            pendingRecoveries.removeIf(pendingRecovery -> {
                if (predicate.test(pendingRecovery)) {
                    cancelled.add(pendingRecovery);
                    return true;
                }
                return false;
            });
            // immutable
            return List.copyOf(cancelled);
        }

        private record PendingRecovery(
            StartRecoveryRequest request,
            Task task,
            IndexShard shard,
            SubscribableListener<RecoveryResponse> listener
        ) {}

        private final class ShardRecoveryContext {
            final Map<RecoverySourceHandler, RemoteRecoveryTargetHandler> recoveryHandlers = new HashMap<>();

            /// Creates and registers a new recovery source handler for the given request.
            Tuple<RecoverySourceHandler, RemoteRecoveryTargetHandler> addNewRecovery(
                StartRecoveryRequest request,
                Task task,
                IndexShard shard
            ) {
                assert Thread.holdsLock(OngoingRecoveries.this);
                final Tuple<RecoverySourceHandler, RemoteRecoveryTargetHandler> handlers = createRecoverySourceHandler(
                    request,
                    task,
                    shard
                );
                recoveryHandlers.put(handlers.v1(), handlers.v2());
                return handlers;
            }

            /// Attaches `listener` to the active handler matching `request`. Returns `false` if no handler matches.
            boolean reestablishRecovery(ReestablishRecoveryRequest request, ActionListener<RecoveryResponse> listener) {
                assert Thread.holdsLock(OngoingRecoveries.this);
                RecoverySourceHandler handler = null;
                for (RecoverySourceHandler existingHandler : recoveryHandlers.keySet()) {
                    if (existingHandler.getRequest().recoveryId() == request.recoveryId()
                        && existingHandler.getRequest().targetAllocationId().equals(request.targetAllocationId())) {
                        handler = existingHandler;
                        break;
                    }
                }
                if (handler == null) {
                    return false;
                }
                handler.addListener(listener);
                return true;
            }

            private Tuple<RecoverySourceHandler, RemoteRecoveryTargetHandler> createRecoverySourceHandler(
                StartRecoveryRequest request,
                Task task,
                IndexShard shard
            ) {
                RecoverySourceHandler handler;
                final RemoteRecoveryTargetHandler recoveryTarget = new RemoteRecoveryTargetHandler(
                    request.recoveryId(),
                    request.shardId(),
                    transportService,
                    request.targetNode(),
                    recoverySettings,
                    throttleTime -> shard.recoveryStats().addThrottleTime(throttleTime),
                    task
                );
                handler = new RecoverySourceHandler(
                    shard,
                    recoveryTarget,
                    shard.getThreadPool(),
                    request,
                    Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
                    recoverySettings.getMaxConcurrentFileChunks(),
                    recoverySettings.getMaxConcurrentOperations(),
                    recoverySettings.getMaxConcurrentSnapshotFileDownloads(),
                    recoverySettings.getUseSnapshotsDuringRecovery(),
                    recoveryPlannerService
                );
                return Tuple.tuple(handler, recoveryTarget);
            }
        }
    }
}
