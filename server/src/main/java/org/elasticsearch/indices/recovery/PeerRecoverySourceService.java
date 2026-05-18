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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.plan.RecoveryPlannerService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
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
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * The source recovery accepts recovery requests from other peer shards and start the recovery process from this
 * source shard to the target shard.
 */
public class PeerRecoverySourceService extends AbstractLifecycleComponent implements IndexEventListener, ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(PeerRecoverySourceService.class);

    // This setting is intentionally not registered; it is only used in tests.
    // In production the default value mirrors the master-side throttle
    // (cluster.routing.allocation.node_concurrent_recoveries).
    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_OUTBOUND_RECOVERIES_SETTING = Setting.intSetting(
        "indices.recovery.max_concurrent_outbound_recoveries",
        ThrottlingAllocationDecider.DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES,
        1,
        Property.NodeScope
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

    // TODO: make this dynamic?
    private final int maxConcurrentOutboundRecoveries;

    // visible for testing
    final OngoingRecoveries ongoingRecoveries = new OngoingRecoveries();

    final int numberOfOngoingRecoveries() {
        return ongoingRecoveries.ongoingRecoveries.size();
    }

    public PeerRecoverySourceService(
        TransportService transportService,
        IndicesService indicesService,
        ClusterService clusterService,
        RecoverySettings recoverySettings,
        RecoveryPlannerService recoveryPlannerService
    ) {
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.recoverySettings = recoverySettings;
        this.recoveryPlannerService = recoveryPlannerService;
        this.maxConcurrentOutboundRecoveries = INDICES_RECOVERY_MAX_CONCURRENT_OUTBOUND_RECOVERIES_SETTING.get(
            clusterService.getSettings()
        );
        // When the target node wants to start a peer recovery it sends a START_RECOVERY request to the source
        // node. Upon receiving START_RECOVERY, the source node will initiate the peer recovery.
        transportService.registerRequestHandler(
            Actions.START_RECOVERY,
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC),
            StartRecoveryRequest::new,
            (request, channel, task) -> recover(request, task, new ChannelActionListener<>(channel))
        );
        // When the target node's START_RECOVERY request has failed due to a network disconnection, it will
        // send a REESTABLISH_RECOVERY. This attempts to reconnect to an existing recovery process taking
        // place on the source node. If the recovery process no longer exists, then the REESTABLISH_RECOVERY
        // action will fail and the target node will send a new START_RECOVERY request.
        transportService.registerRequestHandler(
            Actions.REESTABLISH_RECOVERY,
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC),
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
    }

    @Override
    protected void doStop() {
        final ClusterService clusterService = indicesService.clusterService();
        if (DiscoveryNode.canContainData(clusterService.getSettings())) {
            ongoingRecoveries.cancelAllPendingRecoveries();
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

        final RecoverySourceHandler handler = ongoingRecoveries.addOrEnqueueNewRecovery(request, task, shard, listener);
        if (handler != null) {
            logger.trace(
                "[{}][{}] starting recovery to {}",
                request.shardId().getIndex().getName(),
                request.shardId().id(),
                request.targetNode()
            );
            handler.recoverToTarget(ActionListener.runAfter(listener, () -> ongoingRecoveries.onRecoveryComplete(shard, handler)));
        }
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

        private final Map<IndexShard, ShardRecoveryContext> ongoingRecoveries = new HashMap<>();

        private final Map<DiscoveryNode, Collection<RemoteRecoveryTargetHandler>> nodeToHandlers = new HashMap<>();

        // TODO: should we do some smarter aggregation by shard?
        private final Deque<PendingRecovery> pendingRecoveries = new ArrayDeque<>();

        private int activeRecoveryHandlersCount = 0;

        @Nullable
        private List<ActionListener<Void>> emptyListeners;

        // visible for testing
        synchronized int activeRecoveryCount() {
            return activeRecoveryHandlersCount;
        }

        // visible for testing
        synchronized int pendingRecoveriesCount() {
            return pendingRecoveries.size();
        }

        /// Starts the recovery immediately if a slot is available, otherwise queues it for later.
        /// Returns the handler to start (non-null) if a slot was available, or null if the request was queued.
        synchronized RecoverySourceHandler addOrEnqueueNewRecovery(
            StartRecoveryRequest request,
            Task task,
            IndexShard shard,
            ActionListener<RecoveryResponse> listener
        ) {
            assert lifecycle.started();
            ensureNoDuplicateAllocationId(request.targetAllocationId());
            if (activeRecoveryHandlersCount < maxConcurrentOutboundRecoveries) {
                return addNewRecovery(request, task, shard);
            }
            shard.recoveryStats().incCurrentQueuedAsSource();
            // TODO: should we add a limit to the queuing as well? After which we just straight up reject?
            pendingRecoveries.add(new PendingRecovery(request, task, shard, listener));
            return null;
        }

        private RecoverySourceHandler addNewRecovery(StartRecoveryRequest request, Task task, IndexShard shard) {
            assert lifecycle.started();
            assert activeRecoveryHandlersCount < maxConcurrentOutboundRecoveries;
            activeRecoveryHandlersCount++;
            final ShardRecoveryContext shardContext = ongoingRecoveries.computeIfAbsent(shard, s -> new ShardRecoveryContext());
            final Tuple<RecoverySourceHandler, RemoteRecoveryTargetHandler> handlers = shardContext.addNewRecovery(request, task, shard);
            final RemoteRecoveryTargetHandler recoveryTargetHandler = handlers.v2();
            nodeToHandlers.computeIfAbsent(recoveryTargetHandler.targetNode(), k -> new HashSet<>()).add(recoveryTargetHandler);
            shard.recoveryStats().incCurrentAsSource();
            return handlers.v1();
        }

        synchronized void cancelOnNodeLeft(DiscoveryNode node) {
            final Collection<RemoteRecoveryTargetHandler> handlers = nodeToHandlers.get(node);
            if (handlers != null) {
                for (RemoteRecoveryTargetHandler handler : handlers) {
                    handler.cancel();
                }
            }
            cancelPendingRecoveries(
                pendingRecovery -> pendingRecovery.request().targetNode().equals(node),
                "target node left during queued recovery"
            );
        }

        synchronized void reestablishRecovery(
            ReestablishRecoveryRequest request,
            IndexShard shard,
            ActionListener<RecoveryResponse> listener
        ) {
            assert lifecycle.started();
            final ShardRecoveryContext shardContext = ongoingRecoveries.get(shard);
            if (shardContext == null) {
                throw new PeerRecoveryNotFound(request.recoveryId(), request.shardId(), request.targetAllocationId());
            }
            shardContext.reestablishRecovery(request, listener);
        }

        /// Called when an active recovery completes (successfully or not).
        /// Frees the throttling slot and starts the next queued recovery if one is waiting.
        void onRecoveryComplete(IndexShard shard, RecoverySourceHandler handler) {
            final PendingRecovery nextRecovery;
            final RecoverySourceHandler nextHandler;
            synchronized (this) {
                remove(shard, handler);
                if (activeRecoveryHandlersCount < maxConcurrentOutboundRecoveries && pendingRecoveries.isEmpty() == false) {
                    assert activeRecoveryHandlersCount == maxConcurrentOutboundRecoveries - 1;
                    nextRecovery = pendingRecoveries.poll();
                    nextRecovery.shard().recoveryStats().decCurrentQueuedAsSource();
                    nextHandler = addNewRecovery(nextRecovery.request(), nextRecovery.task(), nextRecovery.shard());
                } else {
                    nextHandler = null;
                    nextRecovery = null;
                }
            }
            if (nextHandler != null) {
                logger.trace(
                    "[{}][{}] starting queued recovery to {}",
                    nextRecovery.request().shardId().getIndex().getName(),
                    nextRecovery.request().shardId().id(),
                    nextRecovery.request().targetNode()
                );
                nextHandler.recoverToTarget(
                    ActionListener.runAfter(nextRecovery.listener(), () -> onRecoveryComplete(nextRecovery.shard(), nextHandler))
                );
            }
        }

        synchronized void cancelAllPendingRecoveries() {
            cancelPendingRecoveries(ignored -> true, "source node is closing");
        }

        synchronized void remove(IndexShard shard, RecoverySourceHandler handler) {
            final ShardRecoveryContext shardRecoveryContext = ongoingRecoveries.get(shard);
            assert shardRecoveryContext != null : "Shard was not registered [" + shard + "]";
            final RemoteRecoveryTargetHandler removed = shardRecoveryContext.recoveryHandlers.remove(handler);
            assert removed != null : "Handler was not registered [" + handler + "]";
            if (removed != null) {
                activeRecoveryHandlersCount--;
                shard.recoveryStats().decCurrentAsSource();
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
                ongoingRecoveries.remove(shard);
            }
            if (ongoingRecoveries.isEmpty() && pendingRecoveries.isEmpty()) {
                if (emptyListeners != null) {
                    final List<ActionListener<Void>> onEmptyListeners = emptyListeners;
                    emptyListeners = null;
                    ActionListener.onResponse(onEmptyListeners, null);
                }
            }
        }

        synchronized void cancel(IndexShard shard) {
            final ShardRecoveryContext shardRecoveryContext = ongoingRecoveries.get(shard);
            if (shardRecoveryContext != null) {
                final List<Exception> failures = new ArrayList<>();
                for (RecoverySourceHandler handlers : shardRecoveryContext.recoveryHandlers.keySet()) {
                    try {
                        handlers.cancel("shard is closed");
                    } catch (Exception ex) {
                        failures.add(ex);
                    } finally {
                        shard.recoveryStats().decCurrentAsSource();
                    }
                }
                ExceptionsHelper.maybeThrowRuntimeAndSuppress(failures);
            }
            cancelPendingRecoveries(pendingRecovery -> pendingRecovery.shard() == shard, "index shard closed");
        }

        void awaitEmpty() {
            assert lifecycle.stoppedOrClosed();
            final PlainActionFuture<Void> future;
            synchronized (this) {
                if (ongoingRecoveries.isEmpty() && pendingRecoveries.isEmpty()) {
                    return;
                }
                future = new PlainActionFuture<>();
                if (emptyListeners == null) {
                    emptyListeners = new ArrayList<>();
                }
                emptyListeners.add(future);
            }
            FutureUtils.get(future);
        }

        // TODO: optimize this?
        private void ensureNoDuplicateAllocationId(String targetAllocationId) {
            for (PendingRecovery pending : pendingRecoveries) {
                if (pending.request().targetAllocationId().equals(targetAllocationId)) {
                    throw new DelayRecoveryException(
                        "recovery with same target already registered, waiting for "
                            + "previous recovery attempt to be cancelled or completed"
                    );
                }
            }
            for (ShardRecoveryContext ctx : ongoingRecoveries.values()) {
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

        private void cancelPendingRecoveries(Predicate<PendingRecovery> predicate, String reason) {
            final List<PendingRecovery> cancelled = new ArrayList<>();
            pendingRecoveries.removeIf(pendingRecovery -> {
                if (predicate.test(pendingRecovery)) {
                    cancelled.add(pendingRecovery);
                    return true;
                }
                return false;
            });
            cancelled.forEach(pendingRecovery -> {
                final String cancellationMsg = Strings.format(
                    "cancelled pending recovery for shard {}, with target node {}",
                    pendingRecovery.shard.shardId(),
                    pendingRecovery.request.targetNode()
                );
                pendingRecovery.shard().recoveryStats().decCurrentQueuedAsSource();
                pendingRecovery.listener().onFailure(new DelayRecoveryException(cancellationMsg + " : " + reason));
            });
        }

        private record PendingRecovery(
            StartRecoveryRequest request,
            Task task,
            IndexShard shard,
            ActionListener<RecoveryResponse> listener
        ) {}

        private final class ShardRecoveryContext {
            final Map<RecoverySourceHandler, RemoteRecoveryTargetHandler> recoveryHandlers = new HashMap<>();

            /**
             * Adds recovery source handler.
             */
            synchronized Tuple<RecoverySourceHandler, RemoteRecoveryTargetHandler> addNewRecovery(
                StartRecoveryRequest request,
                Task task,
                IndexShard shard
            ) {
                final Tuple<RecoverySourceHandler, RemoteRecoveryTargetHandler> handlers = createRecoverySourceHandler(
                    request,
                    task,
                    shard
                );
                recoveryHandlers.put(handlers.v1(), handlers.v2());
                return handlers;
            }

            /**
             * Adds recovery source handler.
             */
            synchronized void reestablishRecovery(ReestablishRecoveryRequest request, ActionListener<RecoveryResponse> listener) {
                RecoverySourceHandler handler = null;
                for (RecoverySourceHandler existingHandler : recoveryHandlers.keySet()) {
                    if (existingHandler.getRequest().recoveryId() == request.recoveryId()
                        && existingHandler.getRequest().targetAllocationId().equals(request.targetAllocationId())) {
                        handler = existingHandler;
                        break;
                    }
                }
                if (handler == null) {
                    throw new ResourceNotFoundException(
                        "Cannot reestablish recovery, recovery id [" + request.recoveryId() + "] not found."
                    );
                }
                handler.addListener(listener);
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
