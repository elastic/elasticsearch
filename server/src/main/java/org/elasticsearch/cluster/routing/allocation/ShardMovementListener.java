/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.routing.MoveReason;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Receives every change the master makes to the location of a shard copy while computing a new routing table. Plugins register listeners
 * via {@link org.elasticsearch.plugins.ClusterPlugin#getShardMovementListeners()}.
 * <p>
 * Events are delivered as they are made, i.e. before the resulting cluster state is published: if publication fails the change is
 * discarded and a later allocation round may report the same event again. The first {@link ShardRouting} argument of each method is
 * the copy concerned, as it was <em>before</em> the change.
 * <p>
 * The methods are called on the master service thread inside the allocation round, so implementations must be non-blocking and cheap:
 * hand the event to a bounded queue and do any I/O elsewhere. Events that cannot be queued should be dropped and counted rather than
 * applying back-pressure to allocation. Implementations must not throw. Nothing is allocated for an event unless a listener needs it, so a
 * listener should check whether it wants the event before building anything from the arguments.
 */
public interface ShardMovementListener {

    ShardMovementListener NOOP = new ShardMovementListener() {};

    /**
     * An unassigned shard copy was assigned to a node. Does not include relocation targets, which are reported by
     * {@link #relocationStarted}.
     */
    default void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {}

    /**
     * An initializing shard copy started. Includes relocation targets.
     */
    default void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {}

    /**
     * A started shard copy began relocating from its node to {@code targetRelocatingShard}'s node.
     */
    default void relocationStarted(ShardRouting startedShard, ShardRouting targetRelocatingShard, MoveReason reason) {}

    /**
     * A relocation completed: the relocation target started and the relocation source was removed.
     */
    default void relocationCompleted(ShardRouting removedRelocationSource) {}

    /**
     * An assigned shard copy failed or was cancelled. Cancelled relocations are reported here too, with the cancellation cause in
     * {@link UnassignedInfo#message()} and {@link UnassignedInfo#failure()}.
     */
    default void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {}

    /**
     * Combines listeners so each receives every event. A listener that throws is skipped for that event and the failure is logged, so one
     * misbehaving listener cannot affect allocation or the other listeners.
     */
    static ShardMovementListener composite(Collection<? extends ShardMovementListener> listeners) {
        return switch (listeners.size()) {
            case 0 -> NOOP;
            case 1 -> new SafeListener(listeners.iterator().next());
            default -> {
                final var safeListeners = listeners.stream().map(SafeListener::new).toList();
                yield new ShardMovementListener() {
                    @Override
                    public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {
                        for (final var listener : safeListeners) {
                            listener.shardInitialized(unassignedShard, initializedShard);
                        }
                    }

                    @Override
                    public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
                        for (final var listener : safeListeners) {
                            listener.shardStarted(initializingShard, startedShard);
                        }
                    }

                    @Override
                    public void relocationStarted(ShardRouting startedShard, ShardRouting targetRelocatingShard, MoveReason reason) {
                        for (final var listener : safeListeners) {
                            listener.relocationStarted(startedShard, targetRelocatingShard, reason);
                        }
                    }

                    @Override
                    public void relocationCompleted(ShardRouting removedRelocationSource) {
                        for (final var listener : safeListeners) {
                            listener.relocationCompleted(removedRelocationSource);
                        }
                    }

                    @Override
                    public void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {
                        for (final var listener : safeListeners) {
                            listener.shardFailed(failedShard, unassignedInfo);
                        }
                    }
                };
            }
        };
    }

    /**
     * Wraps a listener so that exceptions are logged (at WARN the first time, DEBUG thereafter) rather than propagated.
     */
    final class SafeListener implements ShardMovementListener {
        private static final Logger logger = LogManager.getLogger(SafeListener.class);

        private final ShardMovementListener delegate;
        private final AtomicBoolean warned = new AtomicBoolean();

        SafeListener(ShardMovementListener delegate) {
            this.delegate = delegate;
        }

        @Override
        public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {
            try {
                delegate.shardInitialized(unassignedShard, initializedShard);
            } catch (Exception e) {
                onFailure(e);
            }
        }

        @Override
        public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
            try {
                delegate.shardStarted(initializingShard, startedShard);
            } catch (Exception e) {
                onFailure(e);
            }
        }

        @Override
        public void relocationStarted(ShardRouting startedShard, ShardRouting targetRelocatingShard, MoveReason reason) {
            try {
                delegate.relocationStarted(startedShard, targetRelocatingShard, reason);
            } catch (Exception e) {
                onFailure(e);
            }
        }

        @Override
        public void relocationCompleted(ShardRouting removedRelocationSource) {
            try {
                delegate.relocationCompleted(removedRelocationSource);
            } catch (Exception e) {
                onFailure(e);
            }
        }

        @Override
        public void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {
            try {
                delegate.shardFailed(failedShard, unassignedInfo);
            } catch (Exception e) {
                onFailure(e);
            }
        }

        private void onFailure(Exception e) {
            if (warned.compareAndSet(false, true)) {
                logger.warn(() -> "shard movement listener [" + delegate + "] failed, further failures will be logged at DEBUG", e);
            } else {
                logger.debug(() -> "shard movement listener [" + delegate + "] failed", e);
            }
        }
    }
}
