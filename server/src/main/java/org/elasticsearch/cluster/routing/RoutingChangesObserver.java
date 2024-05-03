/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

/**
 * Records changes made to {@link RoutingNodes} during an allocation round.
 */
public interface RoutingChangesObserver {
    /**
     * Called when unassigned shard is initialized. Does not include initializing relocation target shards.
     */
    default void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {}

    /**
     * Called when an initializing shard is started.
     */
    default void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {}

    /**
     * Called when relocation of a started shard is initiated.
     */
    default void relocationStarted(ShardRouting startedShard, ShardRouting targetRelocatingShard) {}

    /**
     * Called when an unassigned shard's unassigned information was updated
     */
    default void unassignedInfoUpdated(ShardRouting unassignedShard, UnassignedInfo newUnassignedInfo) {}

    /**
     * Called when a relocating shard's failure information was updated
     */
    default void relocationFailureInfoUpdated(ShardRouting relocatedShard, RelocationFailureInfo relocationFailureInfo) {}

    /**
     * Called when a shard is failed or cancelled.
     */
    default void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {}

    /**
     * Called on relocation source when relocation completes after relocation target is started.
     */
    default void relocationCompleted(ShardRouting removedRelocationSource) {}

    /**
     * Called on replica relocation target when replica relocation source fails. Promotes the replica relocation target to ordinary
     * initializing shard.
     */
    default void relocationSourceRemoved(ShardRouting removedReplicaRelocationSource) {}

    /**
     * Called when started replica is promoted to primary.
     */
    default void replicaPromoted(ShardRouting replicaShard) {}

    /**
     * Called when an initializing replica is reinitialized. This happens when a primary relocation completes, which
     * reinitializes all currently initializing replicas as their recovery source node changes
     */
    default void initializedReplicaReinitialized(ShardRouting oldReplica, ShardRouting reinitializedReplica) {}

    class DelegatingRoutingChangesObserver implements RoutingChangesObserver {

        private final RoutingChangesObserver[] routingChangesObservers;

        public DelegatingRoutingChangesObserver(RoutingChangesObserver... routingChangesObservers) {
            this.routingChangesObservers = routingChangesObservers;
        }

        @Override
        public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.shardInitialized(unassignedShard, initializedShard);
            }
        }

        @Override
        public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.shardStarted(initializingShard, startedShard);
            }
        }

        @Override
        public void relocationStarted(ShardRouting startedShard, ShardRouting targetRelocatingShard) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.relocationStarted(startedShard, targetRelocatingShard);
            }
        }

        @Override
        public void unassignedInfoUpdated(ShardRouting unassignedShard, UnassignedInfo newUnassignedInfo) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.unassignedInfoUpdated(unassignedShard, newUnassignedInfo);
            }
        }

        @Override
        public void shardFailed(ShardRouting activeShard, UnassignedInfo unassignedInfo) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.shardFailed(activeShard, unassignedInfo);
            }
        }

        @Override
        public void relocationCompleted(ShardRouting removedRelocationSource) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.relocationCompleted(removedRelocationSource);
            }
        }

        @Override
        public void relocationSourceRemoved(ShardRouting removedReplicaRelocationSource) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.relocationSourceRemoved(removedReplicaRelocationSource);
            }
        }

        @Override
        public void replicaPromoted(ShardRouting replicaShard) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.replicaPromoted(replicaShard);
            }
        }

        @Override
        public void initializedReplicaReinitialized(ShardRouting oldReplica, ShardRouting reinitializedReplica) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.initializedReplicaReinitialized(oldReplica, reinitializedReplica);
            }
        }
    }
}
