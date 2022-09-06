/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Observer that tracks changes made to RoutingNodes in order to update the primary terms and in-sync allocation ids in
 * {@link IndexMetadata} once the allocation round has completed.
 *
 * Primary terms are updated on primary initialization or when an active primary fails.
 *
 * Allocation ids are added for shards that become active and removed for shards that stop being active.
 */
public class IndexMetadataUpdater extends RoutingChangesObserver.AbstractRoutingChangesObserver {
    private final Map<ShardId, Updates> shardChanges = new HashMap<>();

    @Override
    public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {
        assert initializedShard.isRelocationTarget() == false : "shardInitialized is not called on relocation target: " + initializedShard;
        if (initializedShard.primary()) {
            increasePrimaryTerm(initializedShard.shardId());

            Updates updates = changes(initializedShard.shardId());
            assert updates.initializedPrimary == null
                : "Primary cannot be initialized more than once in same allocation round: "
                    + "(previous: "
                    + updates.initializedPrimary
                    + ", next: "
                    + initializedShard
                    + ")";
            updates.initializedPrimary = initializedShard;
        }
    }

    @Override
    public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
        assert Objects.equals(initializingShard.allocationId().getId(), startedShard.allocationId().getId())
            : "initializingShard.allocationId ["
                + initializingShard.allocationId().getId()
                + "] and startedShard.allocationId ["
                + startedShard.allocationId().getId()
                + "] have to have the same";
        Updates updates = changes(startedShard.shardId());
        updates.addedAllocationIds.add(startedShard.allocationId().getId());
        if (startedShard.primary()
            // started shard has to have null recoverySource; have to pick up recoverySource from its initializing state
            && (initializingShard.recoverySource() == RecoverySource.ExistingStoreRecoverySource.FORCE_STALE_PRIMARY_INSTANCE)) {
            updates.removedAllocationIds.add(RecoverySource.ExistingStoreRecoverySource.FORCED_ALLOCATION_ID);
        }
    }

    @Override
    public void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {
        if (failedShard.active() && failedShard.primary()) {
            Updates updates = changes(failedShard.shardId());
            if (updates.firstFailedPrimary == null) {
                // more than one primary can be failed (because of batching, primary can be failed, replica promoted and then failed...)
                updates.firstFailedPrimary = failedShard;
            }
            increasePrimaryTerm(failedShard.shardId());
        }
    }

    @Override
    public void relocationCompleted(ShardRouting removedRelocationSource) {
        removeAllocationId(removedRelocationSource);
    }

    /**
     * Updates the current {@link Metadata} based on the changes of this RoutingChangesObserver. Specifically
     * we update {@link IndexMetadata#getInSyncAllocationIds()} and {@link IndexMetadata#primaryTerm(int)} based on
     * the changes made during this allocation.
     *
     * @param oldMetadata {@link Metadata} object from before the routing nodes was changed.
     * @param newRoutingTable {@link RoutingTable} object after routing changes were applied.
     * @return adapted {@link Metadata}, potentially the original one if no change was needed.
     */
    public Metadata applyChanges(Metadata oldMetadata, RoutingTable newRoutingTable) {
        Map<Index, List<Map.Entry<ShardId, Updates>>> changesGroupedByIndex = shardChanges.entrySet()
            .stream()
            .collect(Collectors.groupingBy(e -> e.getKey().getIndex()));

        final Map<String, IndexMetadata> updatedIndices = Maps.newHashMapWithExpectedSize(changesGroupedByIndex.size());
        for (Map.Entry<Index, List<Map.Entry<ShardId, Updates>>> indexChanges : changesGroupedByIndex.entrySet()) {
            Index index = indexChanges.getKey();
            final IndexMetadata oldIndexMetadata = oldMetadata.getIndexSafe(index);
            IndexMetadata updatedIndexMetadata = oldIndexMetadata;
            for (Map.Entry<ShardId, Updates> shardEntry : indexChanges.getValue()) {
                ShardId shardId = shardEntry.getKey();
                Updates updates = shardEntry.getValue();
                updatedIndexMetadata = updateInSyncAllocations(newRoutingTable, oldIndexMetadata, updatedIndexMetadata, shardId, updates);
                updatedIndexMetadata = updates.increaseTerm
                    ? updatedIndexMetadata.withIncrementedPrimaryTerm(shardId.id())
                    : updatedIndexMetadata;
            }

            if (updatedIndexMetadata != oldIndexMetadata) {
                updatedIndices.put(updatedIndexMetadata.getIndex().getName(), updatedIndexMetadata.withIncrementedVersion());
            }
        }
        return oldMetadata.withAllocationAndTermUpdatesOnly(updatedIndices);
    }

    /**
     * Updates in-sync allocations with routing changes that were made to the routing table.
     */
    private static IndexMetadata updateInSyncAllocations(
        RoutingTable newRoutingTable,
        IndexMetadata oldIndexMetadata,
        IndexMetadata updatedIndexMetadata,
        ShardId shardId,
        Updates updates
    ) {
        assert Sets.haveEmptyIntersection(updates.addedAllocationIds, updates.removedAllocationIds)
            : "allocation ids cannot be both added and removed in the same allocation round, added ids: "
                + updates.addedAllocationIds
                + ", removed ids: "
                + updates.removedAllocationIds;

        Set<String> oldInSyncAllocationIds = oldIndexMetadata.inSyncAllocationIds(shardId.id());

        // check if we have been force-initializing an empty primary or a stale primary
        if (updates.initializedPrimary != null
            && oldInSyncAllocationIds.isEmpty() == false
            && oldInSyncAllocationIds.contains(updates.initializedPrimary.allocationId().getId()) == false) {
            // we're not reusing an existing in-sync allocation id to initialize a primary, which means that we're either force-allocating
            // an empty or a stale primary (see AllocateEmptyPrimaryAllocationCommand or AllocateStalePrimaryAllocationCommand).
            RecoverySource recoverySource = updates.initializedPrimary.recoverySource();
            RecoverySource.Type recoverySourceType = recoverySource.getType();
            boolean emptyPrimary = recoverySourceType == RecoverySource.Type.EMPTY_STORE;
            assert updates.addedAllocationIds.isEmpty()
                : (emptyPrimary ? "empty" : "stale")
                    + " primary is not force-initialized in same allocation round where shards are started";
            if (emptyPrimary) {
                // forcing an empty primary resets the in-sync allocations to the empty set (ShardRouting.allocatedPostIndexCreate)
                updatedIndexMetadata = updatedIndexMetadata.withInSyncAllocationIds(shardId.id(), Set.of());
            } else {
                final String allocationId;
                if (recoverySource == RecoverySource.ExistingStoreRecoverySource.FORCE_STALE_PRIMARY_INSTANCE) {
                    allocationId = RecoverySource.ExistingStoreRecoverySource.FORCED_ALLOCATION_ID;
                    updatedIndexMetadata = updatedIndexMetadata.withTimestampRange(
                        updatedIndexMetadata.getTimestampRange().removeShard(shardId.id(), oldIndexMetadata.getNumberOfShards())
                    );
                } else {
                    assert recoverySource instanceof RecoverySource.SnapshotRecoverySource : recoverySource;
                    allocationId = updates.initializedPrimary.allocationId().getId();
                }
                // forcing a stale primary resets the in-sync allocations to the singleton set with the stale id
                updatedIndexMetadata = updatedIndexMetadata.withInSyncAllocationIds(shardId.id(), Set.of(allocationId));
            }
        } else {
            // standard path for updating in-sync ids
            Set<String> inSyncAllocationIds = new HashSet<>(oldInSyncAllocationIds);
            inSyncAllocationIds.addAll(updates.addedAllocationIds);
            inSyncAllocationIds.removeAll(updates.removedAllocationIds);

            assert oldInSyncAllocationIds.contains(RecoverySource.ExistingStoreRecoverySource.FORCED_ALLOCATION_ID) == false
                || inSyncAllocationIds.contains(RecoverySource.ExistingStoreRecoverySource.FORCED_ALLOCATION_ID) == false
                : "fake allocation id has to be removed, inSyncAllocationIds:" + inSyncAllocationIds;

            // Prevent set of inSyncAllocationIds to grow unboundedly. This can happen for example if we don't write to a primary
            // but repeatedly shut down nodes that have active replicas.
            // We use number_of_replicas + 1 (= possible active shard copies) to bound the inSyncAllocationIds set
            // Only trim the set of allocation ids when it grows, otherwise we might trim too eagerly when the number
            // of replicas was decreased while shards were unassigned.
            int maxActiveShards = oldIndexMetadata.getNumberOfReplicas() + 1; // +1 for the primary
            IndexShardRoutingTable newShardRoutingTable = newRoutingTable.shardRoutingTable(shardId);
            assert newShardRoutingTable.assignedShards()
                .stream()
                .filter(ShardRouting::isRelocationTarget)
                .map(s -> s.allocationId().getId())
                .noneMatch(inSyncAllocationIds::contains) : newShardRoutingTable.assignedShards() + " vs " + inSyncAllocationIds;
            if (inSyncAllocationIds.size() > oldInSyncAllocationIds.size() && inSyncAllocationIds.size() > maxActiveShards) {
                // trim entries that have no corresponding shard routing in the cluster state (i.e. trim unavailable copies)
                List<ShardRouting> assignedShards = newShardRoutingTable.assignedShards()
                    .stream()
                    .filter(s -> s.isRelocationTarget() == false)
                    .toList();
                assert assignedShards.size() <= maxActiveShards
                    : "cannot have more assigned shards " + assignedShards + " than maximum possible active shards " + maxActiveShards;
                Set<String> assignedAllocations = assignedShards.stream().map(s -> s.allocationId().getId()).collect(Collectors.toSet());
                inSyncAllocationIds = inSyncAllocationIds.stream()
                    .sorted(Comparator.comparing(assignedAllocations::contains).reversed()) // values with routing entries first
                    .limit(maxActiveShards)
                    .collect(Collectors.toSet());
            }

            // only remove allocation id of failed active primary if there is at least one active shard remaining. Assume for example that
            // the primary fails but there is no new primary to fail over to. If we were to remove the allocation id of the primary from the
            // in-sync set, this could create an empty primary on the next allocation.
            if (newShardRoutingTable.activeShards().isEmpty() && updates.firstFailedPrimary != null) {
                // add back allocation id of failed primary
                inSyncAllocationIds.add(updates.firstFailedPrimary.allocationId().getId());
            }

            assert inSyncAllocationIds.isEmpty() == false || oldInSyncAllocationIds.isEmpty()
                : "in-sync allocations cannot become empty after they have been non-empty: " + oldInSyncAllocationIds;

            // be extra safe here and only update in-sync set if it is non-empty
            if (inSyncAllocationIds.isEmpty() == false) {
                updatedIndexMetadata = updatedIndexMetadata.withInSyncAllocationIds(shardId.id(), inSyncAllocationIds);
            }
        }
        return updatedIndexMetadata;
    }

    /**
     * Removes allocation ids from the in-sync set for shard copies for which there is no routing entries in the routing table.
     * This method is called in AllocationService before any changes to the routing table are made.
     */
    public static ClusterState removeStaleIdsWithoutRoutings(ClusterState clusterState, List<StaleShard> staleShards, Logger logger) {
        Metadata oldMetadata = clusterState.metadata();
        RoutingTable oldRoutingTable = clusterState.routingTable();
        Metadata.Builder metadataBuilder = null;
        // group staleShards entries by index
        for (Map.Entry<Index, List<StaleShard>> indexEntry : staleShards.stream()
            .collect(Collectors.groupingBy(fs -> fs.shardId().getIndex()))
            .entrySet()) {
            final IndexMetadata oldIndexMetadata = oldMetadata.getIndexSafe(indexEntry.getKey());
            IndexMetadata.Builder indexMetadataBuilder = null;
            // group staleShards entries by shard id
            for (Map.Entry<ShardId, List<StaleShard>> shardEntry : indexEntry.getValue()
                .stream()
                .collect(Collectors.groupingBy(StaleShard::shardId))
                .entrySet()) {
                int shardNumber = shardEntry.getKey().getId();
                Set<String> oldInSyncAllocations = oldIndexMetadata.inSyncAllocationIds(shardNumber);
                Set<String> idsToRemove = shardEntry.getValue().stream().map(StaleShard::allocationId).collect(Collectors.toSet());
                assert idsToRemove.stream().allMatch(id -> oldRoutingTable.getByAllocationId(shardEntry.getKey(), id) == null)
                    : "removing stale ids: " + idsToRemove + ", some of which have still a routing entry: " + oldRoutingTable;
                Set<String> remainingInSyncAllocations = Sets.difference(oldInSyncAllocations, idsToRemove);
                assert remainingInSyncAllocations.isEmpty() == false
                    : "Set of in-sync ids cannot become empty for shard "
                        + shardEntry.getKey()
                        + " (before: "
                        + oldInSyncAllocations
                        + ", ids to remove: "
                        + idsToRemove
                        + ")";
                // be extra safe here: if the in-sync set were to become empty, this would create an empty primary on the next allocation
                // (see ShardRouting#allocatedPostIndexCreate)
                if (remainingInSyncAllocations.isEmpty() == false) {
                    if (indexMetadataBuilder == null) {
                        indexMetadataBuilder = IndexMetadata.builder(oldIndexMetadata);
                    }
                    indexMetadataBuilder.putInSyncAllocationIds(shardNumber, remainingInSyncAllocations);
                }
                logger.warn("{} marking unavailable shards as stale: {}", shardEntry.getKey(), idsToRemove);
            }

            if (indexMetadataBuilder != null) {
                if (metadataBuilder == null) {
                    metadataBuilder = Metadata.builder(oldMetadata);
                }
                metadataBuilder.put(indexMetadataBuilder);
            }
        }

        if (metadataBuilder != null) {
            return ClusterState.builder(clusterState).metadata(metadataBuilder).build();
        } else {
            return clusterState;
        }
    }

    /**
     * Helper method that creates update entry for the given shard id if such an entry does not exist yet.
     */
    private Updates changes(ShardId shardId) {
        return shardChanges.computeIfAbsent(shardId, k -> new Updates());
    }

    /**
     * Remove allocation id of this shard from the set of in-sync shard copies
     */
    void removeAllocationId(ShardRouting shardRouting) {
        if (shardRouting.active()) {
            changes(shardRouting.shardId()).removedAllocationIds.add(shardRouting.allocationId().getId());
        }
    }

    /**
     * Increase primary term for this shard id
     */
    private void increasePrimaryTerm(ShardId shardId) {
        changes(shardId).increaseTerm = true;
    }

    private static class Updates {
        private boolean increaseTerm; // whether primary term should be increased
        private Set<String> addedAllocationIds = new HashSet<>(); // allocation ids that should be added to the in-sync set
        private Set<String> removedAllocationIds = new HashSet<>(); // allocation ids that should be removed from the in-sync set
        private ShardRouting initializedPrimary = null; // primary that was initialized from unassigned
        private ShardRouting firstFailedPrimary = null; // first active primary that was failed
    }
}
