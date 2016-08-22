/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Observer that tracks changes made to RoutingNodes in order to update the primary terms and in-sync allocation ids in
 * {@link IndexMetaData} once the allocation round has completed.
 *
 * Primary terms are updated on primary initialization or primary promotion.
 *
 * Allocation ids are added for shards that become active and removed for shards that stop being active.
 */
public class IndexMetaDataUpdater extends RoutingChangesObserver.AbstractRoutingChangesObserver {
    private final Map<ShardId, Updates> shardChanges = new HashMap<>();

    @Override
    public void shardInitialized(ShardRouting unassignedShard) {
        if (unassignedShard.primary()) {
            increasePrimaryTerm(unassignedShard);
        }
    }

    @Override
    public void replicaPromoted(ShardRouting replicaShard) {
        increasePrimaryTerm(replicaShard);
    }

    @Override
    public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
        addAllocationId(startedShard);
    }

    @Override
    public void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {
        if (failedShard.active()) {
            removeAllocationId(failedShard);
        }
    }

    @Override
    public void relocationCompleted(ShardRouting removedRelocationSource) {
        removeAllocationId(removedRelocationSource);
    }

    @Override
    public void startedPrimaryReinitialized(ShardRouting startedPrimaryShard, ShardRouting initializedShard) {
        removeAllocationId(startedPrimaryShard);
    }

    /**
     * Updates the current {@link MetaData} based on the changes of this RoutingChangesObserver. Specifically
     * we update {@link IndexMetaData#getActiveAllocationIds()} and {@link IndexMetaData#primaryTerm(int)} based on
     * the changes made during this allocation.
     *
     * @param oldMetaData {@link MetaData} object from before the routing nodes was changed.
     * @return adapted {@link MetaData}, potentially the original one if no change was needed.
     */
    public MetaData applyChanges(MetaData oldMetaData) {
        Map<Index, List<Map.Entry<ShardId, Updates>>> changesGroupedByIndex =
            shardChanges.entrySet().stream().collect(Collectors.groupingBy(e -> e.getKey().getIndex()));

        MetaData.Builder metaDataBuilder = null;
        for (Map.Entry<Index, List<Map.Entry<ShardId, Updates>>> indexChanges : changesGroupedByIndex.entrySet()) {
            Index index = indexChanges.getKey();
            final IndexMetaData oldIndexMetaData = oldMetaData.index(index);
            if (oldIndexMetaData == null) {
                throw new IllegalStateException("no metadata found for index " + index);
            }
            IndexMetaData.Builder indexMetaDataBuilder = null;
            for (Map.Entry<ShardId, Updates> shardEntry : indexChanges.getValue()) {
                ShardId shardId = shardEntry.getKey();
                Updates updates = shardEntry.getValue();

                assert Sets.haveEmptyIntersection(updates.addedAllocationIds, updates.removedAllocationIds) :
                    "Allocation ids cannot be both added and removed in the same allocation round, added ids: " +
                        updates.addedAllocationIds + ", removed ids: " + updates.removedAllocationIds;

                Set<String> activeAllocationIds = new HashSet<>(oldIndexMetaData.activeAllocationIds(shardId.id()));
                activeAllocationIds.addAll(updates.addedAllocationIds);
                activeAllocationIds.removeAll(updates.removedAllocationIds);
                // only update active allocation ids if there is an active shard
                if (activeAllocationIds.isEmpty() == false) {
                    if (indexMetaDataBuilder == null) {
                        indexMetaDataBuilder = IndexMetaData.builder(oldIndexMetaData);
                    }
                    indexMetaDataBuilder.putActiveAllocationIds(shardId.id(), activeAllocationIds);
                }

                if (updates.increaseTerm) {
                    if (indexMetaDataBuilder == null) {
                        indexMetaDataBuilder = IndexMetaData.builder(oldIndexMetaData);
                    }
                    indexMetaDataBuilder.primaryTerm(shardId.id(), oldIndexMetaData.primaryTerm(shardId.id()) + 1);
                }
            }

            if (indexMetaDataBuilder != null) {
                if (metaDataBuilder == null) {
                    metaDataBuilder = MetaData.builder(oldMetaData);
                }
                metaDataBuilder.put(indexMetaDataBuilder);
            }
        }

        if (metaDataBuilder != null) {
            return metaDataBuilder.build();
        } else {
            return oldMetaData;
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
    private void removeAllocationId(ShardRouting shardRouting) {
        changes(shardRouting.shardId()).removedAllocationIds.add(shardRouting.allocationId().getId());
    }

    /**
     * Add allocation id of this shard to the set of in-sync shard copies
     */
    private void addAllocationId(ShardRouting shardRouting) {
        changes(shardRouting.shardId()).addedAllocationIds.add(shardRouting.allocationId().getId());
    }

    /**
     * Increase primary term for this shard id
     */
    private void increasePrimaryTerm(ShardRouting shardRouting) {
        changes(shardRouting.shardId()).increaseTerm = true;
    }

    private static class Updates {
        private boolean increaseTerm; // whether primary term should be increased
        private Set<String> addedAllocationIds = new HashSet<>(); // allocation ids that should be added to the in-sync set
        private Set<String> removedAllocationIds = new HashSet<>(); // allocation ids that should be removed from the in-sync set
    }
}
