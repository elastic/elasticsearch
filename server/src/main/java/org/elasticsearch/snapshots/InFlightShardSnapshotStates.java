/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Holds information about currently in-flight shard level snapshot or clone operations on a per-shard level.
 * Concretely, this means information on which shards are actively being written to in the repository currently
 * as well as the latest written shard generation per shard in case there is a shard generation for a shard that has
 * been cleanly written out to the repository but not yet made part of the current {@link org.elasticsearch.repositories.RepositoryData}
 * through a snapshot finalization.
 */
public final class InFlightShardSnapshotStates {

    private static final InFlightShardSnapshotStates EMPTY = new InFlightShardSnapshotStates(Map.of(), Map.of());

    /**
     * Compute information about all shard ids that currently have in-flight state for the given repository.
     *
     * @param snapshots snapshots in progress for a single repository
     * @return in flight shard states for all snapshot operations
     */
    public static InFlightShardSnapshotStates forEntries(List<SnapshotsInProgress.Entry> snapshots) {
        if (snapshots.isEmpty()) {
            return EMPTY;
        }
        final Map<String, Map<Integer, ShardGeneration>> generations = new HashMap<>();
        final Map<String, Set<Integer>> busyIds = new HashMap<>();
        assert snapshots.stream().map(SnapshotsInProgress.Entry::repository).distinct().count() == 1
            : "snapshots must either be an empty list or all belong to the same repository but saw " + snapshots;
        for (SnapshotsInProgress.Entry runningSnapshot : snapshots) {
            for (Map.Entry<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> shard : runningSnapshot
                .shardSnapshotStatusByRepoShardId()
                .entrySet()) {
                final RepositoryShardId sid = shard.getKey();
                addStateInformation(generations, busyIds, shard.getValue(), sid.shardId(), sid.indexName());
            }
        }
        return new InFlightShardSnapshotStates(generations, busyIds);
    }

    private static void addStateInformation(
        Map<String, Map<Integer, ShardGeneration>> generations,
        Map<String, Set<Integer>> busyIds,
        SnapshotsInProgress.ShardSnapshotStatus shardState,
        int shardId,
        String indexName
    ) {
        if (shardState.isActive()) {
            busyIds.computeIfAbsent(indexName, k -> new HashSet<>()).add(shardId);
            assert assertGenerationConsistency(generations, indexName, shardId, shardState.generation());
        } else if (shardState.state() == SnapshotsInProgress.ShardState.SUCCESS) {
            assert busyIds.getOrDefault(indexName, Collections.emptySet()).contains(shardId) == false
                : "Can't have a successful operation queued after an in-progress operation";
            generations.computeIfAbsent(indexName, k -> new HashMap<>()).put(shardId, shardState.generation());
        }
    }

    /**
     * Map that maps index name to a nested map of shard id to most recent successful shard generation for that
     * shard id.
     */
    private final Map<String, Map<Integer, ShardGeneration>> generations;

    /**
     * Map of index name to a set of shard ids that currently are actively executing an operation on the repository.
     */
    private final Map<String, Set<Integer>> activeShardIds;

    private InFlightShardSnapshotStates(Map<String, Map<Integer, ShardGeneration>> generations, Map<String, Set<Integer>> activeShardIds) {
        this.generations = generations;
        this.activeShardIds = activeShardIds;
    }

    private static boolean assertGenerationConsistency(
        Map<String, Map<Integer, ShardGeneration>> generations,
        String indexName,
        int shardId,
        @Nullable ShardGeneration activeGeneration
    ) {
        final ShardGeneration bestGeneration = generations.getOrDefault(indexName, Collections.emptyMap()).get(shardId);
        assert bestGeneration == null || activeGeneration == null || activeGeneration.equals(bestGeneration)
            : "[" + indexName + "][" + shardId + "]: " + bestGeneration + " vs " + activeGeneration;
        return true;
    }

    /**
     * Check if a given shard currently has an actively executing shard operation.
     *
     * @param indexName name of the shard's index
     * @param shardId   shard id of the shard
     * @return true if shard has an actively executing shard operation
     */
    boolean isActive(String indexName, int shardId) {
        return activeShardIds.getOrDefault(indexName, Collections.emptySet()).contains(shardId);
    }

    /**
     * Determine the current generation for a shard by first checking if any in-flight but successful new shard
     * snapshots or clones have set a relevant generation and then falling back to {@link ShardGenerations#getShardGen}
     * if not.
     *
     * @param indexId          index id of the shard
     * @param shardId          shard id of the shard
     * @param shardGenerations current shard generations in the repository data
     * @return most recent shard generation for the given shard
     */
    @Nullable
    ShardGeneration generationForShard(IndexId indexId, int shardId, ShardGenerations shardGenerations) {
        final ShardGeneration inFlightBest = generations.getOrDefault(indexId.getName(), Collections.emptyMap()).get(shardId);
        if (inFlightBest != null) {
            return inFlightBest;
        }
        return shardGenerations.getShardGen(indexId, shardId);
    }
}
