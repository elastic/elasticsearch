/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.core.Nullable;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The decisions of the master about the shard tasks of one downsample operation, as pure functions of the shard task documents
 * and the cluster state, so they can be tested without a cluster.
 */
final class ShardTaskPlanner {

    private ShardTaskPlanner() {}

    /**
     * Where the shard tasks of one operation stand.
     */
    enum Outcome {
        IN_PROGRESS,
        ALL_COMPLETED,
        FAILED
    }

    /**
     * Decides what the master should write for one shard task.
     *
     * @param doc            the shard task as last read
     * @param clusterNodeIds the nodes currently in the cluster
     * @param candidates     the nodes that can run the task, most preferred first (see {@link #candidateNodes})
     * @param maxAttempts    how many times a task may fail with a retriable error before the master fails it for good
     * @return the new version of the document, or {@code null} if there is nothing to write
     */
    @Nullable
    static ShardTaskDoc plan(ShardTaskDoc doc, Set<String> clusterNodeIds, List<String> candidates, int maxAttempts) {
        return switch (doc.status()) {
            case COMPLETED -> null;
            case FAILED -> {
                if (doc.retriable() == false) {
                    yield null;
                }
                if (doc.attempt() + 1 >= maxAttempts) {
                    yield doc.failedByMaster(
                        "shard task failed [" + (doc.attempt() + 1) + "] times, the last error was [" + doc.lastError() + "]"
                    );
                }
                yield candidates.isEmpty() ? null : doc.assignTo(candidates.get(0), true);
            }
            case UNASSIGNED -> candidates.isEmpty() ? null : doc.assignTo(candidates.get(0), false);
            case ASSIGNED, RUNNING -> {
                if (clusterNodeIds.contains(doc.assignedNode())) {
                    yield null;
                }
                // The node left the cluster. This does not count as an attempt: like a persistent task, a shard task survives
                // any number of node restarts (e.g. a rolling restart). Failures are counted when a node reports one.
                yield candidates.isEmpty() ? doc.unassign() : doc.assignTo(candidates.get(0), false);
            }
        };
    }

    /**
     * Whether the operation can move on, has to wait, or has failed.
     */
    static Outcome outcome(Collection<ShardTaskDoc> docs) {
        boolean allCompleted = true;
        for (ShardTaskDoc doc : docs) {
            if (doc.status() == ShardTaskStatus.FAILED && doc.retriable() == false) {
                return Outcome.FAILED;
            }
            allCompleted &= doc.status() == ShardTaskStatus.COMPLETED;
        }
        return allCompleted ? Outcome.ALL_COMPLETED : Outcome.IN_PROGRESS;
    }

    /**
     * The nodes that can run the task of one source shard: those with a started, searchable copy of it, since the shard is read
     * locally. The least loaded comes first, because shard tasks are long running.
     *
     * Nodes marked for shutdown are not candidates, like for persistent tasks; a task already running on such a node stays there
     * until the node leaves (the downsample persistent task does not move on shutdown either).
     *
     * @param shardRoutingTable the routing of the source shard, {@code null} if the source index is gone
     * @param load              the number of shard tasks currently owned by each node
     * @param shuttingDown      the ids of the nodes marked for shutdown
     */
    static List<String> candidateNodes(
        @Nullable IndexShardRoutingTable shardRoutingTable,
        Map<String, Integer> load,
        Set<String> shuttingDown
    ) {
        if (shardRoutingTable == null) {
            return List.of();
        }
        return shardRoutingTable.activeShards()
            .stream()
            .filter(shard -> shard.started() && shard.isSearchable())
            .map(ShardRouting::currentNodeId)
            .filter(node -> shuttingDown.contains(node) == false)
            .distinct()
            .sorted(Comparator.<String>comparingInt(node -> load.getOrDefault(node, 0)).thenComparing(Comparator.naturalOrder()))
            .toList();
    }

    /**
     * The number of shard tasks each node currently owns.
     */
    static Map<String, Integer> load(Collection<ShardTaskDoc> docs) {
        Map<String, Integer> load = new HashMap<>();
        for (ShardTaskDoc doc : docs) {
            if (doc.status().isOwnedByNode()) {
                load.merge(doc.assignedNode(), 1, Integer::sum);
            }
        }
        return load;
    }
}
