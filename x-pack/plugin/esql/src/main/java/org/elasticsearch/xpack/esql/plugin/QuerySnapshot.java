/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.search.SearchContextIdForNode;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.TargetShard;
import org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.TargetShards;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * The snapshot of a query: a point in time on the local indices it reads, so every read of a shard sees the same version.
 * Opened and closed by {@link QuerySnapshots}.
 */
final class QuerySnapshot {
    private final BytesReference pointInTimeId;
    private final Map<ShardId, SearchContextIdForNode> shards;
    private final int totalShards;
    private final int skippedShards;
    private final Supplier<ThreadContext.StoredContext> owner;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * @param pointInTimeId the id of the point in time
     * @param shards        the reader of each shard it holds, and the node that holds it
     * @param totalShards   the shards the point in time targeted
     * @param skippedShards the shards can_match skipped, which have no reader
     * @param owner         the thread context of the user who runs the query, who closes it
     */
    QuerySnapshot(
        BytesReference pointInTimeId,
        Map<ShardId, SearchContextIdForNode> shards,
        int totalShards,
        int skippedShards,
        Supplier<ThreadContext.StoredContext> owner
    ) {
        this.pointInTimeId = pointInTimeId;
        this.shards = Map.copyOf(shards);
        this.totalShards = totalShards;
        this.skippedShards = skippedShards;
        this.owner = owner;
    }

    BytesReference pointInTimeId() {
        return pointInTimeId;
    }

    Map<ShardId, SearchContextIdForNode> shards() {
        return shards;
    }

    Supplier<ThreadContext.StoredContext> owner() {
        return owner;
    }

    /**
     * Whether any copy of {@code shard} can reopen its reader, rather than only the copy that holds it.
     */
    boolean reproducible(ShardId shard) {
        SearchContextIdForNode reader = shards.get(shard);
        return reader != null && reader.getSearchContextId().isRetryable();
    }

    /**
     * The shards to read: each on the node that holds its reader, then on the other copies if they can reopen it. Returns
     * fresh lists, without alias filters.
     */
    TargetShards targetShards(ProjectState project) {
        Map<ShardId, TargetShard> targets = new HashMap<>(shards.size());
        for (Map.Entry<ShardId, SearchContextIdForNode> entry : shards.entrySet()) {
            ShardId shardId = entry.getKey();
            IndexMetadata index = project.metadata().index(shardId.getIndex());
            // a deleted index has no copy to read, so its shard is reported unavailable and never sent
            TargetShard target = index == null
                ? new TargetShard(shardId, new ArrayList<>(), null, SplitShardCountSummary.IRRELEVANT)
                : new TargetShard(
                    shardId,
                    copies(project, shardId, entry.getValue()),
                    null,
                    SplitShardCountSummary.forSearch(index, shardId.id())
                );
            targets.put(shardId, target);
        }
        return new TargetShards(targets, totalShards, skippedShards);
    }

    private List<DiscoveryNode> copies(ProjectState project, ShardId shardId, SearchContextIdForNode reader) {
        DiscoveryNodes nodes = project.cluster().nodes();
        List<DiscoveryNode> copies = new ArrayList<>();
        DiscoveryNode holder = nodes.get(reader.getNode());
        if (holder != null) {
            copies.add(holder);
        }
        IndexRoutingTable routing = project.routingTable().index(shardId.getIndex());
        if (reproducible(shardId) && routing != null) {
            // the copies a search on a point in time falls back to
            for (ShardRouting copy : routing.shard(shardId.id()).activeInitializingShardsRandomIt()) {
                DiscoveryNode node = nodes.get(copy.currentNodeId());
                if (copy.isSearchable() && node != null && node.equals(holder) == false) {
                    copies.add(node);
                }
            }
        }
        return copies;
    }

    /**
     * Whether the caller is the first to close it.
     */
    boolean markClosed() {
        return closed.compareAndSet(false, true);
    }
}
