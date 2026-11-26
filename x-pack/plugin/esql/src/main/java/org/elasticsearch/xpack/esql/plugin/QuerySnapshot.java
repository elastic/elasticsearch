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
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * The snapshot of a query: a point in time on the local indices it reads, so every read of a shard sees the same version.
 * Opened and closed by {@link QuerySnapshots}.
 */
public final class QuerySnapshot {
    private final BytesReference pointInTimeId;
    private final Map<ShardId, SearchContextIdForNode> shards;
    private final Supplier<ThreadContext.StoredContext> owner;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * @param pointInTimeId the id of the point in time
     * @param shards        the reader of each shard it holds, and the node that holds it
     * @param owner         the thread context of the user who runs the query, who closes it
     */
    QuerySnapshot(BytesReference pointInTimeId, Map<ShardId, SearchContextIdForNode> shards, Supplier<ThreadContext.StoredContext> owner) {
        this.pointInTimeId = pointInTimeId;
        this.shards = Map.copyOf(shards);
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
        return reader != null && reader.getSearchContextId() != null && reader.getSearchContextId().isRetryable();
    }

    /**
     * The shards of {@code indices} to read: each on the node that holds its reader, then on the other copies if they can
     * reopen it. A shard without a reader was skipped by can_match. Returns fresh lists, without alias filters.
     */
    TargetShards targetShards(ProjectState project, Set<String> indices) {
        DiscoveryNodes nodes = project.cluster().nodes();
        Map<ShardId, TargetShard> targets = new HashMap<>();
        for (Map.Entry<ShardId, SearchContextIdForNode> entry : shards.entrySet()) {
            ShardId shardId = entry.getKey();
            SearchContextIdForNode reader = entry.getValue();
            if (indices.contains(shardId.getIndexName()) == false || reader.getSearchContextId() == null) {
                continue;
            }
            List<DiscoveryNode> copies = new ArrayList<>();
            DiscoveryNode holder = reader.getNode() == null ? null : nodes.get(reader.getNode());
            if (holder != null) {
                copies.add(holder);
            }
            IndexShardRoutingTable routing = shardRouting(project, shardId);
            if (reproducible(shardId) && routing != null) {
                for (int i = 0; i < routing.size(); i++) {
                    ShardRouting copy = routing.shard(i);
                    DiscoveryNode node = copy.currentNodeId() == null ? null : nodes.get(copy.currentNodeId());
                    if (copy.isSearchable() && node != null && node.equals(holder) == false) {
                        copies.add(node);
                    }
                }
            }
            IndexMetadata index = project.metadata().index(shardId.getIndex());
            SplitShardCountSummary summary = index == null
                ? SplitShardCountSummary.IRRELEVANT
                : SplitShardCountSummary.forSearch(index, shardId.id());
            targets.put(shardId, new TargetShard(shardId, copies, null, summary));
        }
        int total = targets.size();
        for (String name : indices) {
            IndexRoutingTable index = project.routingTable().index(name);
            if (index != null) {
                for (int i = 0; i < index.size(); i++) {
                    if (targets.containsKey(index.shard(i).shardId()) == false) {
                        total++;
                    }
                }
            }
        }
        return new TargetShards(targets, total, total - targets.size());
    }

    private static IndexShardRoutingTable shardRouting(ProjectState project, ShardId shardId) {
        IndexRoutingTable index = project.routingTable().index(shardId.getIndex());
        return index == null || shardId.id() >= index.size() ? null : index.shard(shardId.id());
    }

    /**
     * Whether the caller is the first to close it.
     */
    boolean markClosed() {
        return closed.compareAndSet(false, true);
    }
}
