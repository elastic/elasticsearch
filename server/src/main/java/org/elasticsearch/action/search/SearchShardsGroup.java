/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Represents a group of nodes that a given ShardId is allocated on, along with information about
 * whether this group might match the query or not.
 */
public class SearchShardsGroup implements Writeable {
    private final ShardId shardId;
    private final List<String> allocatedNodes;
    private final boolean skipped;
    private final transient boolean preFiltered;

    public SearchShardsGroup(ShardId shardId, List<String> allocatedNodes, boolean skipped) {
        this.shardId = shardId;
        this.allocatedNodes = allocatedNodes;
        this.skipped = skipped;
        this.preFiltered = true;
    }

    /**
     * Create a new response from a legacy response from the cluster_search_shards API
     */
    SearchShardsGroup(ClusterSearchShardsGroup oldGroup) {
        this.shardId = oldGroup.getShardId();
        this.allocatedNodes = Arrays.stream(oldGroup.getShards()).map(ShardRouting::currentNodeId).toList();
        this.skipped = false;
        this.preFiltered = false;
    }

    public SearchShardsGroup(StreamInput in) throws IOException {
        this.shardId = new ShardId(in);
        this.allocatedNodes = in.readStringList();
        this.skipped = in.readBoolean();
        this.preFiltered = true;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (preFiltered == false) {
            assert false : "Serializing a response created from a legacy response is not allowed";
            throw new IllegalStateException("Serializing a response created from a legacy response is not allowed");
        }
        shardId.writeTo(out);
        out.writeStringCollection(allocatedNodes);
        out.writeBoolean(skipped);
    }

    public ShardId shardId() {
        return shardId;
    }

    /**
     * Returns true if the target shards in this group won't match the query given {@link SearchShardsRequest}.
     */
    public boolean skipped() {
        return skipped;
    }

    /**
     * Returns true if the can_match was performed against this group. This flag is for BWC purpose. It's always
     * true for a response from the new search_shards API; but always false for a response from the old API.
     */
    boolean preFiltered() {
        return preFiltered;
    }

    /**
     * The list of node ids that shard copies on this group are allocated on.
     */
    public List<String> allocatedNodes() {
        return allocatedNodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchShardsGroup group = (SearchShardsGroup) o;
        return skipped == group.skipped
            && preFiltered == group.preFiltered
            && shardId.equals(group.shardId)
            && allocatedNodes.equals(group.allocatedNodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, allocatedNodes, skipped, preFiltered);
    }
}
