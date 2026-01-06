/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexReshardService;
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
    private final SplitShardCountSummary reshardSplitShardCountSummary;
    private final transient boolean preFiltered;

    public SearchShardsGroup(
        ShardId shardId,
        List<String> allocatedNodes,
        boolean skipped,
        SplitShardCountSummary reshardSplitShardCountSummary
    ) {
        this.shardId = shardId;
        this.allocatedNodes = allocatedNodes;
        this.skipped = skipped;
        this.reshardSplitShardCountSummary = reshardSplitShardCountSummary;
        this.preFiltered = true;
    }

    /**
     * Create a new response from a legacy response from the cluster_search_shards API
     */
    SearchShardsGroup(ClusterSearchShardsGroup oldGroup) {
        this.shardId = oldGroup.getShardId();
        this.allocatedNodes = Arrays.stream(oldGroup.getShards()).map(ShardRouting::currentNodeId).toList();
        this.skipped = false;
        // This value is specific to resharding feature and this code path is specific to CCS
        // involving 8.x remote cluster.
        // We don't currently expect resharding to be used in such conditions so it's unset.
        this.reshardSplitShardCountSummary = SplitShardCountSummary.UNSET;
        this.preFiltered = false;
    }

    public SearchShardsGroup(StreamInput in) throws IOException {
        this.shardId = new ShardId(in);
        this.allocatedNodes = in.readStringCollectionAsList();
        this.skipped = in.readBoolean();
        this.reshardSplitShardCountSummary = in.getTransportVersion().supports(IndexReshardService.RESHARDING_SHARD_SUMMARY_IN_ESQL)
            ? SplitShardCountSummary.fromInt(in.readVInt())
            : SplitShardCountSummary.UNSET;
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
        if (out.getTransportVersion().supports(IndexReshardService.RESHARDING_SHARD_SUMMARY_IN_ESQL)) {
            reshardSplitShardCountSummary.writeTo(out);
        }
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

    public SplitShardCountSummary reshardSplitShardCountSummary() {
        return reshardSplitShardCountSummary;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        SearchShardsGroup that = (SearchShardsGroup) o;
        return skipped == that.skipped
            && preFiltered == that.preFiltered
            && Objects.equals(shardId, that.shardId)
            && Objects.equals(allocatedNodes, that.allocatedNodes)
            && Objects.equals(reshardSplitShardCountSummary, that.reshardSplitShardCountSummary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, allocatedNodes, skipped, reshardSplitShardCountSummary, preFiltered);
    }

    @Override
    public String toString() {
        return "SearchShardsGroup{"
            + "shardId="
            + shardId
            + ", allocatedNodes="
            + allocatedNodes
            + ", skipped="
            + skipped
            + ", preFiltered="
            + preFiltered
            + '}';
    }
}
