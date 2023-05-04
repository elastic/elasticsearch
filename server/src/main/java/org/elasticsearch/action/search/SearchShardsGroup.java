/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Represents a group of nodes that a given ShardId is allocated on, along with information about
 * whether this group might match the query or not.
 */
public class SearchShardsGroup implements Writeable {
    private final ShardId shardId;
    private final List<String> allocatedNodes;
    private final boolean preFiltered;
    private final boolean skipped;

    public SearchShardsGroup(ShardId shardId, List<String> allocatedNodes, boolean preFiltered, boolean skipped) {
        this.shardId = shardId;
        this.allocatedNodes = allocatedNodes;
        this.skipped = skipped;
        this.preFiltered = preFiltered;
        assert skipped == false || preFiltered;
    }

    public SearchShardsGroup(StreamInput in) throws IOException {
        this.shardId = new ShardId(in);
        this.allocatedNodes = in.readStringList();
        this.skipped = in.readBoolean();
        this.preFiltered = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeStringCollection(allocatedNodes);
        out.writeBoolean(skipped);
        out.writeBoolean(preFiltered);
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
    public boolean preFiltered() {
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
