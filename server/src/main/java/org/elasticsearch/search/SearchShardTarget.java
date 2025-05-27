/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.IOException;
import java.util.Objects;

/**
 * The target that the search request was executed on.
 */
public final class SearchShardTarget implements Writeable, Comparable<SearchShardTarget> {
    private final Text nodeId;
    private final ShardId shardId;
    private final String clusterAlias;

    public SearchShardTarget(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            nodeId = in.readText();
        } else {
            nodeId = null;
        }
        shardId = new ShardId(in);
        clusterAlias = in.readOptionalString();
    }

    public SearchShardTarget(String nodeId, ShardId shardId, @Nullable String clusterAlias) {
        this.nodeId = nodeId == null ? null : new Text(nodeId);
        this.shardId = shardId;
        this.clusterAlias = clusterAlias;
    }

    @Nullable
    public String getNodeId() {
        return nodeId != null ? nodeId.string() : null;
    }

    public Text getNodeIdText() {
        return this.nodeId;
    }

    public String getIndex() {
        return shardId.getIndexName();
    }

    public ShardId getShardId() {
        return shardId;
    }

    @Nullable
    public String getClusterAlias() {
        return clusterAlias;
    }

    /**
     * Returns the fully qualified index name, including the index prefix that indicates which cluster results come from.
     */
    public String getFullyQualifiedIndexName() {
        return RemoteClusterAware.buildRemoteIndexName(clusterAlias, getIndex());
    }

    @Override
    public int compareTo(SearchShardTarget o) {
        int i = shardId.getIndexName().compareTo(o.getIndex());
        if (i == 0) {
            i = shardId.getId() - o.shardId.id();
        }
        return i;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (nodeId == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeText(nodeId);
        }
        shardId.writeTo(out);
        out.writeOptionalString(clusterAlias);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SearchShardTarget that = (SearchShardTarget) o;
        return Objects.equals(nodeId, that.nodeId)
            && Objects.equals(shardId, that.shardId)
            && Objects.equals(clusterAlias, that.clusterAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, shardId, clusterAlias);
    }

    /**
     * NOTE: this representation is used as the "id" for shards for the REST response
     * when query profiling is requested. So changing this formulation may break
     * systems that rely on the format, including the parser in SearchProfileResults.
     */
    @Override
    public String toString() {
        String shardToString = "["
            + RemoteClusterAware.buildRemoteIndexName(clusterAlias, shardId.getIndexName())
            + "]["
            + shardId.getId()
            + "]";
        if (nodeId == null) {
            return "[_na_]" + shardToString;
        }
        return "[" + nodeId + "]" + shardToString;
    }
}
