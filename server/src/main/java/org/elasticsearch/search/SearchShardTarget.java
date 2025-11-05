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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.Text;

import java.io.IOException;
import java.util.Objects;

/**
 * The target that the search request was executed on.
 */
public final class SearchShardTarget implements Writeable, Comparable<SearchShardTarget> {
    private final Text nodeId;
    private final ShardId shardId;
    private final String clusterAlias;

    /**
     * Reads a SearchShardTarget from the provided stream input.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs during deserialization
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * StreamInput in = ...;
     * SearchShardTarget target = new SearchShardTarget(in);
     * }</pre>
     */
    public SearchShardTarget(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            nodeId = in.readText();
        } else {
            nodeId = null;
        }
        shardId = new ShardId(in);
        clusterAlias = in.readOptionalString();
    }

    /**
     * Constructs a new SearchShardTarget with the specified node ID, shard ID, and cluster alias.
     *
     * @param nodeId the node identifier (may be null)
     * @param shardId the shard identifier
     * @param clusterAlias the cluster alias for cross-cluster search (may be null for local clusters)
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ShardId shardId = new ShardId("my_index", "_na_", 0);
     * SearchShardTarget target = new SearchShardTarget("node1", shardId, null);
     * }</pre>
     */
    public SearchShardTarget(String nodeId, ShardId shardId, @Nullable String clusterAlias) {
        this.nodeId = nodeId == null ? null : new Text(nodeId);
        this.shardId = shardId;
        this.clusterAlias = clusterAlias;
    }

    /**
     * Returns the node identifier where this shard resides.
     *
     * @return the node ID, or null if not set
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchShardTarget target = ...;
     * String nodeId = target.getNodeId();
     * System.out.println("Shard on node: " + nodeId);
     * }</pre>
     */
    @Nullable
    public String getNodeId() {
        return nodeId != null ? nodeId.string() : null;
    }

    /**
     * Returns the node identifier as a Text object.
     *
     * @return the node ID as Text, or null if not set
     */
    public Text getNodeIdText() {
        return this.nodeId;
    }

    /**
     * Returns the name of the index this shard belongs to.
     *
     * @return the index name
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchShardTarget target = ...;
     * String indexName = target.getIndex();
     * System.out.println("Index: " + indexName);
     * }</pre>
     */
    public String getIndex() {
        return shardId.getIndexName();
    }

    /**
     * Returns the shard identifier for this target.
     *
     * @return the ShardId
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchShardTarget target = ...;
     * ShardId shardId = target.getShardId();
     * System.out.println("Shard ID: " + shardId.getId());
     * System.out.println("Index: " + shardId.getIndexName());
     * }</pre>
     */
    public ShardId getShardId() {
        return shardId;
    }

    /**
     * Returns the cluster alias for cross-cluster search scenarios.
     *
     * @return the cluster alias, or null for local clusters
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchShardTarget target = ...;
     * String cluster = target.getClusterAlias();
     * if (cluster != null) {
     *     System.out.println("From remote cluster: " + cluster);
     * }
     * }</pre>
     */
    @Nullable
    public String getClusterAlias() {
        return clusterAlias;
    }

    /**
     * Returns the fully qualified index name, including the cluster prefix for remote clusters.
     * For local clusters, this returns just the index name.
     * For remote clusters, this returns "cluster:index".
     *
     * @return the fully qualified index name
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SearchShardTarget target = ...;
     * String fqIndexName = target.getFullyQualifiedIndexName();
     * // For local: "my_index"
     * // For remote: "remote_cluster:my_index"
     * System.out.println("Fully qualified index: " + fqIndexName);
     * }</pre>
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
