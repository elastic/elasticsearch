/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.io.IOException;
import java.util.Objects;

/**
 * The reader that the segment and doc id of a {@link DocRefBlock} row belong to. The shard index of a {@link DocVector}
 * only means something on the node that opened the shard. An origin names the cluster, the node, the shard and the
 * reader context, so the document can still be loaded after the row has left that node.
 */
public final class DocRefOrigin implements Writeable, Accountable {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DocRefOrigin.class);
    private static final long SHARD_ID_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ShardId.class) + RamUsageEstimator
        .shallowSizeOfInstance(Index.class);
    private static final long CONTEXT_ID_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ShardSearchContextId.class);

    private final String clusterAlias;
    private final String nodeId;
    private final ShardId shardId;
    private final ShardSearchContextId contextId;
    // computed once: a TopN hashes the origin of every row it interns
    private final int hash;

    /**
     * @param clusterAlias the cluster that owns the reader, {@code ""} for the local cluster
     * @param nodeId       the node that owns the reader
     * @param shardId      the shard the reader reads
     * @param contextId    the reader context on that node
     */
    public DocRefOrigin(String clusterAlias, String nodeId, ShardId shardId, ShardSearchContextId contextId) {
        this.clusterAlias = Objects.requireNonNull(clusterAlias, "clusterAlias");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.contextId = Objects.requireNonNull(contextId, "contextId");
        this.hash = Objects.hash(clusterAlias, nodeId, shardId, contextId);
    }

    public static DocRefOrigin readFrom(StreamInput in) throws IOException {
        return new DocRefOrigin(in.readString(), in.readString(), new ShardId(in), new ShardSearchContextId(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(clusterAlias);
        out.writeString(nodeId);
        shardId.writeTo(out);
        contextId.writeTo(out);
    }

    public String clusterAlias() {
        return clusterAlias;
    }

    public String nodeId() {
        return nodeId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public ShardSearchContextId contextId() {
        return contextId;
    }

    /**
     * Does the reader live on {@code localNodeId} in the local cluster?
     */
    public boolean isLocal(String localNodeId) {
        return clusterAlias.isEmpty() && nodeId.equals(localNodeId);
    }

    /**
     * {@code [index][shard]}: the shard without the node and session ids, for messages users see.
     */
    public String describeForUser() {
        return shardId.toString();
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(clusterAlias) + RamUsageEstimator.sizeOf(nodeId) + SHARD_ID_RAM_BYTES_USED
            + RamUsageEstimator.sizeOf(shardId.getIndexName()) + RamUsageEstimator.sizeOf(shardId.getIndex().getUUID())
            + CONTEXT_ID_RAM_BYTES_USED + RamUsageEstimator.sizeOf(contextId.getSessionId()) + RamUsageEstimator.sizeOf(
                contextId.getSearcherId()
            );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof DocRefOrigin == false) {
            return false;
        }
        DocRefOrigin other = (DocRefOrigin) o;
        return hash == other.hash
            && clusterAlias.equals(other.clusterAlias)
            && nodeId.equals(other.nodeId)
            && shardId.equals(other.shardId)
            && contextId.equals(other.contextId);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return "DocRefOrigin[" + clusterAlias + ":" + nodeId + shardId + contextId + "]";
    }
}
