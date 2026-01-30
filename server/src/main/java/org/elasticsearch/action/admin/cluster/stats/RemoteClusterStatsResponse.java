/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Set;

/**
 * Trimmed down cluster stats response for reporting to a remote cluster.
 */
public class RemoteClusterStatsResponse extends ActionResponse {
    final String clusterUUID;
    final ClusterHealthStatus status;
    private final Set<String> versions;
    private final long nodesCount;
    private final long shardsCount;
    private final long indicesCount;
    private final long indicesBytes;
    private final long heapBytes;
    private final long memBytes;

    public Set<String> getVersions() {
        return versions;
    }

    public long getNodesCount() {
        return nodesCount;
    }

    public long getShardsCount() {
        return shardsCount;
    }

    public long getIndicesCount() {
        return indicesCount;
    }

    public long getIndicesBytes() {
        return indicesBytes;
    }

    public long getHeapBytes() {
        return heapBytes;
    }

    public long getMemBytes() {
        return memBytes;
    }

    public RemoteClusterStatsResponse(
        String clusterUUID,
        ClusterHealthStatus status,
        Set<String> versions,
        long nodesCount,
        long shardsCount,
        long indicesCount,
        long indicesBytes,
        long heapBytes,
        long memBytes
    ) {
        this.clusterUUID = clusterUUID;
        this.status = status;
        this.versions = versions;
        this.nodesCount = nodesCount;
        this.shardsCount = shardsCount;
        this.indicesCount = indicesCount;
        this.indicesBytes = indicesBytes;
        this.heapBytes = heapBytes;
        this.memBytes = memBytes;
    }

    public String getClusterUUID() {
        return this.clusterUUID;
    }

    public ClusterHealthStatus getStatus() {
        return this.status;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(clusterUUID);
        status.writeTo(out);
        out.writeStringCollection(versions);
        out.writeLong(nodesCount);
        out.writeLong(shardsCount);
        out.writeLong(indicesCount);
        out.writeLong(indicesBytes);
        out.writeLong(heapBytes);
        out.writeLong(memBytes);
    }

    public RemoteClusterStatsResponse(StreamInput in) throws IOException {
        this.clusterUUID = in.readString();
        this.status = ClusterHealthStatus.readFrom(in);
        this.versions = in.readCollectionAsSet(StreamInput::readString);
        this.nodesCount = in.readLong();
        this.shardsCount = in.readLong();
        this.indicesCount = in.readLong();
        this.indicesBytes = in.readLong();
        this.heapBytes = in.readLong();
        this.memBytes = in.readLong();
    }
}
