/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.RemoteClusterAwareRequest;

import java.io.IOException;

import static org.elasticsearch.xpack.ccr.Ccr.TRANSPORT_VERSION_ACTION_WITH_SHARD_ID;

public class GetCcrRestoreFileChunkRequest extends LegacyActionRequest implements RemoteClusterAwareRequest, IndicesRequest {

    private final DiscoveryNode node;
    private final String sessionUUID;
    private final String fileName;
    private final int size;
    private final ShardId shardId;

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public GetCcrRestoreFileChunkRequest(DiscoveryNode node, String sessionUUID, String fileName, int size, ShardId shardId) {
        this.node = node;
        this.sessionUUID = sessionUUID;
        this.fileName = fileName;
        this.size = size;
        assert size > -1 : "The file chunk request size must be positive. Found: [" + size + "].";
        this.shardId = shardId;
    }

    GetCcrRestoreFileChunkRequest(StreamInput in) throws IOException {
        super(in);
        node = null;
        sessionUUID = in.readString();
        fileName = in.readString();
        size = in.readVInt();
        if (in.getTransportVersion().onOrAfter(TRANSPORT_VERSION_ACTION_WITH_SHARD_ID)) {
            shardId = new ShardId(in);
        } else {
            shardId = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sessionUUID);
        out.writeString(fileName);
        out.writeVInt(size);
        if (out.getTransportVersion().onOrAfter(TRANSPORT_VERSION_ACTION_WITH_SHARD_ID)) {
            shardId.writeTo(out);
        }
    }

    String getSessionUUID() {
        return sessionUUID;
    }

    String getFileName() {
        return fileName;
    }

    int getSize() {
        return size;
    }

    ShardId getShardId() {
        return shardId;
    }

    @Override
    public DiscoveryNode getPreferredTargetNode() {
        assert node != null : "Target node is null";
        return node;
    }

    @Override
    public String[] indices() {
        if (shardId == null) {
            return null;
        } else {
            return new String[] { shardId.getIndexName() };
        }
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }
}
