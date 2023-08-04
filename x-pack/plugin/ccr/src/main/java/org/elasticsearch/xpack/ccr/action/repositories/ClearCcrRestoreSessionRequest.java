/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.RemoteClusterAwareRequest;

import java.io.IOException;

import static org.elasticsearch.transport.RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY_CCR;

public class ClearCcrRestoreSessionRequest extends ActionRequest implements RemoteClusterAwareRequest, IndicesRequest {

    private DiscoveryNode node;
    private final String sessionUUID;
    private final ShardId shardId;

    ClearCcrRestoreSessionRequest(StreamInput in) throws IOException {
        super(in);
        sessionUUID = in.readString();
        if (in.getTransportVersion().onOrAfter(TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY_CCR)) {
            shardId = new ShardId(in);
        } else {
            shardId = null;
        }
    }

    public ClearCcrRestoreSessionRequest(String sessionUUID, DiscoveryNode node, ShardId shardId) {
        this.sessionUUID = sessionUUID;
        this.node = node;
        this.shardId = shardId;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sessionUUID);
        if (out.getTransportVersion().onOrAfter(TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY_CCR)) {
            shardId.writeTo(out);
        }
    }

    String getSessionUUID() {
        return sessionUUID;
    }

    ShardId getShardId() {
        return shardId;
    }

    @Override
    public DiscoveryNode getPreferredTargetNode() {
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
