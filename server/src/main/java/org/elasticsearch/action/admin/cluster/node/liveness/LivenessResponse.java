/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.liveness;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Transport level private response for the transport handler registered under
 * {@value org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction#NAME}
 */
public final class LivenessResponse extends ActionResponse {

    private DiscoveryNode node;
    private ClusterName clusterName;

    public LivenessResponse() {}

    public LivenessResponse(StreamInput in) throws IOException {
        super(in);
        clusterName = new ClusterName(in);
        node = in.readOptionalWriteable(DiscoveryNode::new);
    }

    public LivenessResponse(ClusterName clusterName, DiscoveryNode node) {
        this.node = node;
        this.clusterName = clusterName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterName.writeTo(out);
        out.writeOptionalWriteable(node);
    }

    public ClusterName getClusterName() {
        return clusterName;
    }

    public DiscoveryNode getDiscoveryNode() {
        return node;
    }
}
