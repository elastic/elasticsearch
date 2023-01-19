/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * The response object that will be returned when clearing the cache of native roles
 */
public class ClearRolesCacheResponse extends BaseNodesResponse<ClearRolesCacheResponse.Node> implements ToXContentFragment {

    public ClearRolesCacheResponse(StreamInput in) throws IOException {
        super(in);
    }

    public ClearRolesCacheResponse(ClusterName clusterName, List<Node> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<ClearRolesCacheResponse.Node> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(Node::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<ClearRolesCacheResponse.Node> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (ClearRolesCacheResponse.Node node : getNodes()) {
            builder.startObject(node.getNode().getId());
            builder.field("name", node.getNode().getName());
            builder.endObject();
        }
        builder.endObject();

        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class Node extends BaseNodeResponse {

        public Node(StreamInput in) throws IOException {
            super(in);
        }

        public Node(DiscoveryNode node) {
            super(node);
        }
    }
}
