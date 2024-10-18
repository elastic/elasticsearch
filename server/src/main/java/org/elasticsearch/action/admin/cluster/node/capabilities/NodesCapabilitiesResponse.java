/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.capabilities;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class NodesCapabilitiesResponse extends BaseNodesResponse<NodeCapability> implements ToXContentFragment {
    protected NodesCapabilitiesResponse(ClusterName clusterName, List<NodeCapability> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodeCapability> readNodesFrom(StreamInput in) throws IOException {
        return TransportAction.localOnly();
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeCapability> nodes) throws IOException {
        TransportAction.localOnly();
    }

    public Optional<Boolean> isSupported() {
        if (hasFailures() || getNodes().isEmpty()) {
            // there's no nodes in the response (uh? what about ourselves?)
            // or there's a problem (hopefully transient) talking to one or more nodes.
            // We don't have enough information to decide if it's supported or not, so return unknown
            return Optional.empty();
        }

        return Optional.of(getNodes().stream().allMatch(NodeCapability::isSupported));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Optional<Boolean> supported = isSupported();
        return builder.field("supported", supported.orElse(null));
    }
}
