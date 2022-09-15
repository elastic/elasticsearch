/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

public class PrevalidateNodeRemovalRequest extends MasterNodeReadRequest<PrevalidateNodeRemovalRequest> {
    private final String[] nodeIds; // Maybe named just nodes?
    private DiscoveryNode[] concreteNodes;

    public PrevalidateNodeRemovalRequest(String... nodeIds) {
        this.nodeIds = nodeIds;
    }

    public PrevalidateNodeRemovalRequest(final StreamInput in) throws IOException {
        super(in);
        nodeIds = in.readStringArray();
    }

    public void setConcreteNodes(DiscoveryNode... discoveryNodes) {
        concreteNodes = discoveryNodes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (nodeIds == null) {
            out.writeVInt(0);
        } else {
            out.writeStringArray(nodeIds);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        // TODO: make sure all provided node IDs are in the cluster
        return null;
    }

    public List<DiscoveryNode> getConcreteNodes() {
        return List.of(concreteNodes);
    }

    public List<String> getNodeIds() {
        return List.of(nodeIds);
    }
}
