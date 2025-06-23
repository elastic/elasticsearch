/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.capabilities;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class NodeCapability extends BaseNodeResponse {

    private final boolean supported;

    public NodeCapability(StreamInput in) throws IOException {
        super(in);

        supported = in.readBoolean();
    }

    public NodeCapability(boolean supported, DiscoveryNode node) {
        super(node);
        this.supported = supported;
    }

    public boolean isSupported() {
        return supported;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeBoolean(supported);
    }

    @Override
    public String toString() {
        return "NodeCapability{supported=" + supported + '}';
    }
}
