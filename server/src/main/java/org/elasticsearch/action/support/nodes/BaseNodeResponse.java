/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

/**
 * A base class for node level operations.
 */
public abstract class BaseNodeResponse extends TransportResponse {

    private DiscoveryNode node;

    protected BaseNodeResponse(StreamInput in) throws IOException {
        super(in);
        node = new DiscoveryNode(in);
    }

    protected BaseNodeResponse(DiscoveryNode node) {
        assert node != null;
        this.node = node;
    }

    /**
     * The node this information relates to.
     */
    public DiscoveryNode getNode() {
        return node;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        node.writeTo(out);
    }
}
