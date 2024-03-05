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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

/**
 * A base class for node level operations.
 */
public abstract class BaseNodeResponse extends TransportResponse {

    private final DiscoveryNode node;

    /**
     * Read a response from the given stream, re-using the given {@link DiscoveryNode} object if non-null.
     *
     * On the wire a {@link BaseNodeResponse} message starts with a {@link DiscoveryNode} identifying the original responder. If the sender
     * knows the identity of the responder already then we prefer to use that rather than reading the object from the wire, since {@link
     * DiscoveryNode} objects are sometimes quite large and yet they're immutable so there's no need to have multiple copies in memory.
     *
     * @param node the expected remote node, or {@code null} if not known.
     */
    protected BaseNodeResponse(StreamInput in, @Nullable DiscoveryNode node) throws IOException {
        super(in);
        final DiscoveryNode remoteNode = new DiscoveryNode(in);
        if (node == null) {
            this.node = remoteNode;
        } else {
            assert remoteNode.equals(node) : remoteNode + " vs " + node;
            this.node = node;
        }
    }

    /**
     * Read a response from the given stream, with no {@link DiscoveryNode} object re-use. Callers should not use this constructor if the
     * local node is known, and instead should call {@link #BaseNodeResponse(StreamInput, DiscoveryNode)}.
     */
    protected BaseNodeResponse(StreamInput in) throws IOException {
        this(in, null);
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
