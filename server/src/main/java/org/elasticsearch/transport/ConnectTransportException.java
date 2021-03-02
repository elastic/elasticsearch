/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ConnectTransportException extends ActionTransportException {

    private final DiscoveryNode node;

    public ConnectTransportException(DiscoveryNode node, String msg) {
        this(node, msg, null, null);
    }

    public ConnectTransportException(DiscoveryNode node, String msg, String action) {
        this(node, msg, action, null);
    }

    public ConnectTransportException(DiscoveryNode node, String msg, Throwable cause) {
        this(node, msg, null, cause);
    }

    public ConnectTransportException(DiscoveryNode node, String msg, String action, Throwable cause) {
        super(node == null ? null : node.getName(), node == null ? null : node.getAddress(), action, msg, cause);
        this.node = node;
    }

    public ConnectTransportException(StreamInput in) throws IOException {
        super(in);
        node = in.readOptionalWriteable(DiscoveryNode::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(node);
    }

    public DiscoveryNode node() {
        return node;
    }
}
