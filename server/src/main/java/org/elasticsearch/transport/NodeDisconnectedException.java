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

import java.io.IOException;

public class NodeDisconnectedException extends ConnectTransportException {

    public NodeDisconnectedException(DiscoveryNode node, String msg, String action, Exception cause) {
        super(node, msg, action, cause);
    }

    public NodeDisconnectedException(DiscoveryNode node, String action) {
        super(node, "disconnected", action, null);
    }

    public NodeDisconnectedException(StreamInput in) throws IOException {
        super(in);
    }

    // stack trace is meaningless...

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
