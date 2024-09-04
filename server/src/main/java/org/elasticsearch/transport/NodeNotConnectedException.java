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

/**
 * An exception indicating that a message is sent to a node that is not connected.
 *
 *
 */
public class NodeNotConnectedException extends ConnectTransportException {

    public NodeNotConnectedException(DiscoveryNode node, String msg) {
        super(node, msg, (String) null);
    }

    public NodeNotConnectedException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Throwable fillInStackTrace() {
        return this; // this exception doesn't imply a bug, no need for a stack trace
    }
}
