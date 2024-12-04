/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

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

    @Override
    public RestStatus status() {
        if (getMessage().equals("connection already closed")) {
            return RestStatus.BAD_GATEWAY;
        } else {
            // At the moment, there's no scenario where this Exception is thrown with a
            // different message. However, to be on the safer side, this alternate branch
            // is included that returns the default status code.
            return RestStatus.INTERNAL_SERVER_ERROR;
        }
    }
}
