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

public class ReceiveTimeoutTransportException extends ActionTransportException {

    public ReceiveTimeoutTransportException(DiscoveryNode node, String action, String msg) {
        super(node.getName(), node.getAddress(), action, msg, null);
    }

    public ReceiveTimeoutTransportException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        // stack trace is uninformative
        return this;
    }
}
