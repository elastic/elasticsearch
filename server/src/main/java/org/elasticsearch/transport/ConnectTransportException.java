/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ConnectTransportException extends ActionTransportException {

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
    }

    public ConnectTransportException(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.V_8_1_0)) {
            in.readOptionalWriteable(DiscoveryNode::new);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_8_1_0)) {
            out.writeMissingWriteable(DiscoveryNode.class);
        }
    }
}
