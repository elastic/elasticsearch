/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

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
        if (in.getTransportVersion().before(TransportVersions.V_8_1_0)) {
            in.readOptionalWriteable(DiscoveryNode::new);
        }
    }

    /**
     * The ES REST API is a gateway to a single or multiple clusters. If there is an error connecting to other servers, then we should
     * return a 502 BAD_GATEWAY status code instead of the parent class' 500 INTERNAL_SERVER_ERROR. Clients tend to retry on a 502 but not
     * on a 500, and retrying may help on a connection error.
     *
     * @return a {@link RestStatus#BAD_GATEWAY} code
     */
    @Override
    public final RestStatus status() {
        return RestStatus.BAD_GATEWAY;
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        if (out.getTransportVersion().before(TransportVersions.V_8_1_0)) {
            out.writeMissingWriteable(DiscoveryNode.class);
        }
    }
}
