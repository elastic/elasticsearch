/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.transport.TransportAddress;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * An action invocation failure.
 */
public class ActionTransportException extends TransportException {

    public ActionTransportException(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.V_8_1_0)) {
            in.readOptionalWriteable(TransportAddress::new);
            in.readOptionalString();
        }
    }

    public ActionTransportException(String name, TransportAddress address, String action, Throwable cause) {
        this(name, address, action, null, cause);
    }

    public ActionTransportException(String name, TransportAddress address, String action, String msg, Throwable cause) {
        this(name, address == null ? null : address.address(), action, msg, cause);
    }

    public ActionTransportException(String name, InetSocketAddress address, String action, String msg, Throwable cause) {
        super(buildMessage(name, address, action, msg), cause);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_8_1_0)) {
            out.writeMissingWriteable(TransportAddress.class);
            out.writeMissingString(); // action
        }
    }

    private static String buildMessage(String name, InetSocketAddress address, String action, String msg) {
        StringBuilder sb = new StringBuilder();
        if (name != null) {
            sb.append('[').append(name).append(']');
        }
        if (address != null) {
            sb.append('[').append(NetworkAddress.format(address)).append(']');
        }
        if (action != null) {
            sb.append('[').append(action).append(']');
        }
        if (msg != null) {
            sb.append(" ").append(msg);
        }
        return sb.toString();
    }
}
