/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.transport.TransportAddress;

import java.io.IOException;

/**
 * An action invocation failure.
 *
 *
 */
public class ActionTransportException extends TransportException {

    private final TransportAddress address;

    private final String action;

    public ActionTransportException(StreamInput in) throws IOException {
        super(in);
        address = in.readOptionalWriteable(TransportAddress::new);
        action = in.readOptionalString();
    }

    public ActionTransportException(String name, TransportAddress address, String action, Throwable cause) {
        super(buildMessage(name, address, action, null), cause);
        this.address = address;
        this.action = action;
    }

    public ActionTransportException(String name, TransportAddress address, String action, String msg, Throwable cause) {
        super(buildMessage(name, address, action, msg), cause);
        this.address = address;
        this.action = action;
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        out.writeOptionalWriteable(address);
        out.writeOptionalString(action);
    }

    /**
     * The target address to invoke the action on.
     */
    public TransportAddress address() {
        return address;
    }

    /**
     * The action to invoke.
     */
    public String action() {
        return action;
    }

    private static String buildMessage(String name, TransportAddress address, String action, String msg) {
        StringBuilder sb = new StringBuilder();
        if (name != null) {
            sb.append('[').append(name).append(']');
        }
        if (address != null) {
            sb.append('[').append(address).append(']');
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
