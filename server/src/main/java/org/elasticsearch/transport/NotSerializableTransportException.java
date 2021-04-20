/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class NotSerializableTransportException extends TransportException {

    public NotSerializableTransportException(Throwable t) {
        super(buildMessage(t));
    }

    public NotSerializableTransportException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

    private static String buildMessage(Throwable t) {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(t.getClass().getName()).append("] ");
        while (t != null) {
            sb.append(t.getMessage()).append("; ");
            t = t.getCause();
        }
        return sb.toString();
    }
}
