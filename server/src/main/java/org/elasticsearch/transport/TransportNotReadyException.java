/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

/**
 * Exception indicating that the {@link TransportService} received a request before it was ready to handle it, so the request should be
 * rejected and the connection closed. Never goes over the wire, so it's just a {@link RuntimeException}.
 */
public class TransportNotReadyException extends RuntimeException {
    public TransportNotReadyException() {
        super("transport not ready yet to handle incoming requests");
    }

    @Override
    public Throwable fillInStackTrace() {
        return this; // stack trace is uninteresting
    }
}
