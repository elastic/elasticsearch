/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class TransportSerializationException extends TransportException {

    public TransportSerializationException(StreamInput in) throws IOException {
        super(in);
    }

    public TransportSerializationException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * A tripped circuit breaker while reading a message is back-pressure rather than a serialization failure, so report the breaker's
     * retriable status instead of a 500.
     */
    @Override
    public RestStatus status() {
        if (ExceptionsHelper.unwrap(getCause(), CircuitBreakingException.class) instanceof CircuitBreakingException cbe) {
            return cbe.status();
        }
        return super.status();
    }
}
