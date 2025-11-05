/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Indicates an operation timeout in Elasticsearch.
 * <p>
 * This is the runtime equivalent of {@link java.util.concurrent.TimeoutException}, used throughout
 * Elasticsearch to signal that an operation exceeded its allotted time limit.
 * <p>
 * This exception returns {@link RestStatus#TOO_MANY_REQUESTS} as its HTTP status code, which is
 * the closest semantic match for "your request took longer than you asked for".
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * long startTime = System.currentTimeMillis();
 * long timeout = 5000; // 5 seconds
 * while (!operationComplete()) {
 *     if (System.currentTimeMillis() - startTime > timeout) {
 *         throw new ElasticsearchTimeoutException("Operation timed out after {} ms", timeout);
 *     }
 *     // continue operation
 * }
 * }</pre>
 */
public class ElasticsearchTimeoutException extends ElasticsearchException {
    /**
     * Constructs a timeout exception from a stream input.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs while reading from the stream
     */
    public ElasticsearchTimeoutException(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Constructs a timeout exception with a cause.
     *
     * @param cause the underlying cause of this exception
     */
    public ElasticsearchTimeoutException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a timeout exception with a formatted message.
     *
     * @param message the detail message, can include {} placeholders
     * @param args the arguments to format into the message
     */
    public ElasticsearchTimeoutException(String message, Object... args) {
        super(message, args);
    }

    /**
     * Constructs a timeout exception with a formatted message and cause.
     *
     * @param message the detail message, can include {} placeholders
     * @param cause the underlying cause of this exception
     * @param args the arguments to format into the message
     */
    public ElasticsearchTimeoutException(String message, Throwable cause, Object... args) {
        super(message, cause, args);
    }

    /**
     * Returns the REST status code for this exception.
     *
     * @return {@link RestStatus#TOO_MANY_REQUESTS} indicating the operation timed out
     */
    @Override
    public RestStatus status() {
        // closest thing to "your request took longer than you asked for"
        return RestStatus.TOO_MANY_REQUESTS;
    }

    /**
     * Indicates whether this exception represents a timeout.
     *
     * @return always true for timeout exceptions
     */
    @Override
    public boolean isTimeout() {
        return true;
    }
}
