/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exception;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * The same as {@link java.util.concurrent.TimeoutException} simply a runtime one.
 *
 *
 */
public class ElasticsearchTimeoutException extends ElasticsearchException {
    public ElasticsearchTimeoutException(StreamInput in) throws IOException {
        super(in);
    }

    public ElasticsearchTimeoutException(Throwable cause) {
        super(cause);
    }

    public ElasticsearchTimeoutException(String message, Object... args) {
        super(message, args);
    }

    public ElasticsearchTimeoutException(String message, Throwable cause, Object... args) {
        super(message, cause, args);
    }

    @Override
    public RestStatus status() {
        // closest thing to "your request took longer than you asked for"
        return RestStatus.TOO_MANY_REQUESTS;
    }

    @Override
    public boolean isTimeout() {
        return true;
    }
}
