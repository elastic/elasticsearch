/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Generic security exception
 */
public class ElasticsearchSecurityException extends ElasticsearchStatusException {
    /**
     * Build the exception with a specific status and cause.
     */
    public ElasticsearchSecurityException(String msg, RestStatus status, Throwable cause, Object... args) {
        super(msg, status, cause, args);
    }

    /**
     * Build the exception with the status derived from the cause.
     */
    public ElasticsearchSecurityException(String msg, Exception cause, Object... args) {
        this(msg, ExceptionsHelper.status(cause), cause, args);
    }

    /**
     * Build the exception with a status of {@link RestStatus#INTERNAL_SERVER_ERROR} without a cause.
     */
    public ElasticsearchSecurityException(String msg, Object... args) {
        this(msg, RestStatus.INTERNAL_SERVER_ERROR, args);
    }

    /**
     * Build the exception without a cause.
     */
    public ElasticsearchSecurityException(String msg, RestStatus status, Object... args) {
        super(msg, status, args);
    }

    /**
     * Read from a stream.
     */
    public ElasticsearchSecurityException(StreamInput in) throws IOException {
        super(in);
    }
}
