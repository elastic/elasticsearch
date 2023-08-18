/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Exception who's {@link RestStatus} is arbitrary rather than derived. Used, for example, by reindex-from-remote to wrap remote exceptions
 * that contain a status.
 */
public class ElasticsearchStatusException extends ElasticsearchException {
    private final RestStatus status;

    /**
     * Build the exception with a specific status and cause.
     */
    public ElasticsearchStatusException(String msg, RestStatus status, Throwable cause, Object... args) {
        super(msg, cause, args);
        this.status = status;
    }

    /**
     * Build the exception without a cause.
     */
    public ElasticsearchStatusException(String msg, RestStatus status, Object... args) {
        this(msg, status, null, args);
    }

    /**
     * Read from a stream.
     */
    public ElasticsearchStatusException(StreamInput in) throws IOException {
        super(in);
        status = RestStatus.readFrom(in);
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        RestStatus.writeTo(out, status);
    }

    @Override
    public final RestStatus status() {
        return status;
    }
}
