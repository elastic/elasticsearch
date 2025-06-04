/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchWrapperException;
import org.elasticsearch.xpack.inference.external.request.Request;

public class RetryException extends ElasticsearchException implements ElasticsearchWrapperException, Retryable {
    private final boolean shouldRetry;

    public RetryException(boolean shouldRetry, Throwable cause) {
        super(cause);
        this.shouldRetry = shouldRetry;
    }

    /**
     * This should really only be used for testing. Ideally a retry exception would be associated with
     * an actual exception that can be provided back to the client in the event that retrying fails.
     */
    RetryException(boolean shouldRetry, String msg) {
        super(msg);
        this.shouldRetry = shouldRetry;
    }

    public RetryException(boolean shouldRetry, String msg, Throwable cause) {
        super(msg, cause);
        this.shouldRetry = shouldRetry;
    }

    @Override
    public Request rebuildRequest(Request original) {
        return original;
    }

    @Override
    public boolean shouldRetry() {
        return shouldRetry;
    }
}
