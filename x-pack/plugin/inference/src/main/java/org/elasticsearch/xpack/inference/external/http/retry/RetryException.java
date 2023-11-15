/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchWrapperException;

public class RetryException extends ElasticsearchException implements ElasticsearchWrapperException {
    private final boolean shouldRetry;

    public RetryException(boolean shouldRetry, Throwable cause) {
        super(cause);
        this.shouldRetry = shouldRetry;
    }

    public RetryException(boolean shouldRetry, String msg) {
        super(msg);
        this.shouldRetry = shouldRetry;
    }

    public RetryException(boolean shouldRetry, String msg, Throwable cause) {
        super(msg, cause);
        this.shouldRetry = shouldRetry;
    }

    public boolean shouldRetry() {
        return shouldRetry;
    }
}
