/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.elasticsearch.xpack.inference.external.request.Request;

/**
 * Provides an exception for truncating the request input.
 */
public class ContentTooLargeException extends RetryException {
    public static final double TRUNCATION_PERCENTAGE = 0.5;

    public ContentTooLargeException(Throwable cause) {
        super(true, cause);
    }

    @Override
    public Request initialize(Request original) {
        return original.truncate(TRUNCATION_PERCENTAGE);
    }
}
