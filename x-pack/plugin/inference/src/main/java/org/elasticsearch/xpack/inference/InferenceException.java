/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.rest.RestStatus;

public class InferenceException extends ElasticsearchException {
    public InferenceException(String message, Throwable cause, Object... args) {
        super(message, cause, args);
    }

    @Override
    public RestStatus status() {
        // Override status so that we get the status of the cause while retaining the message of the inference exception when emitting to
        // XContent
        return ExceptionsHelper.status(getCause());
    }
}
