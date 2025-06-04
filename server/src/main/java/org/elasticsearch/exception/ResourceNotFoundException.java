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
 * Generic ResourceNotFoundException corresponding to the {@link RestStatus#NOT_FOUND} status code
 */
public class ResourceNotFoundException extends ElasticsearchException {

    public ResourceNotFoundException(String msg, Object... args) {
        super(msg, args);
    }

    public ResourceNotFoundException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }

    public ResourceNotFoundException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public final RestStatus status() {
        return RestStatus.NOT_FOUND;
    }
}
