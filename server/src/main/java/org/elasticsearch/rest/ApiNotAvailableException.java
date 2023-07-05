/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.GONE;

/**
 * Thrown when an API is not available in the current environment.
 */
public class ApiNotAvailableException extends ElasticsearchException {

    public ApiNotAvailableException(String msg, Object... args) {
        super(msg, args);
    }

    public ApiNotAvailableException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return GONE;
    }
}
