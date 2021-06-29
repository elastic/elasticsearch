/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class MapperParsingException extends MapperException {

    public MapperParsingException(StreamInput in) throws IOException {
        super(in);
    }

    public MapperParsingException(String message) {
        super(message);
    }

    public MapperParsingException(String message, Throwable cause) {
        super(message, cause);
    }

    public MapperParsingException(String message, Throwable cause, Object... args) {
        super(message, cause, args);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
