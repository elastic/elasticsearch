/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class InvalidTypeNameException extends MapperException {

    public InvalidTypeNameException(StreamInput in) throws IOException {
        super(in);
    }

    public InvalidTypeNameException(String message) {
        super(message);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
