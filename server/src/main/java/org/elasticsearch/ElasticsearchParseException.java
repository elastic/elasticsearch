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
 * Unchecked exception that is translated into a {@code 400 BAD REQUEST} error when it bubbles out over HTTP.
 */
public class ElasticsearchParseException extends ElasticsearchException {

    public ElasticsearchParseException(String msg, Object... args) {
        super(msg, args);
    }

    public ElasticsearchParseException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }

    public ElasticsearchParseException(StreamInput in) throws IOException {
        super(in);
    }
    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
