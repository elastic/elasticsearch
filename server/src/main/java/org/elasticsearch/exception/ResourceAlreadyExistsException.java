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
import org.elasticsearch.index.Index;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class ResourceAlreadyExistsException extends ElasticsearchException {

    @SuppressWarnings("this-escape")
    public ResourceAlreadyExistsException(Index index) {
        this("index {} already exists", index.toString());
        setIndex(index);
    }

    public ResourceAlreadyExistsException(String msg, Object... args) {
        super(msg, args);
    }

    public ResourceAlreadyExistsException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
