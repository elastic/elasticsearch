/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.Index;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Exception indicating that one or more requested indices are failure indices.
 */
public final class FailureIndexNotSupportedException extends ElasticsearchException {

    public FailureIndexNotSupportedException(Index index) {
        super("failure index not supported");
        setIndex(index);
    }

    public FailureIndexNotSupportedException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

}
