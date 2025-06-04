/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Thrown when failing to execute an aggregation
 */
public class AggregationExecutionException extends ElasticsearchException {

    public AggregationExecutionException(String msg) {
        super(msg);
    }

    public AggregationExecutionException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public AggregationExecutionException(StreamInput in) throws IOException {
        super(in);
    }

    public static class InvalidPath extends AggregationExecutionException {

        public InvalidPath(String msg) {
            super(msg);
        }

        public InvalidPath(String msg, Throwable cause) {
            super(msg, cause);
        }

        public InvalidPath(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public RestStatus status() {
            return RestStatus.BAD_REQUEST;
        }
    }
}
