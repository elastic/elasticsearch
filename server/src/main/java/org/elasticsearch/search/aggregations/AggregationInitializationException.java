/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Thrown when failing to execute an aggregation
 */
public class AggregationInitializationException extends ElasticsearchException {

    public AggregationInitializationException(String msg) {
        super(msg);
    }

    public AggregationInitializationException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public AggregationInitializationException(StreamInput in) throws IOException{
        super(in);
    }
}
