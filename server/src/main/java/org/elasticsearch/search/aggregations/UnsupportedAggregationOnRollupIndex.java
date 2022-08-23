/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

// NOTE: the name of this class is part of a contract with Kibana which uses it to detect specific
// errors while running aggregations on rollup indices.
/**
 * Thrown when executing an aggregation on a time series index field whose type is not supported.
 * Downsampling uses specific types while aggregating some fields (like 'aggregate_metric_double').
 * Such field types do not support some aggregations.
 */
public class UnsupportedAggregationOnRollupIndex extends AggregationExecutionException {

    public UnsupportedAggregationOnRollupIndex(final String msg) {
        super(msg);
    }

    public UnsupportedAggregationOnRollupIndex(final StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
