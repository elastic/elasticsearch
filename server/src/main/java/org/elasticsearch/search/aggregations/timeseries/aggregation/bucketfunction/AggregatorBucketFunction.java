/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction;

import java.util.Map;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;

public interface AggregatorBucketFunction<Input> {
    String name();

    void collect(Input number, long bucket);

    InternalAggregation getAggregation(long bucket, DocValueFormat formatter, Map<String, Object> metadata);

    void close();
}
