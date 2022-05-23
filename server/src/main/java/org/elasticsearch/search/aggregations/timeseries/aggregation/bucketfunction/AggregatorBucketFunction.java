/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.AggregatorFunction;

import java.util.Map;

/**
 * time series aggregator function interface
 * Different from {@link AggregatorFunction}, this interface need to pass a bucket, which is the key of the aggregator
 * @param <Input> the input type of value
 */
public interface AggregatorBucketFunction<Input> {
    /**
     *
     * @return
     */
    String name();

    /**
     * collect value of the bucket
     */
    void collect(Input number, long bucket);

    /**
     * get the {@link InternalAggregation}, it used to transport cross nodes and reduce result in the coordinate node
     * @return the {@link InternalAggregation} of the input bucket
     */
    InternalAggregation getAggregation(long bucket, Map<String, Object> aggregatorParams, DocValueFormat formatter, Map<String, Object> metadata);

    /**
     * close the inner big array
     */
    void close();
}
