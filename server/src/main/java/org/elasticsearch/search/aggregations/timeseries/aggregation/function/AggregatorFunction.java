/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.function;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.util.Map;

/**
 * time series aggregator function interface
 * @param <Input> the input type of value
 * @param <Output> the output type of the function
 */
public interface AggregatorFunction<Input, Output> {
    /**
     * collect value
     * @param value input value
     */
    void collect(Input value);

    /**
     * get the result of aggregator function
     */
    Output get();

    /**
     * get the {@link InternalAggregation}, it used to transport cross nodes and reduce result in the coordinate node
     * @param formatter the value formatter
     * @param metadata aggregation metadata
     * @return the {@link InternalAggregation} of aggregator function
     */
    InternalAggregation getAggregation(DocValueFormat formatter, Map<String, Object> metadata);
}
