/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@FunctionalInterface
public interface TimeSeriesAggregationAggregatorSupplier {
    Aggregator build(
        String name,
        AggregatorFactories factories,
        boolean keyed,
        List<String> group,
        List<String> without,
        DateHistogramInterval interval,
        DateHistogramInterval offset,
        org.elasticsearch.search.aggregations.timeseries.aggregation.Aggregator aggregator,
        Map<String, Object> aggregatorParams,
        Downsample downsample,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        BucketOrder order,
        long startTime,
        long endTime,
        boolean deferring,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound bucketCardinality,
        Map<String, Object> metadata
    ) throws IOException;
}
