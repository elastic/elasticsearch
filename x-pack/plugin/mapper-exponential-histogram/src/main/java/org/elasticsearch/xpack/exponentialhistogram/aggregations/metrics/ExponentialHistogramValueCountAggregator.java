/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.exponentialhistogram.aggregations.metrics;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.InternalValueCount;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.support.ExponentialHistogramValuesSource;
import org.elasticsearch.xpack.exponentialhistogram.fielddata.ExponentialHistogramValuesReader;

import java.io.IOException;
import java.util.Map;

public final class ExponentialHistogramValueCountAggregator extends NumericMetricsAggregator.SingleValue {

    private final ExponentialHistogramValuesSource.ExponentialHistogram valuesSource;

    // a count per bucket
    private LongArray counts;

    public ExponentialHistogramValueCountAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext aggregationContext,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, aggregationContext, parent, metadata);
        assert valuesSourceConfig.hasValues();
        this.valuesSource = (ExponentialHistogramValuesSource.ExponentialHistogram) valuesSourceConfig.getValuesSource();
        counts = bigArrays().newLongArray(1, true);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        BigArrays bigArrays = bigArrays();
        ExponentialHistogramValuesReader values = valuesSource.getHistogramValues(aggCtx.getLeafReaderContext());

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                counts = bigArrays.grow(counts, bucket + 1);
                if (values.advanceExact(doc)) {
                    counts.increment(bucket, values.valuesCountValue());
                }
            }
        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        return owningBucketOrd >= counts.size() ? 0 : counts.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (bucket >= counts.size()) {
            return buildEmptyAggregation();
        }
        return new InternalValueCount(name, counts.get(bucket), metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return InternalValueCount.empty(name, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(counts);
    }

}
