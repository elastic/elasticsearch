/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.aggregations.metrics;

import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.InternalValueCount;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.analytics.aggregations.support.HistogramValuesSource;

import java.io.IOException;
import java.util.Map;

/**
 * Value count aggregator operating over histogram datatypes {@link HistogramValuesSource}
 * The aggregation counts the number of values a histogram field has within the aggregation context
 * by adding the counts of the histograms.
 */
public class HistoBackedValueCountAggregator extends NumericMetricsAggregator.SingleValue {

    final HistogramValuesSource.Histogram valuesSource;

    /** Count per bucket */
    LongArray counts;

    public HistoBackedValueCountAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext aggregationContext,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, aggregationContext, parent, metadata);
        assert valuesSourceConfig.hasValues();
        this.valuesSource = (HistogramValuesSource.Histogram) valuesSourceConfig.getValuesSource();
        counts = bigArrays().newLongArray(1, true);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        final HistogramValues values = valuesSource.getHistogramValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                counts = bigArrays().grow(counts, bucket + 1);
                if (values.advanceExact(doc)) {
                    final HistogramValue sketch = values.histogram();
                    while (sketch.next()) {
                        counts.increment(bucket, sketch.count());
                    }
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
