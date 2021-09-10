/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.aggregatemetric.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.InternalValueCount;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.aggregatemetric.aggregations.support.AggregateMetricsValuesSource;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper;

import java.io.IOException;
import java.util.Map;

/**
 * A field data based aggregator that adds all values in the value_count metric sub-field from an aggregate_metric field.
 * This aggregator works in a multi-bucket mode, that is, when serves as a sub-aggregator, a single aggregator instance
 * aggregates the counts for all buckets owned by the parent aggregator)
 */
class AggregateMetricBackedValueCountAggregator extends NumericMetricsAggregator.SingleValue {

    private final AggregateMetricsValuesSource.AggregateDoubleMetric valuesSource;

    // a count per bucket
    LongArray counts;

    AggregateMetricBackedValueCountAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext aggregationContext,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, aggregationContext, parent, metadata);
        // TODO: stop expecting nulls here
        this.valuesSource = valuesSourceConfig.hasValues()
            ? (AggregateMetricsValuesSource.AggregateDoubleMetric) valuesSourceConfig.getValuesSource()
            : null;
        if (valuesSource != null) {
            counts = bigArrays().newLongArray(1, true);
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = bigArrays();
        final SortedNumericDoubleValues values = valuesSource.getAggregateMetricValues(
            ctx,
            AggregateDoubleMetricFieldMapper.Metric.value_count
        );

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                counts = bigArrays.grow(counts, bucket + 1);
                if (values.advanceExact(doc)) {
                    for (int i = 0; i < values.docValueCount(); i++) { // For aggregate metric this should always equal to 1
                        long value = Double.valueOf(values.nextValue()).longValue();
                        counts.increment(bucket, value);
                    }
                }
            }
        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        return (valuesSource == null || owningBucketOrd >= counts.size()) ? 0 : counts.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= counts.size()) {
            return buildEmptyAggregation();
        }
        return new InternalValueCount(name, counts.get(bucket), metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalValueCount(name, 0L, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(counts);
    }

}
