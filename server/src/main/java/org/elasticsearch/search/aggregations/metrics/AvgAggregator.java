/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

class AvgAggregator extends SumAggregator {

    LongArray counts;

    AvgAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, valuesSourceConfig, context, parent, metadata);
        counts = context.bigArrays().newLongArray(1, true);
    }

    @Override
    protected LeafBucketCollector getLeafCollector(SortedNumericDoubleValues values, final LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    maybeGrow(bucket);
                    counts.increment(bucket, sumSortedDoubles(bucket, values, sums, compensations));
                }
            }
        };
    }

    @Override
    protected LeafBucketCollector getLeafCollector(NumericDoubleValues values, final LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    maybeGrow(bucket);
                    computeSum(bucket, values.doubleValue(), sums, compensations);
                    counts.increment(bucket, 1L);
                }
            }
        };
    }

    @Override
    protected void doGrow(long bucket, BigArrays bigArrays) {
        super.doGrow(bucket, bigArrays);
        counts = bigArrays.grow(counts, bucket + 1);
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (owningBucketOrd >= sums.size()) {
            return Double.NaN;
        }
        return sums.get(owningBucketOrd) / counts.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (bucket >= sums.size()) {
            return buildEmptyAggregation();
        }
        return new InternalAvg(name, sums.get(bucket), counts.get(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return InternalAvg.empty(name, format, metadata());
    }

    @Override
    public void doClose() {
        super.doClose();
        Releasables.close(counts);
    }

}
