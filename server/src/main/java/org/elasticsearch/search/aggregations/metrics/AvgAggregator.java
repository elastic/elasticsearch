/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongDoubleDoubleArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

class AvgAggregator extends NumericMetricsAggregator.SingleValue {

    final ValuesSource.Numeric valuesSource;

    LongDoubleDoubleArray array;

    LongDoubleDoubleArray.Holder holder;

    DocValueFormat format;

    AvgAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        // TODO Stop expecting nulls here
        this.valuesSource = valuesSourceConfig.hasValues() ? (ValuesSource.Numeric) valuesSourceConfig.getValuesSource() : null;
        this.format = valuesSourceConfig.format();
        if (valuesSource != null) {
            final BigArrays bigArrays = context.bigArrays();
            array = bigArrays.newLongDoubleDoubleArray(1, true);
            holder = new LongDoubleDoubleArray.Holder();
        }
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedNumericDoubleValues values = valuesSource.doubleValues(aggCtx.getLeafReaderContext());
        final CompensatedSum kahanSummation = new CompensatedSum(0, 0);

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                array = bigArrays().grow(array, bucket + 1);

                if (values.advanceExact(doc)) {
                    final int valueCount = values.docValueCount();
                    // Compute the sum of double values with Kahan summation algorithm which is more
                    // accurate than naive summation.
                    // opaqueIndex.setForIndex(bucket);
                    array.get(bucket, holder);
                    final double sum = holder.getDouble0(); // getSum(array, opaqueIndex);
                    final double compensation = holder.getDouble1(); // getDelta(array, opaqueIndex);

                    kahanSummation.reset(sum, compensation);

                    for (int i = 0; i < valueCount; i++) {
                        double value = values.nextValue();
                        kahanSummation.add(value);
                    }
                    // array.set(opaqueIndex, getCount(array, opaqueIndex) + valueCount, kahanSummation.value(), kahanSummation.delta());
                    array.set(bucket, holder.getLong0() + valueCount, kahanSummation.value(), kahanSummation.delta());
                }
            }
        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (valuesSource == null || owningBucketOrd >= array.size()) {
            return Double.NaN;
        }
        // opaqueIndex.setForIndex(owningBucketOrd);
        array.get(owningBucketOrd, holder);

        // return getSum(array, opaqueIndex) / getCount(array, opaqueIndex);
        return holder.getDouble0() / holder.getLong0();
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= array.size()) {
            return buildEmptyAggregation();
        }
        return new InternalAvg(name, getSum(array, bucket), getCount(array, bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalAvg(name, 0.0, 0L, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(array);
    }

    // -- Convenience helpers that improve readability of array access.
    private static long getCount(LongDoubleDoubleArray array, long index) {
        return array.getLong0(index);
    }

    private static double getSum(LongDoubleDoubleArray array, long index) {
        return array.getDouble0(index);
    }

    private static double getDelta(LongDoubleDoubleArray array, long index) {
        return array.getDouble1(index);
    }

}
