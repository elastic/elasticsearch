/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

class MaxAggregator extends NumericMetricsAggregator.SingleValue {

    final ValuesSource.Numeric valuesSource;
    final DocValueFormat formatter;

    final String pointField;
    final Function<byte[], Number> pointConverter;

    DoubleArray maxes;

    MaxAggregator(String name, ValuesSourceConfig config, AggregationContext context, Aggregator parent, Map<String, Object> metadata)
        throws IOException {
        super(name, context, parent, metadata);
        // TODO stop expecting nulls here
        this.valuesSource = config.hasValues() ? (ValuesSource.Numeric) config.getValuesSource() : null;
        if (valuesSource != null) {
            maxes = context.bigArrays().newDoubleArray(1, false);
            maxes.fill(0, maxes.size(), Double.NEGATIVE_INFINITY);
        }
        this.formatter = config.format();
        this.pointConverter = pointReaderIfAvailable(config);
        if (pointConverter != null) {
            pointField = config.fieldContext().field();
        } else {
            pointField = null;
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
        if (pointConverter != null) {
            Number segMax = findLeafMaxValue(aggCtx.getLeafReaderContext().reader(), pointField, pointConverter);
            if (segMax != null) {
                /*
                 * There is no parent aggregator (see {@link AggregatorBase#getPointReaderOrNull}
                 * so the ordinal for the bucket is always 0.
                 */
                assert maxes.size() == 1;
                double max = maxes.get(0);
                max = Math.max(max, segMax.doubleValue());
                maxes.set(0, max);
                // the maximum value has been extracted, we don't need to collect hits on this segment.
                return LeafBucketCollector.NO_OP_COLLECTOR;
            }
        }
        final SortedNumericDoubleValues allValues = valuesSource.doubleValues(aggCtx.getLeafReaderContext());
        final NumericDoubleValues values = MultiValueMode.MAX.select(allValues);
        return new LeafBucketCollectorBase(sub, allValues) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bucket >= maxes.size()) {
                    long from = maxes.size();
                    maxes = bigArrays().grow(maxes, bucket + 1);
                    maxes.fill(from, maxes.size(), Double.NEGATIVE_INFINITY);
                }
                if (values.advanceExact(doc)) {
                    final double value = values.doubleValue();
                    double max = maxes.get(bucket);
                    max = Math.max(max, value);
                    maxes.set(bucket, max);
                }
            }

        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (valuesSource == null || owningBucketOrd >= maxes.size()) {
            return Double.NEGATIVE_INFINITY;
        }
        return maxes.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= maxes.size()) {
            return buildEmptyAggregation();
        }
        return new Max(name, maxes.get(bucket), formatter, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return Max.createEmptyMax(name, formatter, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(maxes);
    }

    /**
     * Returns the maximum value indexed in the <code>fieldName</code> field or <code>null</code>
     * if the value cannot be inferred from the indexed {@link PointValues}.
     */
    static Number findLeafMaxValue(LeafReader reader, String fieldName, Function<byte[], Number> converter) throws IOException {
        final PointValues pointValues = reader.getPointValues(fieldName);
        if (pointValues == null) {
            return null;
        }
        final Bits liveDocs = reader.getLiveDocs();
        if (liveDocs == null) {
            return converter.apply(pointValues.getMaxPackedValue());
        }
        int numBytes = pointValues.getBytesPerDimension();
        final byte[] maxValue = pointValues.getMaxPackedValue();
        final byte[][] result = new byte[1][];
        pointValues.intersect(new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
                if (liveDocs.get(docID)) {
                    // we need to collect all values in this leaf (the sort is ascending) where
                    // the last live doc is guaranteed to contain the max value for the segment.
                    if (result[0] == null) {
                        result[0] = new byte[packedValue.length];
                    }
                    System.arraycopy(packedValue, 0, result[0], 0, packedValue.length);
                }
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                if (Arrays.equals(maxValue, 0, numBytes, maxPackedValue, 0, numBytes)) {
                    // we only check leaves that contain the max value for the segment.
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                } else {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
            }
        });
        return result[0] != null ? converter.apply(result[0]) : null;
    }
}
