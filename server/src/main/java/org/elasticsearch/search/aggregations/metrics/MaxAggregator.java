/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FutureArrays;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.search.aggregations.metrics.MinAggregator.getPointReaderOrNull;

class MaxAggregator extends NumericMetricsAggregator.SingleValue {

    final ValuesSource.Numeric valuesSource;
    final DocValueFormat formatter;

    final String pointField;
    final Function<byte[], Number> pointConverter;

    DoubleArray maxes;

    MaxAggregator(String name,
                    ValuesSourceConfig config,
                    ValuesSource.Numeric valuesSource,
                    SearchContext context,
                    Aggregator parent, Map<String, Object> metadata) throws IOException {
        super(name, context, parent, metadata);
        this.valuesSource = valuesSource;
        if (valuesSource != null) {
            maxes = context.bigArrays().newDoubleArray(1, false);
            maxes.fill(0, maxes.size(), Double.NEGATIVE_INFINITY);
        }
        this.formatter = config.format();
        this.pointConverter = getPointReaderOrNull(context, parent, config);
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
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            if (parent != null) {
                return LeafBucketCollector.NO_OP_COLLECTOR;
            } else {
                // we have no parent and the values source is empty so we can skip collecting hits.
                throw new CollectionTerminatedException();
            }
        }
        if (pointConverter != null) {
            Number segMax = findLeafMaxValue(ctx.reader(), pointField, pointConverter);
            if (segMax != null) {
                /*
                 * There is no parent aggregator (see {@link MinAggregator#getPointReaderOrNull}
                 * so the ordinal for the bucket is always 0.
                 */
                assert maxes.size() == 1;
                double max = maxes.get(0);
                max = Math.max(max, segMax.doubleValue());
                maxes.set(0, max);
                // the maximum value has been extracted, we don't need to collect hits on this segment.
                throw new CollectionTerminatedException();
            }
        }
        final BigArrays bigArrays = context.bigArrays();
        final SortedNumericDoubleValues allValues = valuesSource.doubleValues(ctx);
        final NumericDoubleValues values = MultiValueMode.MAX.select(allValues);
        return new LeafBucketCollectorBase(sub, allValues) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bucket >= maxes.size()) {
                    long from = maxes.size();
                    maxes = bigArrays.grow(maxes, bucket + 1);
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
        return new InternalMax(name, maxes.get(bucket), formatter, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMax(name, Double.NEGATIVE_INFINITY, formatter, metadata());
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
                if (FutureArrays.equals(maxValue, 0, numBytes, maxPackedValue, 0, numBytes)) {
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
