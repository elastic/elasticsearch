/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.search.aggregations.support.MultiValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.analytics.ttest.TTestAggregationBuilder.A_FIELD;
import static org.elasticsearch.xpack.analytics.ttest.TTestAggregationBuilder.B_FIELD;

public class UnpairedTTestAggregator extends TTestAggregator<UnpairedTTestState> {
    private final TTestStatsBuilder a;
    private final TTestStatsBuilder b;
    private final boolean homoscedastic;
    private final Supplier<Tuple<Weight, Weight>> weightsSupplier;

    UnpairedTTestAggregator(String name, MultiValuesSource.NumericMultiValuesSource valuesSources, int tails, boolean homoscedastic,
                            Supplier<Tuple<Weight, Weight>> weightsSupplier, DocValueFormat format, SearchContext context,
                            Aggregator parent, Map<String, Object> metadata) throws IOException {
        super(name, valuesSources, tails, format, context, parent, metadata);
        BigArrays bigArrays = context.bigArrays();
        a = new TTestStatsBuilder(bigArrays);
        b = new TTestStatsBuilder(bigArrays);
        this.weightsSupplier = weightsSupplier;
        this.homoscedastic = homoscedastic;
    }

    @Override
    protected UnpairedTTestState getState(long bucket) {
        return new UnpairedTTestState(a.get(bucket), b.get(bucket), homoscedastic, tails);
    }

    @Override
    protected UnpairedTTestState getEmptyState() {
        return new UnpairedTTestState(new TTestStats(0, 0, 0), new TTestStats(0, 0, 0), homoscedastic, tails);
    }

    @Override
    protected long size() {
        return a.getSize();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        if (valuesSources == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final SortedNumericDoubleValues docAValues = valuesSources.getField(A_FIELD.getPreferredName(), ctx);
        final SortedNumericDoubleValues docBValues = valuesSources.getField(B_FIELD.getPreferredName(), ctx);
        final CompensatedSum compSumA = new CompensatedSum(0, 0);
        final CompensatedSum compSumOfSqrA = new CompensatedSum(0, 0);
        final CompensatedSum compSumB = new CompensatedSum(0, 0);
        final CompensatedSum compSumOfSqrB = new CompensatedSum(0, 0);
        final Tuple<Weight, Weight> weights = weightsSupplier.get();
        final Bits bitsA = getBits(ctx, weights.v1());
        final Bits bitsB = getBits(ctx, weights.v2());

        return new LeafBucketCollectorBase(sub, docAValues) {

            private void processValues(int doc, long bucket, SortedNumericDoubleValues docValues, CompensatedSum compSum,
                                       CompensatedSum compSumOfSqr, TTestStatsBuilder builder) throws IOException {
                if (docValues.advanceExact(doc)) {
                    final int numValues = docValues.docValueCount();
                    for (int i = 0; i < numValues; i++) {
                        builder.addValue(compSum, compSumOfSqr, bucket, docValues.nextValue());
                    }
                }
            }

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bitsA == null || bitsA.get(doc)) {
                    a.grow(bigArrays, bucket + 1);
                    processValues(doc, bucket, docAValues, compSumA, compSumOfSqrA, a);
                }
                if (bitsB == null || bitsB.get(doc)) {
                    processValues(doc, bucket, docBValues, compSumB, compSumOfSqrB, b);
                    b.grow(bigArrays, bucket + 1);
                }
            }
        };
    }

    private Bits getBits(LeafReaderContext ctx, Weight weight) throws IOException {
        if (weight == null) {
            return null;
        }
        return Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), weight.scorerSupplier(ctx));
    }

    @Override
    public void doClose() {
        Releasables.close(a, b);
    }
}
