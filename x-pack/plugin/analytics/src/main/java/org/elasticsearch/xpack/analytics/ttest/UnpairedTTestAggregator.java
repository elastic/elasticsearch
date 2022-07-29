/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.MultiValuesSource;

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

    UnpairedTTestAggregator(
        String name,
        MultiValuesSource.NumericMultiValuesSource valuesSources,
        int tails,
        boolean homoscedastic,
        Supplier<Tuple<Weight, Weight>> weightsSupplier,
        DocValueFormat format,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, valuesSources, tails, format, context, parent, metadata);
        a = new TTestStatsBuilder(bigArrays());
        b = new TTestStatsBuilder(bigArrays());
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
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        if (valuesSources == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedNumericDoubleValues docAValues = valuesSources.getField(A_FIELD.getPreferredName(), aggCtx.getLeafReaderContext());
        final SortedNumericDoubleValues docBValues = valuesSources.getField(B_FIELD.getPreferredName(), aggCtx.getLeafReaderContext());
        final CompensatedSum compSumA = new CompensatedSum(0, 0);
        final CompensatedSum compSumOfSqrA = new CompensatedSum(0, 0);
        final CompensatedSum compSumB = new CompensatedSum(0, 0);
        final CompensatedSum compSumOfSqrB = new CompensatedSum(0, 0);
        final Tuple<Weight, Weight> weights = weightsSupplier.get();
        final Bits bitsA = getBits(aggCtx.getLeafReaderContext(), weights.v1());
        final Bits bitsB = getBits(aggCtx.getLeafReaderContext(), weights.v2());

        return new LeafBucketCollectorBase(sub, docAValues) {

            private void processValues(
                int doc,
                long bucket,
                SortedNumericDoubleValues docValues,
                CompensatedSum compSum,
                CompensatedSum compSumOfSqr,
                TTestStatsBuilder builder
            ) throws IOException {
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
                    a.grow(bigArrays(), bucket + 1);
                    processValues(doc, bucket, docAValues, compSumA, compSumOfSqrA, a);
                }
                if (bitsB == null || bitsB.get(doc)) {
                    processValues(doc, bucket, docBValues, compSumB, compSumOfSqrB, b);
                    b.grow(bigArrays(), bucket + 1);
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
