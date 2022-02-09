/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.MultiValuesSource;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.analytics.ttest.TTestAggregationBuilder.A_FIELD;
import static org.elasticsearch.xpack.analytics.ttest.TTestAggregationBuilder.B_FIELD;

public class PairedTTestAggregator extends TTestAggregator<PairedTTestState> {
    private TTestStatsBuilder statsBuilder;

    PairedTTestAggregator(
        String name,
        MultiValuesSource.NumericMultiValuesSource valuesSources,
        int tails,
        DocValueFormat format,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, valuesSources, tails, format, context, parent, metadata);
        statsBuilder = new TTestStatsBuilder(bigArrays());
    }

    @Override
    protected PairedTTestState getState(long bucket) {
        return new PairedTTestState(statsBuilder.get(bucket), tails);
    }

    @Override
    protected PairedTTestState getEmptyState() {
        return new PairedTTestState(new TTestStats(0, 0, 0), tails);
    }

    @Override
    protected long size() {
        return statsBuilder.getSize();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        if (valuesSources == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedNumericDoubleValues docAValues = valuesSources.getField(A_FIELD.getPreferredName(), ctx);
        final SortedNumericDoubleValues docBValues = valuesSources.getField(B_FIELD.getPreferredName(), ctx);
        final CompensatedSum compDiffSum = new CompensatedSum(0, 0);
        final CompensatedSum compDiffSumOfSqr = new CompensatedSum(0, 0);

        return new LeafBucketCollectorBase(sub, docAValues) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (docAValues.advanceExact(doc) && docBValues.advanceExact(doc)) {
                    if (docAValues.docValueCount() > 1 || docBValues.docValueCount() > 1) {
                        throw new AggregationExecutionException(
                            "Encountered more than one value for a "
                                + "single document. Use a script to combine multiple values per doc into a single value."
                        );
                    }
                    statsBuilder.grow(bigArrays(), bucket + 1);
                    // There should always be one value if advanceExact lands us here, either
                    // a real value or a `missing` value
                    assert docAValues.docValueCount() == 1;
                    assert docBValues.docValueCount() == 1;
                    double diff = docAValues.nextValue() - docBValues.nextValue();
                    statsBuilder.addValue(compDiffSum, compDiffSumOfSqr, bucket, diff);
                }
            }
        };
    }

    @Override
    public void doClose() {
        Releasables.close(statsBuilder);
    }
}
