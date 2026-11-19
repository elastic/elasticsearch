/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.HdrHistogram.DoubleHistogram;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * A sketch takes far more heap than its serialized form, since its arrays are sized by its compression or range and not by the values it
 * holds.
 */
public class SketchAggregationsHeapEstimateTests extends ESTestCase {

    private static final int BUCKETS = 200;
    private static final int VALUES = 2000;

    public void testTDigestPercentiles() {
        assertEstimateCloseToHeap(() -> new InternalTDigestPercentiles("p", new double[] { 50, 99 }, tDigestUnion(), false, RAW, null));
    }

    public void testTDigestPercentileRanks() {
        assertEstimateCloseToHeap(() -> new InternalTDigestPercentileRanks("p", new double[] { 50, 99 }, tDigestUnion(), false, RAW, null));
    }

    public void testMedianAbsoluteDeviation() {
        assertEstimateCloseToHeap(() -> {
            TDigestState sketch = TDigestState.create(HistogramUnionState.NOOP_BREAKER, 100.0);
            for (int i = 0; i < VALUES; i++) {
                sketch.add(randomDouble() * 1000);
            }
            return new InternalMedianAbsoluteDeviation("mad", null, RAW, sketch);
        });
    }

    public void testHdrPercentiles() {
        assertEstimateCloseToHeap(() -> {
            DoubleHistogram histogram = new DoubleHistogram(3);
            for (int i = 0; i < VALUES; i++) {
                histogram.recordValue(randomDouble() * 1000 + 1);
            }
            return new InternalHDRPercentiles("p", new double[] { 50, 99 }, histogram, false, RAW, null);
        });
    }

    private static final DocValueFormat RAW = DocValueFormat.RAW;

    private static HistogramUnionState tDigestUnion() {
        HistogramUnionState state = HistogramUnionState.create(HistogramUnionState.NOOP_BREAKER, 100.0);
        for (int i = 0; i < VALUES; i++) {
            state.add(randomDouble() * 1000);
        }
        return state;
    }

    /**
     * Asserts that the estimate for a terms aggregation with a sketch in each bucket is close to its heap, which is far more than its
     * serialized size.
     */
    private static void assertEstimateCloseToHeap(Supplier<InternalAggregation> sketch) {
        assumeTrue("the object sizes assume compressed references", RamUsageEstimator.COMPRESSED_REFS_ENABLED);
        List<StringTerms.Bucket> buckets = new ArrayList<>(BUCKETS);
        for (int i = 0; i < BUCKETS; i++) {
            BytesRef key = new BytesRef(String.format(Locale.ROOT, "%032d", i).getBytes(StandardCharsets.UTF_8));
            buckets.add(new StringTerms.Bucket(key, 1, InternalAggregations.from(sketch.get()), false, 0, DocValueFormat.RAW));
        }
        InternalAggregations aggregations = InternalAggregations.from(
            new StringTerms(
                "terms",
                BucketOrder.key(true),
                BucketOrder.count(false),
                BUCKETS,
                1,
                null,
                DocValueFormat.RAW,
                BUCKETS,
                false,
                0,
                buckets,
                0L
            )
        );
        long serialized = DelayableWriteable.getUncompressedSerializedSize(aggregations);
        long heap = RamUsageTester.ramUsed(aggregations);
        long estimate = aggregations.estimateHeapBytes();
        assertThat("the sketches must be far larger than their serialized form", heap, greaterThan(5 * serialized));
        assertThat("estimate over heap", (double) estimate / heap, both(greaterThan(0.8)).and(lessThan(1.3)));
    }
}
