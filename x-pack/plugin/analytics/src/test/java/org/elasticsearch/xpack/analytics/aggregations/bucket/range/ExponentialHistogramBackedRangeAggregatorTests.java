/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.aggregations.bucket.range;

import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialScaleUtils;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.xpack.analytics.aggregations.ExponentialHistogramAggregatorTestCase;
import org.elasticsearch.xpack.analytics.mapper.ExponentialHistogramFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class ExponentialHistogramBackedRangeAggregatorTests extends ExponentialHistogramAggregatorTestCase {

    private static final String FIELD_NAME = "histo_field";

    @SuppressWarnings({ "unchecked" })
    public void testNonOverlapping() throws Exception {
        ExponentialHistogramCircuitBreaker noopBreaker = ExponentialHistogramCircuitBreaker.noop();
        List<ExponentialHistogram> histograms = List.of(
            ExponentialHistogram.create(100, noopBreaker, -99, -79, -49, -30, -9, -4, -1, 2, 15, 40, 80),
            ExponentialHistogram.create(100, noopBreaker, -90, -70, -40, -19, -8, -3, -0.5, 0.1, 7, 25, 60, 95),
            ExponentialHistogram.create(100, noopBreaker, -200, -150, -75, -25, -12, -2, 0, 5, 11, 30, 51, 110)
        );

        testCase(iw -> histograms.forEach(histo -> addHistogramDoc(iw, FIELD_NAME, histo)), aggBuilder -> {
            aggBuilder.addUnboundedTo(-100)
                .addRange(-100, -50)
                .addRange(-50, -20)
                .addRange(-20, -5)
                .addRange(-5, 0)
                .addRange(0, 10)
                .addRange(10, 50)
                .addRange(50, 100)
                .addUnboundedFrom(100);
        }, range -> {
            assertTrue(AggregationInspectionHelper.hasValue(range));
            List<? extends InternalRange.Bucket> buckets = range.getBuckets();
            assertEquals(9, buckets.size());

            assertEquals("*--100.0", buckets.get(0).getKeyAsString());
            assertEquals(2, buckets.get(0).getDocCount());
            assertEquals("-100.0--50.0", buckets.get(1).getKeyAsString());
            assertEquals(5, buckets.get(1).getDocCount());
            assertEquals("-50.0--20.0", buckets.get(2).getKeyAsString());
            assertEquals(4, buckets.get(2).getDocCount());
            assertEquals("-20.0--5.0", buckets.get(3).getKeyAsString());
            assertEquals(4, buckets.get(3).getDocCount());
            assertEquals("-5.0-0.0", buckets.get(4).getKeyAsString());
            assertEquals(5, buckets.get(4).getDocCount());
            assertEquals("0.0-10.0", buckets.get(5).getKeyAsString());
            assertEquals(5, buckets.get(5).getDocCount());
            assertEquals("10.0-50.0", buckets.get(6).getKeyAsString());
            assertEquals(5, buckets.get(6).getDocCount());
            assertEquals("50.0-100.0", buckets.get(7).getKeyAsString());
            assertEquals(4, buckets.get(7).getDocCount());
            assertEquals("100.0-*", buckets.get(8).getKeyAsString());
            assertEquals(1, buckets.get(8).getDocCount());
        });
    }

    @SuppressWarnings({ "unchecked" })
    public void testOverlapping() throws Exception {
        ExponentialHistogramCircuitBreaker noopBreaker = ExponentialHistogramCircuitBreaker.noop();
        List<ExponentialHistogram> histograms = List.of(
            ExponentialHistogram.create(100, noopBreaker, -99, -79, -49, -30, -9, -4, -1, 2, 15, 40, 80),
            ExponentialHistogram.create(100, noopBreaker, -90, -70, -40, -19, -8, -3, -0.5, 0.1, 7, 25, 60, 95),
            ExponentialHistogram.create(100, noopBreaker, -200, -150, -75, -25, -12, -2, 0, 5, 11, 30, 51, 110)
        );

        testCase(iw -> histograms.forEach(histo -> addHistogramDoc(iw, FIELD_NAME, histo)), aggBuilder -> {
            aggBuilder.addUnboundedTo(0)
                .addRange(-100, -20)
                .addRange(-80, -10)
                .addRange(-50, 50)
                .addRange(0, 20)
                .addRange(0, 100)
                .addRange(10, 50)
                .addUnboundedFrom(50);
        }, range -> {
            assertTrue(AggregationInspectionHelper.hasValue(range));
            List<? extends InternalRange.Bucket> buckets = range.getBuckets();
            assertEquals(8, buckets.size());

            assertEquals("*-0.0", buckets.get(0).getKeyAsString());
            assertEquals(20, buckets.get(0).getDocCount());
            assertEquals("-100.0--20.0", buckets.get(1).getKeyAsString());
            assertEquals(9, buckets.get(1).getDocCount());
            assertEquals("-80.0--10.0", buckets.get(2).getKeyAsString());
            assertEquals(9, buckets.get(2).getDocCount());
            assertEquals("-50.0-50.0", buckets.get(3).getKeyAsString());
            assertEquals(23, buckets.get(3).getDocCount());
            assertEquals("0.0-20.0", buckets.get(4).getKeyAsString());
            assertEquals(7, buckets.get(4).getDocCount());
            assertEquals("0.0-100.0", buckets.get(5).getKeyAsString());
            assertEquals(14, buckets.get(5).getDocCount());
            assertEquals("10.0-50.0", buckets.get(6).getKeyAsString());
            assertEquals(5, buckets.get(6).getDocCount());
            assertEquals("50.0-*", buckets.get(7).getKeyAsString());
            assertEquals(5, buckets.get(7).getDocCount());
        });
    }

    @SuppressWarnings({ "unchecked" })
    public void testRandomRanges() throws Exception {
        List<ExponentialHistogram> histograms = createRandomHistograms(randomIntBetween(1, 100));
        int numRanges = randomIntBetween(1, 10);
        double[] bounds = new double[numRanges + 1];
        for (int i = 0; i < bounds.length; i++) {
            bounds[i] = randomDoubleBetween(-100, 100, true);
        }
        Arrays.sort(bounds);

        RangeAggregator.Range[] ranges = new RangeAggregator.Range[numRanges];
        for (int i = 0; i < numRanges; i++) {
            ranges[i] = new RangeAggregator.Range(null, bounds[i], bounds[i + 1]);
        }

        testCase(iw -> histograms.forEach(histo -> addHistogramDoc(iw, FIELD_NAME, histo)), aggBuilder -> {
            for (RangeAggregator.Range r : ranges) {
                aggBuilder.addRange(r);
            }
        }, range -> {
            long[] expectedCounts = computeExpectedRangeCounts(histograms, ranges);
            List<? extends InternalRange.Bucket> buckets = range.getBuckets();
            assertEquals(ranges.length, buckets.size());
            for (int i = 0; i < buckets.size(); i++) {
                assertEquals("bucket " + buckets.get(i).getKey(), expectedCounts[i], buckets.get(i).getDocCount());
            }
        });
    }

    @SuppressWarnings({ "unchecked" })
    public void testNoDocs() throws Exception {
        testCase(iw -> {
            // Intentionally not writing any docs
        }, aggBuilder -> aggBuilder.addRange(0, 10), range -> {
            List<? extends InternalRange.Bucket> buckets = range.getBuckets();
            assertEquals(1, buckets.size());
            assertEquals(0, buckets.get(0).getDocCount());
            assertFalse(AggregationInspectionHelper.hasValue(range));
        });
    }

    public void testSubAggs() throws Exception {
        ExponentialHistogramCircuitBreaker noopBreaker = ExponentialHistogramCircuitBreaker.noop();
        List<ExponentialHistogram> histograms = List.of(ExponentialHistogram.create(100, noopBreaker, -4.5, 4.3));

        RangeAggregationBuilder aggBuilder = new RangeAggregationBuilder("my_agg").field(FIELD_NAME)
            .addRange(-1.0, 3.0)
            .subAggregation(new TopHitsAggregationBuilder("top_hits"));

        var fieldType = defaultFieldType();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            testCase(
                iw -> histograms.forEach(histo -> addHistogramDoc(iw, FIELD_NAME, histo)),
                aggBuilder,
                fieldType,
                range -> fail("should have thrown")
            );
        });
        assertEquals("Range aggregation on exponential_histogram fields does not support sub-aggregations", e.getMessage());
    }

    @SuppressWarnings("rawtypes")
    private void testCase(
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<RangeAggregationBuilder> aggCustomizer,
        Consumer<InternalRange> verify
    ) throws IOException {
        var fieldType = defaultFieldType();
        RangeAggregationBuilder aggBuilder = new RangeAggregationBuilder("my_agg").field(FIELD_NAME);
        aggCustomizer.accept(aggBuilder);
        testCase(buildIndex, aggBuilder, fieldType, verify);
    }

    @SuppressWarnings("rawtypes")
    private void testCase(
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        AggregationBuilder aggBuilder,
        MappedFieldType fieldType,
        Consumer<InternalRange> verify
    ) throws IOException {
        testCase(buildIndex, verify, new AggTestConfig(aggBuilder, fieldType));
    }

    private MappedFieldType defaultFieldType() {
        return new ExponentialHistogramFieldMapper.ExponentialHistogramFieldType(FIELD_NAME, Collections.emptyMap(), null);
    }

    /**
     * Compute expected range counts by iterating over all histogram bucket centers and assigning them to ranges,
     * mirroring the logic of ExponentialHistogramBackedRangeAggregator, but without the fancy binary search optimizations.
     */
    private long[] computeExpectedRangeCounts(List<ExponentialHistogram> histograms, RangeAggregator.Range[] ranges) {
        long[] counts = new long[ranges.length];
        for (ExponentialHistogram histogram : histograms) {
            BucketIterator negIt = histogram.negativeBuckets().iterator();
            while (negIt.hasNext()) {
                double center = -ExponentialScaleUtils.getPointOfLeastRelativeError(negIt.peekIndex(), negIt.scale());
                center = Math.clamp(center, histogram.min(), histogram.max());
                addToRanges(counts, ranges, center, negIt.peekCount());
                negIt.advance();
            }
            if (histogram.zeroBucket().count() > 0) {
                addToRanges(counts, ranges, 0.0, histogram.zeroBucket().count());
            }
            BucketIterator posIt = histogram.positiveBuckets().iterator();
            while (posIt.hasNext()) {
                double center = ExponentialScaleUtils.getPointOfLeastRelativeError(posIt.peekIndex(), posIt.scale());
                center = Math.clamp(center, histogram.min(), histogram.max());
                addToRanges(counts, ranges, center, posIt.peekCount());
                posIt.advance();
            }
        }
        return counts;
    }

    private void addToRanges(long[] counts, RangeAggregator.Range[] ranges, double value, long count) {
        for (int i = 0; i < ranges.length; i++) {
            if (ranges[i].matches(value)) {
                counts[i] += count;
            }
        }
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new RangeAggregationBuilder("_name").field(fieldName).addRange(0, 10);
    }
}
