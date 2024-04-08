/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class InternalHistogramTests extends InternalMultiBucketAggregationTestCase<InternalHistogram> {

    private boolean keyed;
    private DocValueFormat format;
    private int interval;
    private int minDocCount;
    private InternalHistogram.EmptyBucketInfo emptyBucketInfo;
    private int offset;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        keyed = randomBoolean();
        format = randomNumericDocValueFormat();
        // in order for reduction to work properly (and be realistic) we need to use the same interval, minDocCount, emptyBucketInfo
        // and offset in all randomly created aggs as part of the same test run. This is particularly important when minDocCount is
        // set to 0 as empty buckets need to be added to fill the holes.
        interval = randomIntBetween(1, 3);
        offset = randomIntBetween(0, 3);
        if (randomBoolean()) {
            minDocCount = randomIntBetween(1, 10);
            emptyBucketInfo = null;
        } else {
            minDocCount = 0;
            // it's ok if minBound and maxBound are outside the range of the generated buckets, that will just mean that
            // empty buckets won't be added before the first bucket and/or after the last one
            int minBound = randomInt(50) - 30;
            int maxBound = randomNumberOfBuckets() * interval + randomIntBetween(0, 10);
            emptyBucketInfo = new InternalHistogram.EmptyBucketInfo(interval, offset, minBound, maxBound, InternalAggregations.EMPTY);
        }
    }

    private double round(double key) {
        return Math.floor((key - offset) / interval) * interval + offset;
    }

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected InternalHistogram createTestInstance(String name, Map<String, Object> metadata, InternalAggregations aggregations) {
        final double base = round(randomInt(50) - 30);
        final int numBuckets = randomNumberOfBuckets();
        List<InternalHistogram.Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < numBuckets; ++i) {
            // rarely leave some holes to be filled up with empty buckets in case minDocCount is set to 0
            if (frequently()) {
                final int docCount = TestUtil.nextInt(random(), 1, 50);
                buckets.add(new InternalHistogram.Bucket(base + i * interval, docCount, keyed, format, aggregations));
            }
        }
        BucketOrder order = BucketOrder.key(randomBoolean());
        return new InternalHistogram(name, buckets, order, minDocCount, emptyBucketInfo, format, keyed, metadata);
    }

    // issue 26787
    public void testHandlesNaN() {
        InternalHistogram histogram = createTestInstance();
        InternalHistogram histogram2 = createTestInstance();
        List<InternalHistogram.Bucket> buckets = histogram.getBuckets();
        if (buckets == null || buckets.isEmpty()) {
            return;
        }

        // Set the key of one bucket to NaN. Must be the last bucket because NaN is greater than everything else.
        List<InternalHistogram.Bucket> newBuckets = new ArrayList<>(buckets.size());
        if (buckets.size() > 1) {
            newBuckets.addAll(buckets.subList(0, buckets.size() - 1));
        }
        InternalHistogram.Bucket b = buckets.get(buckets.size() - 1);
        newBuckets.add(new InternalHistogram.Bucket(Double.NaN, b.docCount, keyed, b.format, b.aggregations));

        List<InternalAggregation> reduceMe = List.of(histogram, histogram2);
        InternalAggregationTestCase.reduce(reduceMe, mockReduceContext(mockBuilder(reduceMe)).forPartialReduction());
    }

    public void testLargeReduce() {
        InternalHistogram largeHisto = new InternalHistogram(
            "h",
            List.of(),
            BucketOrder.key(true),
            0,
            new InternalHistogram.EmptyBucketInfo(5e-8, 0, 0, 100, InternalAggregations.EMPTY),
            DocValueFormat.RAW,
            false,
            null
        );
        expectReduceUsesTooManyBuckets(largeHisto, 100000);
        expectReduceThrowsRealMemoryBreaker(largeHisto);
    }

    @Override
    protected void assertReduced(InternalHistogram reduced, List<InternalHistogram> inputs) {
        TreeMap<Double, Long> expectedCounts = new TreeMap<>();
        for (Histogram histogram : inputs) {
            for (Histogram.Bucket bucket : histogram.getBuckets()) {
                expectedCounts.compute(
                    (Double) bucket.getKey(),
                    (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount()
                );
            }
        }
        if (minDocCount == 0) {
            double minBound = round(emptyBucketInfo.minBound);
            if (expectedCounts.isEmpty() && emptyBucketInfo.minBound <= emptyBucketInfo.maxBound) {
                expectedCounts.put(minBound, 0L);
            }
            if (expectedCounts.isEmpty() == false) {
                Double nextKey = expectedCounts.firstKey();
                while (nextKey < expectedCounts.lastKey()) {
                    expectedCounts.putIfAbsent(nextKey, 0L);
                    nextKey += interval;
                }
                while (minBound < expectedCounts.firstKey()) {
                    expectedCounts.put(expectedCounts.firstKey() - interval, 0L);
                }
                double maxBound = round(emptyBucketInfo.maxBound);
                while (expectedCounts.lastKey() < maxBound) {
                    expectedCounts.put(expectedCounts.lastKey() + interval, 0L);
                }
            }
        } else {
            expectedCounts.entrySet().removeIf(doubleLongEntry -> doubleLongEntry.getValue() < minDocCount);
        }

        Map<Double, Long> actualCounts = new TreeMap<>();
        for (Histogram.Bucket bucket : reduced.getBuckets()) {
            actualCounts.compute((Double) bucket.getKey(), (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
        }
        assertEquals(expectedCounts, actualCounts);
    }

    @Override
    protected InternalHistogram mutateInstance(InternalHistogram instance) {
        String name = instance.getName();
        List<InternalHistogram.Bucket> buckets = instance.getBuckets();
        BucketOrder order = instance.getOrder();
        long minDocCount = instance.getMinDocCount();
        Map<String, Object> metadata = instance.getMetadata();
        InternalHistogram.EmptyBucketInfo emptyBucketInfo = instance.emptyBucketInfo;
        switch (between(0, 4)) {
            case 0 -> name += randomAlphaOfLength(5);
            case 1 -> {
                buckets = new ArrayList<>(buckets);
                buckets.add(
                    new InternalHistogram.Bucket(
                        randomNonNegativeLong(),
                        randomIntBetween(1, 100),
                        keyed,
                        format,
                        InternalAggregations.EMPTY
                    )
                );
            }
            case 2 -> order = BucketOrder.count(randomBoolean());
            case 3 -> {
                minDocCount += between(1, 10);
                emptyBucketInfo = null;
            }
            case 4 -> {
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalHistogram(name, buckets, order, minDocCount, emptyBucketInfo, format, keyed, metadata);
    }
}
