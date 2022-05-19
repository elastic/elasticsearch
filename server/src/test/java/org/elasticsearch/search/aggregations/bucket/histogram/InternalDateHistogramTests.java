/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.Rounding.DateTimeUnit;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.core.TimeValue.timeValueHours;
import static org.elasticsearch.core.TimeValue.timeValueMinutes;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;

public class InternalDateHistogramTests extends InternalMultiBucketAggregationTestCase<InternalDateHistogram> {

    private boolean keyed;
    private DocValueFormat format;
    private long intervalMillis;
    private long baseMillis;
    private long minDocCount;
    private InternalDateHistogram.EmptyBucketInfo emptyBucketInfo;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        keyed = randomBoolean();
        format = randomDateDocValueFormat();
        // in order for reduction to work properly (and be realistic) we need to use the same interval, minDocCount, emptyBucketInfo
        // and base in all randomly created aggs as part of the same test run. This is particularly important when minDocCount is
        // set to 0 as empty buckets need to be added to fill the holes.
        long interval = randomIntBetween(1, 3);
        intervalMillis = randomFrom(timeValueSeconds(interval), timeValueMinutes(interval), timeValueHours(interval)).getMillis();
        Rounding rounding = Rounding.builder(TimeValue.timeValueMillis(intervalMillis)).build();
        long now = System.currentTimeMillis();
        baseMillis = rounding.prepare(now, now).round(now);
        if (randomBoolean()) {
            minDocCount = randomIntBetween(1, 10);
            emptyBucketInfo = null;
        } else {
            minDocCount = 0;
            LongBounds extendedBounds = null;
            if (randomBoolean()) {
                // it's ok if min and max are outside the range of the generated buckets, that will just mean that
                // empty buckets won't be added before the first bucket and/or after the last one
                long min = baseMillis - intervalMillis * randomNumberOfBuckets();
                long max = baseMillis + randomNumberOfBuckets() * intervalMillis;
                extendedBounds = new LongBounds(min, max);
            }
            emptyBucketInfo = new InternalDateHistogram.EmptyBucketInfo(rounding, InternalAggregations.EMPTY, extendedBounds);
        }
    }

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected InternalDateHistogram createTestInstance(String name, Map<String, Object> metadata, InternalAggregations aggregations) {
        return createTestInstance(name, metadata, aggregations, format);
    }

    @Override
    protected InternalDateHistogram createTestInstanceForXContent(String name, Map<String, Object> metadata, InternalAggregations subAggs) {
        // We have to force a format that won't throw away precision and cause duplicate fields
        DocValueFormat xContentCompatibleFormat = new DocValueFormat.DateTime(
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            ZoneOffset.UTC,
            Resolution.MILLISECONDS
        );
        return createTestInstance(name, metadata, subAggs, xContentCompatibleFormat);
    }

    private InternalDateHistogram createTestInstance(
        String name,
        Map<String, Object> metadata,
        InternalAggregations aggregations,
        DocValueFormat format
    ) {
        int nbBuckets = randomNumberOfBuckets();
        List<InternalDateHistogram.Bucket> buckets = new ArrayList<>(nbBuckets);
        // avoid having different random instance start from exactly the same base
        long startingDate = baseMillis - intervalMillis * randomNumberOfBuckets();
        for (int i = 0; i < nbBuckets; i++) {
            // rarely leave some holes to be filled up with empty buckets in case minDocCount is set to 0
            if (frequently()) {
                long key = startingDate + intervalMillis * i;
                buckets.add(new InternalDateHistogram.Bucket(key, randomIntBetween(1, 100), keyed, format, aggregations));
            }
        }
        BucketOrder order = BucketOrder.key(randomBoolean());
        return new InternalDateHistogram(name, buckets, order, minDocCount, 0L, emptyBucketInfo, format, keyed, metadata);
    }

    @Override
    protected void assertReduced(InternalDateHistogram reduced, List<InternalDateHistogram> inputs) {
        TreeMap<Long, Long> expectedCounts = new TreeMap<>();
        for (Histogram histogram : inputs) {
            for (Histogram.Bucket bucket : histogram.getBuckets()) {
                expectedCounts.compute(
                    ((ZonedDateTime) bucket.getKey()).toInstant().toEpochMilli(),
                    (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount()
                );
            }
        }
        if (minDocCount == 0) {
            long minBound = -1;
            long maxBound = -1;
            if (emptyBucketInfo.bounds != null) {
                Rounding.Prepared prepared = emptyBucketInfo.rounding.prepare(
                    emptyBucketInfo.bounds.getMin(),
                    emptyBucketInfo.bounds.getMax()
                );
                minBound = prepared.round(emptyBucketInfo.bounds.getMin());
                maxBound = prepared.round(emptyBucketInfo.bounds.getMax());
                if (expectedCounts.isEmpty() && minBound <= maxBound) {
                    expectedCounts.put(minBound, 0L);
                }
            }
            if (expectedCounts.isEmpty() == false) {
                Long nextKey = expectedCounts.firstKey();
                while (nextKey < expectedCounts.lastKey()) {
                    expectedCounts.putIfAbsent(nextKey, 0L);
                    nextKey += intervalMillis;
                }
                if (emptyBucketInfo.bounds != null) {
                    while (minBound < expectedCounts.firstKey()) {
                        expectedCounts.put(expectedCounts.firstKey() - intervalMillis, 0L);
                    }
                    while (expectedCounts.lastKey() < maxBound) {
                        expectedCounts.put(expectedCounts.lastKey() + intervalMillis, 0L);
                    }
                }
            }
        } else {
            expectedCounts.entrySet().removeIf(doubleLongEntry -> doubleLongEntry.getValue() < minDocCount);
        }

        Map<Long, Long> actualCounts = new TreeMap<>();
        for (Histogram.Bucket bucket : reduced.getBuckets()) {
            actualCounts.compute(
                ((ZonedDateTime) bucket.getKey()).toInstant().toEpochMilli(),
                (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount()
            );
        }
        assertEquals(expectedCounts, actualCounts);
    }

    @Override
    protected Class<ParsedDateHistogram> implementationClass() {
        return ParsedDateHistogram.class;
    }

    @Override
    protected InternalDateHistogram mutateInstance(InternalDateHistogram instance) {
        String name = instance.getName();
        List<InternalDateHistogram.Bucket> buckets = instance.getBuckets();
        BucketOrder order = instance.getOrder();
        long minDocCount = instance.getMinDocCount();
        long offset = instance.getOffset();
        InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = instance.emptyBucketInfo;
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 5)) {
            case 0 -> name += randomAlphaOfLength(5);
            case 1 -> {
                buckets = new ArrayList<>(buckets);
                buckets.add(
                    new InternalDateHistogram.Bucket(
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
            case 4 -> offset += between(1, 20);
            case 5 -> {
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalDateHistogram(name, buckets, order, minDocCount, offset, emptyBucketInfo, format, keyed, metadata);
    }

    public void testLargeReduce() {
        expectReduceUsesTooManyBuckets(
            new InternalDateHistogram(
                "h",
                List.of(),
                BucketOrder.key(true),
                0,
                0,
                new InternalDateHistogram.EmptyBucketInfo(
                    Rounding.builder(DateTimeUnit.SECOND_OF_MINUTE).build(),
                    InternalAggregations.EMPTY,
                    new LongBounds(
                        DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2018-01-01T00:00:00Z"),
                        DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2021-01-01T00:00:00Z")
                    )
                ),
                DocValueFormat.RAW,
                false,
                null
            ),
            100000
        );
    }
}
