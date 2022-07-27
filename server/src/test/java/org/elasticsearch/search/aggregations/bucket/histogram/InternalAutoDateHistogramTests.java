/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.Version;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder.RoundingInfo;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram.BucketInfo;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

public class InternalAutoDateHistogramTests extends InternalMultiBucketAggregationTestCase<InternalAutoDateHistogram> {
    protected InternalAutoDateHistogram createTestInstance(
        String name,
        Map<String, Object> metadata,
        InternalAggregations aggregations,
        long startingDate,
        RoundingInfo[] roundingInfos,
        int roundingIndex,
        DocValueFormat format
    ) {
        int nbBuckets = randomNumberOfBuckets();
        int targetBuckets = randomIntBetween(1, nbBuckets * 2 + 1);
        List<InternalAutoDateHistogram.Bucket> buckets = new ArrayList<>(nbBuckets);

        long interval = randomIntBetween(1, 3);
        long intervalMillis = roundingInfos[roundingIndex].roughEstimateDurationMillis * interval;

        for (int i = 0; i < nbBuckets; i++) {
            long key = startingDate + (intervalMillis * i);
            buckets.add(i, new InternalAutoDateHistogram.Bucket(key, randomIntBetween(1, 100), format, aggregations));
        }
        BucketInfo bucketInfo = new BucketInfo(roundingInfos, roundingIndex, InternalAggregations.EMPTY);
        return new InternalAutoDateHistogram(name, buckets, targetBuckets, bucketInfo, format, metadata, randomNonNegativeLong());
    }

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected InternalAutoDateHistogram createTestInstance(String name, Map<String, Object> metadata, InternalAggregations aggregations) {
        RoundingInfo[] roundingInfos = AutoDateHistogramAggregationBuilder.buildRoundings(null, null);
        int roundingIndex = between(0, roundingInfos.length - 1);
        return createTestInstance(
            name,
            metadata,
            aggregations,
            randomLongBetween(0, utcMillis("2050-01-01")),
            roundingInfos,
            roundingIndex,
            randomDateDocValueFormat()
        );
    }

    /*
     * This test was added to reproduce a bug where getAppropriateRounding was only ever using the first innerIntervals
     * passed in, instead of using the interval associated with the loop.
     */
    public void testGetAppropriateRoundingUsesCorrectIntervals() {
        RoundingInfo[] roundings = new RoundingInfo[6];
        ZoneId timeZone = ZoneOffset.UTC;
        // Since we pass 0 as the starting index to getAppropriateRounding, we'll also use
        // an innerInterval that is quite large, such that targetBuckets * roundings[i].getMaximumInnerInterval()
        // will be larger than the estimate.
        roundings[0] = new RoundingInfo(Rounding.DateTimeUnit.SECOND_OF_MINUTE, timeZone, 1000L, "s", 1000);
        roundings[1] = new RoundingInfo(Rounding.DateTimeUnit.MINUTES_OF_HOUR, timeZone, 60 * 1000L, "m", 1, 5, 10, 30);
        roundings[2] = new RoundingInfo(Rounding.DateTimeUnit.HOUR_OF_DAY, timeZone, 60 * 60 * 1000L, "h", 1, 3, 12);

        OffsetDateTime timestamp = Instant.parse("2018-01-01T00:00:01.000Z").atOffset(ZoneOffset.UTC);
        // We want to pass a roundingIdx of zero, because in order to reproduce this bug, we need the function
        // to increment the rounding (because the bug was that the function would not use the innerIntervals
        // from the new rounding.
        int result = InternalAutoDateHistogram.getAppropriateRounding(
            timestamp.toEpochSecond() * 1000,
            timestamp.plusDays(1).toEpochSecond() * 1000,
            0,
            roundings,
            25
        );
        assertThat(result, equalTo(2));
    }

    @Override
    protected BuilderAndToReduce<InternalAutoDateHistogram> randomResultsToReduce(String name, int size) {
        long startingDate = randomLongBetween(0, utcMillis("2050-01-01"));
        RoundingInfo[] roundingInfos = AutoDateHistogramAggregationBuilder.buildRoundings(null, null);
        int roundingIndex = between(0, roundingInfos.length - 1);
        DocValueFormat format = randomDateDocValueFormat();
        List<InternalAutoDateHistogram> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            long thisResultStart = startingDate;
            thisResultStart += usually() ? 0 : randomFrom(TimeUnit.MINUTES, TimeUnit.HOURS, TimeUnit.DAYS).toMillis(between(1, 10000));
            result.add(createTestInstance(name, null, InternalAggregations.EMPTY, thisResultStart, roundingInfos, roundingIndex, format));
        }
        return new BuilderAndToReduce<>(mock(AggregationBuilder.class), result);
    }

    @Override
    protected void assertReduced(InternalAutoDateHistogram reduced, List<InternalAutoDateHistogram> inputs) {
        int totalBucketConut = 0;
        long lowest = Long.MAX_VALUE;
        long highest = 0;

        for (InternalAutoDateHistogram histogram : inputs) {
            for (Histogram.Bucket bucket : histogram.getBuckets()) {
                long bucketKey = ((ZonedDateTime) bucket.getKey()).toInstant().toEpochMilli();
                if (bucketKey < lowest) {
                    lowest = bucketKey;
                }
                if (bucketKey > highest) {
                    highest = bucketKey;
                }
                totalBucketConut++;
            }
        }

        int roundingIndex = reduced.getBucketInfo().roundingIdx;
        RoundingInfo roundingInfo = AutoDateHistogramAggregationBuilder.buildRoundings(null, null)[roundingIndex];
        Rounding.Prepared prepared = totalBucketConut > 0 ? roundingInfo.rounding.prepare(lowest, highest) : null;

        long normalizedDuration = (highest - lowest) / roundingInfo.getRoughEstimateDurationMillis();
        int innerIntervalIndex = 0;

        /*
         * Guess the interval to use based on the roughly estimated
         * duration. It'll be accurate or it'll produce more buckets
         * than we need but it is quick.
         */
        if (normalizedDuration != 0) {
            for (int j = roundingInfo.innerIntervals.length - 1; j >= 0; j--) {
                int interval = roundingInfo.innerIntervals[j];
                if (normalizedDuration / interval < reduced.getBuckets().size()) {
                    innerIntervalIndex = j;
                }
            }
        }

        /*
         * Next pick smaller intervals until we find the one that makes the right
         * number of buckets.
         */
        int innerIntervalToUse;
        if (totalBucketConut == 0) {
            innerIntervalToUse = roundingInfo.innerIntervals[0];
        } else {
            do {
                innerIntervalToUse = roundingInfo.innerIntervals[innerIntervalIndex];
                int bucketCountAtInterval = getBucketCount(lowest, highest, prepared, innerIntervalToUse);
                if (bucketCountAtInterval == reduced.getBuckets().size()) {
                    break;
                }
                if (bucketCountAtInterval < reduced.getBuckets().size()) {
                    innerIntervalToUse = roundingInfo.innerIntervals[Math.max(0, innerIntervalIndex - 1)];
                    break;
                }
            } while (++innerIntervalIndex < roundingInfo.innerIntervals.length);
        }

        assertThat(reduced.getInterval().toString(), equalTo(innerIntervalToUse + roundingInfo.unitAbbreviation));
        Map<Instant, Long> expectedCounts = new TreeMap<>();
        if (totalBucketConut > 0) {
            long keyForBucket = prepared.round(lowest);
            while (keyForBucket <= prepared.round(highest)) {
                long nextKey = keyForBucket;
                for (int i = 0; i < innerIntervalToUse; i++) {
                    nextKey = prepared.nextRoundingValue(nextKey);
                }
                Instant key = Instant.ofEpochMilli(keyForBucket);
                expectedCounts.put(key, 0L);

                // Iterate through the input buckets, and for each bucket, determine if it's inside
                // the range of the bucket in the outer loop. if it is, add the doc count to the total
                // for that bucket.

                for (InternalAutoDateHistogram histogram : inputs) {
                    for (Histogram.Bucket bucket : histogram.getBuckets()) {
                        long roundedBucketKey = prepared.round(((ZonedDateTime) bucket.getKey()).toInstant().toEpochMilli());
                        long docCount = bucket.getDocCount();
                        if (roundedBucketKey >= keyForBucket && roundedBucketKey < nextKey) {
                            expectedCounts.compute(key, (k, oldValue) -> (oldValue == null ? 0 : oldValue) + docCount);
                        }
                    }
                }
                keyForBucket = nextKey;
            }

            // If there is only a single bucket, and we haven't added it above, add a bucket with no documents.
            // this step is necessary because of the roundedBucketKey < keyForBucket + intervalInMillis above.
            if (prepared.round(lowest) == prepared.round(highest) && expectedCounts.isEmpty()) {
                expectedCounts.put(Instant.ofEpochMilli(prepared.round(lowest)), 0L);
            }
        }

        // pick out the actual reduced values to the make the assertion more readable
        Map<Instant, Long> actualCounts = new TreeMap<>();
        for (Histogram.Bucket bucket : reduced.getBuckets()) {
            actualCounts.compute(
                ((ZonedDateTime) bucket.getKey()).toInstant(),
                (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount()
            );
        }
        assertEquals(expectedCounts, actualCounts);

        DateHistogramInterval expectedInterval;
        if (reduced.getBuckets().size() == 1) {
            expectedInterval = reduced.getInterval();
        } else {
            expectedInterval = new DateHistogramInterval(innerIntervalToUse + roundingInfo.unitAbbreviation);
        }
        assertThat(reduced.getInterval(), equalTo(expectedInterval));
    }

    private int getBucketCount(long min, long max, Rounding.Prepared prepared, int interval) {
        int bucketCount = 0;
        long key = prepared.round(min);
        while (key < max) {
            for (int i = 0; i < interval; i++) {
                key = prepared.nextRoundingValue(key);
            }
            bucketCount++;
        }
        return bucketCount;
    }

    @Override
    protected Class<ParsedAutoDateHistogram> implementationClass() {
        return ParsedAutoDateHistogram.class;
    }

    @Override
    protected InternalAutoDateHistogram mutateInstance(InternalAutoDateHistogram instance) {
        String name = instance.getName();
        List<InternalAutoDateHistogram.Bucket> buckets = instance.getBuckets();
        int targetBuckets = instance.getTargetBuckets();
        BucketInfo bucketInfo = instance.getBucketInfo();
        Map<String, Object> metadata = instance.getMetadata();
        long interval = instance.getBucketInnerInterval();
        switch (between(0, 4)) {
            case 0 -> name += randomAlphaOfLength(5);
            case 1 -> {
                buckets = new ArrayList<>(buckets);
                buckets.add(
                    new InternalAutoDateHistogram.Bucket(
                        randomNonNegativeLong(),
                        randomIntBetween(1, 100),
                        instance.getFormatter(),
                        InternalAggregations.EMPTY
                    )
                );
            }
            case 2 -> {
                int roundingIdx = bucketInfo.roundingIdx == bucketInfo.roundingInfos.length - 1 ? 0 : bucketInfo.roundingIdx + 1;
                bucketInfo = new BucketInfo(bucketInfo.roundingInfos, roundingIdx, bucketInfo.emptySubAggregations);
            }
            case 3 -> {
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
            }
            case 4 -> interval = randomNonNegativeLong();
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalAutoDateHistogram(name, buckets, targetBuckets, bucketInfo, instance.getFormatter(), metadata, interval);
    }

    public void testReduceSecond() {
        InternalAutoDateHistogram h = new ReduceTestBuilder(10).bucket("1970-01-01T00:00:01", 1)
            .bucket("1970-01-01T00:00:02", 1)
            .bucket("1970-01-01T00:00:03", 1)
            .finishShardResult("s", 1)
            .bucket("1970-01-01T00:00:03", 1)
            .bucket("1970-01-01T00:00:04", 1)
            .finishShardResult("s", 1)
            .reduce();
        assertThat(
            keys(h),
            equalTo(Arrays.asList("1970-01-01T00:00:01Z", "1970-01-01T00:00:02Z", "1970-01-01T00:00:03Z", "1970-01-01T00:00:04Z"))
        );
        assertThat(docCounts(h), equalTo(Arrays.asList(1, 1, 2, 1)));
    }

    public void testReduceThirtySeconds() {
        InternalAutoDateHistogram h = new ReduceTestBuilder(10).bucket("1970-01-01T00:00:00", 1)
            .bucket("1970-01-01T00:00:30", 1)
            .bucket("1970-01-01T00:02:00", 1)
            .finishShardResult("s", 1)
            .bucket("1970-01-01T00:00:30", 1)
            .bucket("1970-01-01T00:01:00", 1)
            .finishShardResult("s", 1)
            .reduce();
        assertThat(
            keys(h),
            equalTo(
                Arrays.asList(
                    "1970-01-01T00:00:00Z",
                    "1970-01-01T00:00:30Z",
                    "1970-01-01T00:01:00Z",
                    "1970-01-01T00:01:30Z",
                    "1970-01-01T00:02:00Z"
                )
            )
        );
        assertThat(docCounts(h), equalTo(Arrays.asList(1, 2, 1, 0, 1)));
    }

    public void testReduceBumpsInnerRange() {
        InternalAutoDateHistogram h = new ReduceTestBuilder(2).bucket("1970-01-01T00:00:01", 1)
            .bucket("1970-01-01T00:00:02", 1)
            .finishShardResult("s", 1)
            .bucket("1970-01-01T00:00:00", 1)
            .bucket("1970-01-01T00:00:05", 1)
            .finishShardResult("s", 5)
            .reduce();
        assertThat(keys(h), equalTo(Arrays.asList("1970-01-01T00:00:00Z", "1970-01-01T00:00:05Z")));
        assertThat(docCounts(h), equalTo(Arrays.asList(3, 1)));
    }

    public void testReduceBumpsRounding() {
        InternalAutoDateHistogram h = new ReduceTestBuilder(2).bucket("1970-01-01T00:00:01", 1)
            .bucket("1970-01-01T00:00:02", 1)
            .finishShardResult("s", 1)
            .bucket("1970-01-01T00:00:00", 1)
            .bucket("1970-01-01T00:01:00", 1)
            .finishShardResult("m", 1)
            .reduce();
        assertThat(keys(h), equalTo(Arrays.asList("1970-01-01T00:00:00Z", "1970-01-01T00:01:00Z")));
        assertThat(docCounts(h), equalTo(Arrays.asList(3, 1)));
    }

    private static class ReduceTestBuilder {
        private static final DocValueFormat FORMAT = new DocValueFormat.DateTime(
            DateFormatter.forPattern("date_time_no_millis"),
            ZoneOffset.UTC,
            DateFieldMapper.Resolution.MILLISECONDS
        );
        private final List<InternalAggregation> results = new ArrayList<>();
        private final List<InternalAutoDateHistogram.Bucket> buckets = new ArrayList<>();
        private final int targetBuckets;

        ReduceTestBuilder(int targetBuckets) {
            this.targetBuckets = targetBuckets;
        }

        ReduceTestBuilder bucket(String key, long docCount) {
            buckets.add(new InternalAutoDateHistogram.Bucket(utcMillis(key), docCount, FORMAT, InternalAggregations.EMPTY));
            return this;
        }

        ReduceTestBuilder finishShardResult(String whichRounding, int innerInterval) {
            RoundingInfo roundings[] = AutoDateHistogramAggregationBuilder.buildRoundings(ZoneOffset.UTC, null);
            int roundingIdx = -1;
            for (int i = 0; i < roundings.length; i++) {
                if (roundings[i].unitAbbreviation.equals(whichRounding)) {
                    roundingIdx = i;
                    break;
                }
            }
            assertThat("rounding [" + whichRounding + "] should be in " + Arrays.toString(roundings), roundingIdx, greaterThan(-1));
            assertTrue(
                Arrays.toString(roundings[roundingIdx].innerIntervals) + " must contain " + innerInterval,
                Arrays.binarySearch(roundings[roundingIdx].innerIntervals, innerInterval) >= 0
            );
            BucketInfo bucketInfo = new BucketInfo(roundings, roundingIdx, InternalAggregations.EMPTY);
            results.add(
                new InternalAutoDateHistogram(
                    "test",
                    new ArrayList<>(buckets),
                    targetBuckets,
                    bucketInfo,
                    FORMAT,
                    emptyMap(),
                    innerInterval
                )
            );
            buckets.clear();
            return this;
        }

        InternalAutoDateHistogram reduce() {
            assertThat("finishShardResult must be called before reduce", buckets, empty());
            return (InternalAutoDateHistogram) results.get(0).reduce(results, emptyReduceContextBuilder().forFinalReduction());
        }
    }

    private static long utcMillis(String time) {
        return DateFormatter.forPattern("date_optional_time").parseMillis(time);
    }

    private List<String> keys(InternalAutoDateHistogram h) {
        return h.getBuckets().stream().map(InternalAutoDateHistogram.Bucket::getKeyAsString).collect(toList());
    }

    private List<Integer> docCounts(InternalAutoDateHistogram h) {
        return h.getBuckets().stream().map(b -> (int) b.getDocCount()).collect(toList());
    }

    public void testCreateWithReplacementBuckets() {
        InternalAutoDateHistogram noInterval = createTestInstance();
        InternalAutoDateHistogram orig = new InternalAutoDateHistogram(
            noInterval.getName(),
            noInterval.getBuckets(),
            noInterval.getTargetBuckets(),
            noInterval.getBucketInfo(),
            noInterval.getFormatter(),
            noInterval.getMetadata(),
            randomLong()
        );
        InternalAutoDateHistogram copy = orig.create(List.of());
        assertThat(copy.getName(), equalTo(orig.getName()));
        assertThat(copy.getBuckets(), hasSize(0));
        assertThat(copy.getTargetBuckets(), equalTo(orig.getTargetBuckets()));
        assertThat(copy.getBucketInfo(), equalTo(orig.getBucketInfo()));
        assertThat(copy.getFormatter(), equalTo(orig.getFormatter()));
        assertThat(copy.getInterval(), equalTo(orig.getInterval()));
    }

    public void testSerializationPre830() throws IOException {
        // we need to test without sub-aggregations, otherwise we need to also update the interval within the inner aggs
        InternalAutoDateHistogram instance = createTestInstance(
            randomAlphaOfLengthBetween(3, 7),
            createTestMetadata(),
            InternalAggregations.EMPTY
        );
        Version version = VersionUtils.randomVersionBetween(
            random(),
            Version.CURRENT.minimumCompatibilityVersion(),
            VersionUtils.getPreviousVersion(Version.V_8_3_0)
        );
        InternalAutoDateHistogram deserialized = copyInstance(instance, version);
        assertEquals(1, deserialized.getBucketInnerInterval());

        InternalAutoDateHistogram modified = new InternalAutoDateHistogram(
            deserialized.getName(),
            deserialized.getBuckets(),
            deserialized.getTargetBuckets(),
            deserialized.getBucketInfo(),
            deserialized.getFormatter(),
            deserialized.getMetadata(),
            instance.getBucketInnerInterval()
        );
        assertEqualInstances(instance, modified);
    }

    public void testReadFromPre830() throws IOException {
        byte[] bytes = Base64.getDecoder()
            .decode(
                "BG5hbWUKAAYBCAFa6AcEAAAAAQAAAAUAAAAKAAAAHgFzBnNlY29uZAEHAVrg1AMEAAAAAQAAAAUAAAAKAAA"
                    + "AHgFtBm1pbnV0ZQEGAVqA3dsBAwAAAAEAAAADAAAADAFoBGhvdXIBBQFagLiZKQIAAAABAAAABwFk"
                    + "A2RheQEEAVqAkPvTCQIAAAABAAAAAwFNBW1vbnRoAQIBWoDYxL11BgAAAAEAAAAFAAAACgAAABQAA"
                    + "AAyAAAAZAF5BHllYXIAAARib29sAQAAAAAAAAAKZAADAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            );
        try (StreamInput in = new NamedWriteableAwareStreamInput(new BytesArray(bytes).streamInput(), getNamedWriteableRegistry())) {
            in.setVersion(Version.V_8_2_0);
            InternalAutoDateHistogram deserialized = new InternalAutoDateHistogram(in);
            assertEquals("name", deserialized.getName());
            assertEquals(1, deserialized.getBucketInnerInterval());
            assertEquals(1, deserialized.getBuckets().size());
            InternalAutoDateHistogram.Bucket bucket = deserialized.getBuckets().iterator().next();
            assertEquals(10, bucket.key);
            assertEquals(100, bucket.docCount);
        }
    }
}
