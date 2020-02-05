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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder.RoundingInfo;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram.BucketInfo;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.common.unit.TimeValue.timeValueHours;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.equalTo;

public class InternalAutoDateHistogramTests extends InternalMultiBucketAggregationTestCase<InternalAutoDateHistogram> {

    private DocValueFormat format;
    private RoundingInfo[] roundingInfos;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        format = randomNumericDocValueFormat();
    }

    @Override
    protected InternalAutoDateHistogram createTestInstance(String name,
                                                       List<PipelineAggregator> pipelineAggregators,
                                                       Map<String, Object> metaData,
                                                       InternalAggregations aggregations) {

        roundingInfos = AutoDateHistogramAggregationBuilder.buildRoundings(null, null);
        int nbBuckets = randomNumberOfBuckets();
        int targetBuckets = randomIntBetween(1, nbBuckets * 2 + 1);
        List<InternalAutoDateHistogram.Bucket> buckets = new ArrayList<>(nbBuckets);

        long startingDate = System.currentTimeMillis();

        long interval = randomIntBetween(1, 3);
        long intervalMillis = randomFrom(timeValueSeconds(interval), timeValueMinutes(interval), timeValueHours(interval)).getMillis();

        for (int i = 0; i < nbBuckets; i++) {
            long key = startingDate + (intervalMillis * i);
            buckets.add(i, new InternalAutoDateHistogram.Bucket(key, randomIntBetween(1, 100), format, aggregations));
        }
        InternalAggregations subAggregations = new InternalAggregations(Collections.emptyList());
        BucketInfo bucketInfo = new BucketInfo(roundingInfos, randomIntBetween(0, roundingInfos.length - 1), subAggregations);
        return new InternalAutoDateHistogram(name, buckets, targetBuckets, bucketInfo, format, pipelineAggregators, metaData, 1);
    }

    /*
    This test was added to reproduce a bug where getAppropriateRounding was only ever using the first innerIntervals
    passed in, instead of using the interval associated with the loop.
     */
    public void testGetAppropriateRoundingUsesCorrectIntervals() {
        RoundingInfo[] roundings = new RoundingInfo[6];
        ZoneId timeZone = ZoneOffset.UTC;
        // Since we pass 0 as the starting index to getAppropriateRounding, we'll also use
        // an innerInterval that is quite large, such that targetBuckets * roundings[i].getMaximumInnerInterval()
        // will be larger than the estimate.
        roundings[0] = new RoundingInfo(Rounding.DateTimeUnit.SECOND_OF_MINUTE, timeZone,
            1000L, "s", 1000);
        roundings[1] = new RoundingInfo(Rounding.DateTimeUnit.MINUTES_OF_HOUR, timeZone,
            60 * 1000L, "m", 1, 5, 10, 30);
        roundings[2] = new RoundingInfo(Rounding.DateTimeUnit.HOUR_OF_DAY, timeZone,
            60 * 60 * 1000L, "h", 1, 3, 12);

        OffsetDateTime timestamp = Instant.parse("2018-01-01T00:00:01.000Z").atOffset(ZoneOffset.UTC);
        // We want to pass a roundingIdx of zero, because in order to reproduce this bug, we need the function
        // to increment the rounding (because the bug was that the function would not use the innerIntervals
        // from the new rounding.
        int result = InternalAutoDateHistogram.getAppropriateRounding(timestamp.toEpochSecond()*1000,
            timestamp.plusDays(1).toEpochSecond()*1000, 0, roundings, 25);
        assertThat(result, equalTo(2));
    }

    public void testReduceRandom() {
        super.testReduceRandom();
    }

    @Override
    protected void assertReduced(InternalAutoDateHistogram reduced, List<InternalAutoDateHistogram> inputs) {

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
            }
        }

        int roundingIndex = reduced.getBucketInfo().roundingIdx;
        RoundingInfo roundingInfo = roundingInfos[roundingIndex];

        long normalizedDuration = (highest - lowest) / roundingInfo.getRoughEstimateDurationMillis();
        long innerIntervalToUse = roundingInfo.innerIntervals[0];
        int innerIntervalIndex = 0;

        // First, try to calculate the correct innerInterval using the normalizedDuration.
        // This handles cases where highest and lowest are further apart than the interval being used.
        if (normalizedDuration != 0) {
            for (int j = roundingInfo.innerIntervals.length-1; j >= 0; j--) {
                int interval = roundingInfo.innerIntervals[j];
                if (normalizedDuration / interval < reduced.getBuckets().size()) {
                    innerIntervalToUse = interval;
                    innerIntervalIndex = j;
                }
            }
        }

        long intervalInMillis = innerIntervalToUse * roundingInfo.getRoughEstimateDurationMillis();
        int bucketCount = getBucketCount(lowest, highest, roundingInfo, intervalInMillis);

        //Next, if our bucketCount is still above what we need, we'll go back and determine the interval
        // based on a size calculation.
        if (bucketCount > reduced.getBuckets().size()) {
            for (int i = innerIntervalIndex; i < roundingInfo.innerIntervals.length; i++) {
                long newIntervalMillis = roundingInfo.innerIntervals[i] * roundingInfo.getRoughEstimateDurationMillis();
                if (getBucketCount(lowest, highest, roundingInfo, newIntervalMillis) <= reduced.getBuckets().size()) {
                    innerIntervalToUse = roundingInfo.innerIntervals[i];
                    intervalInMillis = innerIntervalToUse * roundingInfo.getRoughEstimateDurationMillis();
                }
            }
        }

        Map<Long, Long> expectedCounts = new TreeMap<>();
        for (long keyForBucket = roundingInfo.rounding.round(lowest);
             keyForBucket <= roundingInfo.rounding.round(highest);
             keyForBucket = keyForBucket + intervalInMillis) {
            expectedCounts.put(keyForBucket, 0L);

            // Iterate through the input buckets, and for each bucket, determine if it's inside
            // the range of the bucket in the outer loop. if it is, add the doc count to the total
            // for that bucket.

            for (InternalAutoDateHistogram histogram : inputs) {
                for (Histogram.Bucket bucket : histogram.getBuckets()) {
                    long roundedBucketKey = roundingInfo.rounding.round(((ZonedDateTime) bucket.getKey()).toInstant().toEpochMilli());
                    long docCount = bucket.getDocCount();
                    if (roundedBucketKey >= keyForBucket
                        && roundedBucketKey < keyForBucket + intervalInMillis) {
                        expectedCounts.compute(keyForBucket,
                            (key, oldValue) -> (oldValue == null ? 0 : oldValue) + docCount);
                    }
                }
            }
        }

        // If there is only a single bucket, and we haven't added it above, add a bucket with no documents.
        // this step is necessary because of the roundedBucketKey < keyForBucket + intervalInMillis above.
        if (roundingInfo.rounding.round(lowest) == roundingInfo.rounding.round(highest) && expectedCounts.isEmpty()) {
            expectedCounts.put(roundingInfo.rounding.round(lowest), 0L);
        }


        // pick out the actual reduced values to the make the assertion more readable
        Map<Long, Long> actualCounts = new TreeMap<>();
        for (Histogram.Bucket bucket : reduced.getBuckets()) {
            actualCounts.compute(((ZonedDateTime) bucket.getKey()).toInstant().toEpochMilli(),
                    (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
        }
        assertEquals(expectedCounts, actualCounts);

        DateHistogramInterval expectedInterval;
        if (reduced.getBuckets().size() == 1) {
            expectedInterval = reduced.getInterval();
        } else {
            expectedInterval = new DateHistogramInterval(innerIntervalToUse+roundingInfo.unitAbbreviation);
        }
        assertThat(reduced.getInterval(), equalTo(expectedInterval));
    }

    private int getBucketCount(long lowest, long highest, RoundingInfo roundingInfo, long intervalInMillis) {
        int bucketCount = 0;
        for (long keyForBucket = roundingInfo.rounding.round(lowest);
             keyForBucket <= roundingInfo.rounding.round(highest);
             keyForBucket = keyForBucket + intervalInMillis) {
            bucketCount++;
        }
        return bucketCount;
    }

    @Override
    protected Writeable.Reader<InternalAutoDateHistogram> instanceReader() {
        return InternalAutoDateHistogram::new;
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedAutoDateHistogram.class;
    }

    @Override
    protected InternalAutoDateHistogram mutateInstance(InternalAutoDateHistogram instance) {
        String name = instance.getName();
        List<InternalAutoDateHistogram.Bucket> buckets = instance.getBuckets();
        int targetBuckets = instance.getTargetBuckets();
        BucketInfo bucketInfo = instance.getBucketInfo();
        List<PipelineAggregator> pipelineAggregators = instance.pipelineAggregators();
        Map<String, Object> metaData = instance.getMetaData();
        switch (between(0, 3)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            buckets = new ArrayList<>(buckets);
            buckets.add(new InternalAutoDateHistogram.Bucket(randomNonNegativeLong(), randomIntBetween(1, 100), format,
                    InternalAggregations.EMPTY));
            break;
        case 2:
            int roundingIdx = bucketInfo.roundingIdx == bucketInfo.roundingInfos.length - 1 ? 0 : bucketInfo.roundingIdx + 1;
            bucketInfo = new BucketInfo(bucketInfo.roundingInfos, roundingIdx, bucketInfo.emptySubAggregations);
            break;
        case 3:
            if (metaData == null) {
                metaData = new HashMap<>(1);
            } else {
                metaData = new HashMap<>(instance.getMetaData());
            }
            metaData.put(randomAlphaOfLength(15), randomInt());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalAutoDateHistogram(name, buckets, targetBuckets, bucketInfo, format, pipelineAggregators, metaData, 1);
    }
}
