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

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder.RoundingInfo;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram.BucketInfo;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.common.unit.TimeValue.timeValueHours;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

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

        roundingInfos = AutoDateHistogramAggregationBuilder.buildRoundings(null);
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


        return new InternalAutoDateHistogram(name, buckets, targetBuckets, bucketInfo, format, pipelineAggregators, metaData);
    }

    @Override
    protected void assertReduced(InternalAutoDateHistogram reduced, List<InternalAutoDateHistogram> inputs) {
        int roundingIdx = 0;
        for (InternalAutoDateHistogram histogram : inputs) {
            if (histogram.getBucketInfo().roundingIdx > roundingIdx) {
                roundingIdx = histogram.getBucketInfo().roundingIdx;
            }
        }
        RoundingInfo roundingInfo = roundingInfos[roundingIdx];

        long lowest = Long.MAX_VALUE;
        long highest = 0;
        for (InternalAutoDateHistogram histogram : inputs) {
            for (Histogram.Bucket bucket : histogram.getBuckets()) {
                long bucketKey = ((DateTime) bucket.getKey()).getMillis();
                if (bucketKey < lowest) {
                    lowest = bucketKey;
                }
                if (bucketKey > highest) {
                    highest = bucketKey;
                }
            }
        }
        long normalizedDuration = (highest - lowest) / roundingInfo.getRoughEstimateDurationMillis();
        long innerIntervalToUse = 0;
        for (int interval : roundingInfo.innerIntervals) {
            if (normalizedDuration / interval < maxNumberOfBuckets()) {
                innerIntervalToUse = interval;
            }
        }
        Map<Long, Long> expectedCounts = new TreeMap<>();
        long intervalInMillis = innerIntervalToUse*roundingInfo.getRoughEstimateDurationMillis();
        for (long keyForBucket = roundingInfo.rounding.round(lowest);
             keyForBucket <= highest;
             keyForBucket = keyForBucket + intervalInMillis) {
            expectedCounts.put(keyForBucket, 0L);

            for (InternalAutoDateHistogram histogram : inputs) {
                for (Histogram.Bucket bucket : histogram.getBuckets()) {
                    long bucketKey = ((DateTime) bucket.getKey()).getMillis();
                    long roundedBucketKey = roundingInfo.rounding.round(bucketKey);
                    if (roundedBucketKey >= keyForBucket
                        && roundedBucketKey < keyForBucket + intervalInMillis) {
                        long count = bucket.getDocCount();
                        expectedCounts.compute(keyForBucket,
                            (key, oldValue) -> (oldValue == null ? 0 : oldValue) + count);
                    }
                }
            }
        }


        Map<Long, Long> actualCounts = new TreeMap<>();
        for (Histogram.Bucket bucket : reduced.getBuckets()) {
            actualCounts.compute(((DateTime) bucket.getKey()).getMillis(),
                    (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
        }
        assertEquals(expectedCounts, actualCounts);
    }

    @Override
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/32215")
    public void testReduceRandom() {
        super.testReduceRandom();
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
        return new InternalAutoDateHistogram(name, buckets, targetBuckets, bucketInfo, format, pipelineAggregators, metaData);
    }
}
