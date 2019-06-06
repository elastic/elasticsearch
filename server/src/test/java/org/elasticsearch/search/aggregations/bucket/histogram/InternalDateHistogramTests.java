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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.common.unit.TimeValue.timeValueHours;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

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
        format = randomNumericDocValueFormat();
        //in order for reduction to work properly (and be realistic) we need to use the same interval, minDocCount, emptyBucketInfo
        //and base in all randomly created aggs as part of the same test run. This is particularly important when minDocCount is
        //set to 0 as empty buckets need to be added to fill the holes.
        long interval = randomIntBetween(1, 3);
        intervalMillis = randomFrom(timeValueSeconds(interval), timeValueMinutes(interval), timeValueHours(interval)).getMillis();
        Rounding rounding = Rounding.builder(TimeValue.timeValueMillis(intervalMillis)).build();
        baseMillis = rounding.round(System.currentTimeMillis());
        if (randomBoolean()) {
            minDocCount = randomIntBetween(1, 10);
            emptyBucketInfo = null;
        } else {
            minDocCount = 0;
            ExtendedBounds extendedBounds = null;
            if (randomBoolean()) {
                //it's ok if min and max are outside the range of the generated buckets, that will just mean that
                //empty buckets won't be added before the first bucket and/or after the last one
                long min = baseMillis - intervalMillis * randomNumberOfBuckets();
                long max = baseMillis + randomNumberOfBuckets() * intervalMillis;
                extendedBounds = new ExtendedBounds(min, max);
            }
            emptyBucketInfo = new InternalDateHistogram.EmptyBucketInfo(rounding, InternalAggregations.EMPTY, extendedBounds);
        }
    }

    @Override
    protected InternalDateHistogram createTestInstance(String name,
                                                       List<PipelineAggregator> pipelineAggregators,
                                                       Map<String, Object> metaData,
                                                       InternalAggregations aggregations) {
        int nbBuckets = randomNumberOfBuckets();
        List<InternalDateHistogram.Bucket> buckets = new ArrayList<>(nbBuckets);
        //avoid having different random instance start from exactly the same base
        long startingDate = baseMillis - intervalMillis * randomNumberOfBuckets();
        for (int i = 0; i < nbBuckets; i++) {
            //rarely leave some holes to be filled up with empty buckets in case minDocCount is set to 0
            if (frequently()) {
                long key = startingDate + intervalMillis * i;
                buckets.add(new InternalDateHistogram.Bucket(key, randomIntBetween(1, 100), keyed, format, aggregations));
            }
        }
        BucketOrder order = BucketOrder.key(randomBoolean());
        return new InternalDateHistogram(name, buckets, order, minDocCount, 0L, emptyBucketInfo, format, keyed,
            pipelineAggregators, metaData);
    }

    @Override
    protected void assertReduced(InternalDateHistogram reduced, List<InternalDateHistogram> inputs) {
        TreeMap<Long, Long> expectedCounts = new TreeMap<>();
        for (Histogram histogram : inputs) {
            for (Histogram.Bucket bucket : histogram.getBuckets()) {
                expectedCounts.compute(((ZonedDateTime) bucket.getKey()).toInstant().toEpochMilli(),
                        (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
            }
        }
        if (minDocCount == 0) {
            long minBound = -1;
            long maxBound = -1;
            if (emptyBucketInfo.bounds != null) {
                minBound = emptyBucketInfo.rounding.round(emptyBucketInfo.bounds.getMin());
                maxBound = emptyBucketInfo.rounding.round(emptyBucketInfo.bounds.getMax());
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
            actualCounts.compute(((ZonedDateTime) bucket.getKey()).toInstant().toEpochMilli(),
                    (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
        }
        assertEquals(expectedCounts, actualCounts);
    }

    @Override
    protected Writeable.Reader<InternalDateHistogram> instanceReader() {
        return InternalDateHistogram::new;
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedDateHistogram.class;
    }

    @Override
    protected InternalDateHistogram mutateInstance(InternalDateHistogram instance) {
        String name = instance.getName();
        List<InternalDateHistogram.Bucket> buckets = instance.getBuckets();
        BucketOrder order = instance.getOrder();
        long minDocCount = instance.getMinDocCount();
        long offset = instance.getOffset();
        List<PipelineAggregator> pipelineAggregators = instance.pipelineAggregators();
        InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = instance.emptyBucketInfo;
        Map<String, Object> metaData = instance.getMetaData();
        switch (between(0, 5)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            buckets = new ArrayList<>(buckets);
            buckets.add(new InternalDateHistogram.Bucket(randomNonNegativeLong(), randomIntBetween(1, 100), keyed, format,
                    InternalAggregations.EMPTY));
            break;
        case 2:
            order = BucketOrder.count(randomBoolean());
            break;
        case 3:
            minDocCount += between(1, 10);
            emptyBucketInfo = null;
            break;
        case 4:
            offset += between(1, 20);
            break;
        case 5:
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
        return new InternalDateHistogram(name, buckets, order, minDocCount, offset, emptyBucketInfo, format, keyed, pipelineAggregators,
                metaData);
    }
}
