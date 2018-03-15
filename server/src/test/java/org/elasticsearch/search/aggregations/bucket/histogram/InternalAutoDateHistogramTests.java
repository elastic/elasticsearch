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
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.Rounding;
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

        roundingInfos = new RoundingInfo[6];
        roundingInfos[0] = new RoundingInfo(Rounding.builder(DateTimeUnit.SECOND_OF_MINUTE).build(), 1, 5, 10, 30);
        roundingInfos[1] = new RoundingInfo(Rounding.builder(DateTimeUnit.MINUTES_OF_HOUR).build(), 1, 5, 10, 30);
        roundingInfos[2] = new RoundingInfo(Rounding.builder(DateTimeUnit.HOUR_OF_DAY).build(), 1, 3, 12);
        roundingInfos[3] = new RoundingInfo(Rounding.builder(DateTimeUnit.DAY_OF_MONTH).build(), 1, 7);
        roundingInfos[4] = new RoundingInfo(Rounding.builder(DateTimeUnit.MONTH_OF_YEAR).build(), 1, 3);
        roundingInfos[5] = new RoundingInfo(Rounding.builder(DateTimeUnit.YEAR_OF_CENTURY).build(), 1, 10, 20, 50, 100);
    }

    @Override
    protected InternalAutoDateHistogram createTestInstance(String name,
                                                       List<PipelineAggregator> pipelineAggregators,
                                                       Map<String, Object> metaData,
                                                       InternalAggregations aggregations) {
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
        Map<Long, Long> expectedCounts = new TreeMap<>();
        for (Histogram histogram : inputs) {
            for (Histogram.Bucket bucket : histogram.getBuckets()) {
                expectedCounts.compute(roundingInfos[roundingIdx].rounding.round(((DateTime) bucket.getKey()).getMillis()),
                        (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
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
