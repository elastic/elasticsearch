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

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalGeoDistanceTests extends InternalRangeTestCase<InternalGeoDistance> {

    private List<Tuple<Double, Double>> geoDistanceRanges;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        final int interval = randomFrom(1, 5, 10, 25, 50, 100);
        final int numRanges = randomNumberOfBuckets();
        final double max = (double) numRanges * interval;

        List<Tuple<Double, Double>> listOfRanges = new ArrayList<>(numRanges);
        for (int i = 0; i < numRanges; i++) {
            double from = i * interval;
            double to = from + interval;

            Tuple<Double, Double> range;
            if (randomBoolean()) {
                range = Tuple.tuple(from, to);
            } else {
                // Add some overlapping range
                range = Tuple.tuple(randomFrom(0.0, max / 3), randomFrom(max, max / 2, max / 3 * 2));
            }
            listOfRanges.add(range);
        }
        Collections.shuffle(listOfRanges, random());
        geoDistanceRanges = Collections.unmodifiableList(listOfRanges);
    }

    @Override
    protected Writeable.Reader<InternalGeoDistance> instanceReader() {
        return InternalGeoDistance::new;
    }

    @Override
    protected InternalGeoDistance createTestInstance(String name,
                                                     List<PipelineAggregator> pipelineAggregators,
                                                     Map<String, Object> metaData,
                                                     InternalAggregations aggregations,
                                                     boolean keyed) {
        final List<InternalGeoDistance.Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < geoDistanceRanges.size(); ++i) {
            Tuple<Double, Double> range = geoDistanceRanges.get(i);
            int docCount = randomIntBetween(0, 1000);
            double from = range.v1();
            double to = range.v2();
            buckets.add(new InternalGeoDistance.Bucket("range_" + i, from, to, docCount, aggregations, keyed));
        }
        return new InternalGeoDistance(name, buckets, keyed, pipelineAggregators, metaData);
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedGeoDistance.class;
    }

    @Override
    protected Class<? extends InternalMultiBucketAggregation.InternalBucket> internalRangeBucketClass() {
        return InternalGeoDistance.Bucket.class;
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation.ParsedBucket> parsedRangeBucketClass() {
        return ParsedGeoDistance.ParsedBucket.class;
    }

    @Override
    protected InternalGeoDistance mutateInstance(InternalGeoDistance instance) {
        String name = instance.getName();
        boolean keyed = instance.keyed;
        List<InternalGeoDistance.Bucket> buckets = instance.getBuckets();
        List<PipelineAggregator> pipelineAggregators = instance.pipelineAggregators();
        Map<String, Object> metaData = instance.getMetaData();
        switch (between(0, 3)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            keyed = keyed == false;
            break;
        case 2:
            buckets = new ArrayList<>(buckets);
            double from = randomDouble();
            buckets.add(new InternalGeoDistance.Bucket("range_a", from, from + randomDouble(), randomNonNegativeLong(),
                    InternalAggregations.EMPTY, false));
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
        return new InternalGeoDistance(name, buckets, keyed, pipelineAggregators, metaData);
    }
}
