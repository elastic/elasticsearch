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

package org.elasticsearch.search.aggregations.bucket.range.geodistance;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.range.InternalRangeTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InternalGeoDistanceTests extends InternalRangeTestCase<InternalGeoDistance> {

    private List<Tuple<Double, Double>> geoDistanceRanges;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        final int interval = randomFrom(1, 5, 10, 25, 50, 100);
        final int numRanges = randomIntBetween(1, 10);

        List<Tuple<Double, Double>> listOfRanges = new ArrayList<>(numRanges);
        for (int i = 0; i < numRanges; i++) {
            double from = i * interval;
            double to = from + interval;
            listOfRanges.add(Tuple.tuple(from, to));
        }
        if (randomBoolean()) {
            // Add some overlapping ranges
            double max = (double) numRanges * interval;
            listOfRanges.add(Tuple.tuple(0.0, max));
            listOfRanges.add(Tuple.tuple(0.0, max / 2));
            listOfRanges.add(Tuple.tuple(max / 3, max / 3 * 2));
        }
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
                                                     boolean keyed) {
        final List<InternalGeoDistance.Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < geoDistanceRanges.size(); ++i) {
            Tuple<Double, Double> range = geoDistanceRanges.get(i);
            int docCount = randomIntBetween(0, 1000);
            double from = range.v1();
            double to = range.v2();
            buckets.add(new InternalGeoDistance.Bucket("range_" + i, from, to, docCount, InternalAggregations.EMPTY, keyed));
        }
        return new InternalGeoDistance(name, buckets, keyed, pipelineAggregators, metaData);
    }
}
