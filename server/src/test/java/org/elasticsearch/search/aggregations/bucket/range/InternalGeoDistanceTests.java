/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;

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
    protected InternalGeoDistance createTestInstance(String name,
                                                     Map<String, Object> metadata,
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
        return new InternalGeoDistance(name, buckets, keyed, metadata);
    }

    @Override
    protected Class<ParsedGeoDistance> implementationClass() {
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
        Map<String, Object> metadata = instance.getMetadata();
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
            if (metadata == null) {
                metadata = new HashMap<>(1);
            } else {
                metadata = new HashMap<>(instance.getMetadata());
            }
            metadata.put(randomAlphaOfLength(15), randomInt());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalGeoDistance(name, buckets, keyed, metadata);
    }
}
