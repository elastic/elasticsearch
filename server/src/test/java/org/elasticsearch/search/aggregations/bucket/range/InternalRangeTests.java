/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalRangeTests extends InternalRangeTestCase<InternalRange<InternalRange.Bucket, ?>> {

    private DocValueFormat format;
    private List<Tuple<Double, Double>> ranges;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        format = randomNumericDocValueFormat();

        List<Tuple<Double, Double>> listOfRanges = new ArrayList<>();
        if (rarely()) {
            listOfRanges.add(Tuple.tuple(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));
        }
        if (rarely()) {
            listOfRanges.add(Tuple.tuple(Double.NEGATIVE_INFINITY, randomDouble()));
        }
        if (rarely()) {
            listOfRanges.add(Tuple.tuple(randomDouble(), Double.POSITIVE_INFINITY));
        }

        final int interval = randomFrom(1, 5, 10, 25, 50, 100);
        final int numRanges = Math.max(0, randomNumberOfBuckets() - listOfRanges.size());
        final double max = (double) numRanges * interval;

        for (int i = 0; numRanges - listOfRanges.size() > 0; i++) {
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
        ranges = Collections.unmodifiableList(listOfRanges);
    }

    @Override
    protected InternalRange<InternalRange.Bucket, ?> createTestInstance(
        String name,
        Map<String, Object> metadata,
        InternalAggregations aggregations,
        boolean keyed
    ) {
        final List<InternalRange.Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < ranges.size(); ++i) {
            Tuple<Double, Double> range = ranges.get(i);
            int docCount = randomIntBetween(0, 1000);
            double from = range.v1();
            double to = range.v2();
            buckets.add(new InternalRange.Bucket("range_" + i, from, to, docCount, aggregations, format));
        }
        return new InternalRange<>(name, buckets, format, keyed, metadata);
    }

    @Override
    protected Class<? extends InternalMultiBucketAggregation.InternalBucket> internalRangeBucketClass() {
        return InternalRange.Bucket.class;
    }

    @Override
    protected InternalRange<InternalRange.Bucket, ?> mutateInstance(InternalRange<InternalRange.Bucket, ?> instance) {
        String name = instance.getName();
        DocValueFormat format = instance.format;
        boolean keyed = instance.keyed;
        List<InternalRange.Bucket> buckets = instance.getBuckets();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
            case 0 -> name += randomAlphaOfLength(5);
            case 1 -> keyed = keyed == false;
            case 2 -> {
                buckets = new ArrayList<>(buckets);
                double from = randomDouble();
                double to = from + randomDouble();
                buckets.add(new InternalRange.Bucket("range_a", from, to, randomNonNegativeLong(), InternalAggregations.EMPTY, format));
            }
            case 3 -> {
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalRange<>(name, buckets, format, keyed, metadata);
    }
}
