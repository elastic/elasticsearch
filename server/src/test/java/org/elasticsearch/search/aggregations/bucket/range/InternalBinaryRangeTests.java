/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.range;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalBinaryRangeTests extends InternalRangeTestCase<InternalBinaryRange> {

    private List<Tuple<BytesRef, BytesRef>> ranges;

    @Override
    protected int minNumberOfBuckets() {
        return 1;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        List<Tuple<BytesRef, BytesRef>> listOfRanges = new ArrayList<>();
        if (randomBoolean()) {
            listOfRanges.add(Tuple.tuple(null, new BytesRef(randomAlphaOfLength(15))));
        }
        if (randomBoolean()) {
            listOfRanges.add(Tuple.tuple(new BytesRef(randomAlphaOfLength(15)), null));
        }
        if (randomBoolean()) {
            listOfRanges.add(Tuple.tuple(null, null));
        }

        final int numRanges = Math.max(0, randomNumberOfBuckets() - listOfRanges.size());
        for (int i = 0; i < numRanges; i++) {
            BytesRef[] values = new BytesRef[2];
            values[0] = new BytesRef(randomAlphaOfLength(15));
            values[1] = new BytesRef(randomAlphaOfLength(15));
            Arrays.sort(values);
            listOfRanges.add(Tuple.tuple(values[0], values[1]));
        }
        Collections.shuffle(listOfRanges, random());
        ranges = Collections.unmodifiableList(listOfRanges);
    }

    @Override
    protected InternalBinaryRange createTestInstance(
        String name,
        Map<String, Object> metadata,
        InternalAggregations aggregations,
        boolean keyed
    ) {
        DocValueFormat format = DocValueFormat.RAW;
        List<InternalBinaryRange.Bucket> buckets = new ArrayList<>();

        int nullKey = randomBoolean() ? randomIntBetween(0, ranges.size() - 1) : -1;
        for (int i = 0; i < ranges.size(); ++i) {
            final int docCount = randomIntBetween(1, 100);
            final String key = (i == nullKey) ? null : randomAlphaOfLength(10);
            buckets.add(new InternalBinaryRange.Bucket(format, keyed, key, ranges.get(i).v1(), ranges.get(i).v2(), docCount, aggregations));
        }
        return new InternalBinaryRange(name, format, keyed, buckets, metadata);
    }

    @Override
    protected void assertReduced(InternalBinaryRange reduced, List<InternalBinaryRange> inputs) {
        int pos = 0;
        for (InternalBinaryRange input : inputs) {
            assertEquals(reduced.getBuckets().size(), input.getBuckets().size());
        }
        for (Range.Bucket bucket : reduced.getBuckets()) {
            int expectedCount = 0;
            for (InternalBinaryRange input : inputs) {
                expectedCount += (int) input.getBuckets().get(pos).getDocCount();
            }
            assertEquals(expectedCount, bucket.getDocCount());
            pos++;
        }
    }

    @Override
    protected Class<? extends InternalMultiBucketAggregation.InternalBucket> internalRangeBucketClass() {
        return InternalBinaryRange.Bucket.class;
    }

    @Override
    protected InternalBinaryRange mutateInstance(InternalBinaryRange instance) {
        String name = instance.getName();
        DocValueFormat format = instance.format;
        boolean keyed = instance.keyed;
        List<InternalBinaryRange.Bucket> buckets = instance.getBuckets();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
            case 0 -> name += randomAlphaOfLength(5);
            case 1 -> keyed = keyed == false;
            case 2 -> {
                buckets = new ArrayList<>(buckets);
                buckets.add(
                    new InternalBinaryRange.Bucket(
                        format,
                        keyed,
                        "range_a",
                        new BytesRef(randomAlphaOfLengthBetween(1, 20)),
                        new BytesRef(randomAlphaOfLengthBetween(1, 20)),
                        randomNonNegativeLong(),
                        InternalAggregations.EMPTY
                    )
                );
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
        return new InternalBinaryRange(name, format, keyed, buckets, metadata);
    }

    /**
     * Checks the invariant that bucket keys are always non-null, even if null keys
     * were originally provided.
     */
    public void testKeyGeneration() {
        InternalBinaryRange range = createTestInstance();
        for (InternalBinaryRange.Bucket bucket : range.getBuckets()) {
            assertNotNull(bucket.getKey());
        }
    }
}
