/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesRefViewScratch;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

public class HllStatesTests extends ESTestCase {
    private static final BigArrays BIG_ARRAYS = BigArrays.NON_RECYCLING_INSTANCE;
    private static final int PRECISION_THRESHOLD = 200;
    private static final int VALUE_COUNT = 2000;
    private static final int GROUPS = 3;
    private static final int VALUES_PER_GROUP = 500;
    private static final int EXTRA_VALUE_MOD = 7;
    private static final BytesRef[] EXTRA_BYTES = new BytesRef[] { new BytesRef("foo"), new BytesRef("bar") };

    public void testSingleStateSerializedMergeRoundTrip() {
        try (
            var source = new HllStates.SingleState(BIG_ARRAYS, PRECISION_THRESHOLD);
            var target = new HllStates.SingleState(BIG_ARRAYS, PRECISION_THRESHOLD)
        ) {
            collectSingleState(source);
            BytesStreamOutput scratch = new BytesStreamOutput();
            BytesRefViewScratch scratchBytes = new BytesRefViewScratch();
            BytesRef serialized = serializeCopy(0, source.hll, scratch, scratchBytes);

            target.merge(0, serialized, 0);

            assertTrue(source.hll.equals(0, target.hll, 0));
            assertEquals(source.cardinality(), target.cardinality());
        }
    }

    public void testGroupingStateSerializedMergeRoundTrip() {
        try (
            var source = new HllStates.GroupingState(BIG_ARRAYS, PRECISION_THRESHOLD);
            var target = new HllStates.GroupingState(BIG_ARRAYS, PRECISION_THRESHOLD)
        ) {
            collectGroupingState(source);
            BytesStreamOutput scratch = new BytesStreamOutput();
            BytesRefViewScratch scratchBytes = new BytesRefViewScratch();
            List<BytesRef> serialized = new ArrayList<>();
            for (int group = 0; group < GROUPS; group++) {
                serialized.add(serializeCopy(group, source.hll, scratch, scratchBytes));
            }

            for (int group = 0; group < GROUPS; group++) {
                target.merge(group, serialized.get(group), 0);
            }

            for (int group = 0; group < GROUPS; group++) {
                assertTrue(source.hll.equals(group, target.hll, group));
                assertEquals(source.cardinality(group), target.cardinality(group));
            }
        }
    }

    private static void collectSingleState(HllStates.SingleState state) {
        for (int i = 0; i < VALUE_COUNT; i++) {
            state.collect(i);
            if (i % EXTRA_VALUE_MOD == 0) {
                state.collect((double) i / 3);
            }
        }
        for (BytesRef value : EXTRA_BYTES) {
            state.collect(value);
        }
    }

    private static void collectGroupingState(HllStates.GroupingState state) {
        for (int group = 0; group < GROUPS; group++) {
            for (int i = 0; i < VALUES_PER_GROUP; i++) {
                state.collect(group, group * 10_000L + i);
            }
        }
    }

    private static BytesRef serializeCopy(
        int groupId,
        HyperLogLogPlusPlus hll,
        BytesStreamOutput scratch,
        BytesRefViewScratch scratchBytes
    ) {
        // Simulate block builder behavior by copying the BytesRef.
        return BytesRef.deepCopyOf(HllStates.serializeHLL(groupId, hll, scratch, scratchBytes));
    }
}
