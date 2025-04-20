/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockFactoryTests;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;

public class AddPageTests extends ESTestCase {
    private final BlockFactory blockFactory = BlockFactoryTests.blockFactory(ByteSizeValue.ofGb(1));

    public void testSv() {
        TestAddInput result = new TestAddInput();
        List<Added> expected = new ArrayList<>();
        try (AddPage add = new AddPage(blockFactory, 3, result)) {
            add.appendOrdSv(0, 0);
            add.appendOrdSv(1, 2);
            add.appendOrdSv(2, 3);
            expected.add(added(0, 0, 2, 3));
            assertThat(result.added, equalTo(expected));
            add.appendOrdSv(3, 4);
            add.flushRemaining();
            assertThat(add.added(), equalTo(4L));
        }
        expected.add(added(3, 4));
        assertThat(result.added, equalTo(expected));
    }

    public void testMvBlockEndsOnBatchBoundary() {
        TestAddInput result = new TestAddInput();
        List<Added> expected = new ArrayList<>();
        try (AddPage add = new AddPage(blockFactory, 3, result)) {
            add.appendOrdInMv(0, 0);
            add.appendOrdInMv(0, 2);
            add.appendOrdInMv(0, 3);
            expected.add(new Added(0, List.of(List.of(0, 2, 3))));
            assertThat(result.added, equalTo(expected));
            add.appendOrdInMv(0, 4);
            add.finishMv();
            add.appendOrdInMv(1, 0);
            add.appendOrdInMv(1, 2);
            expected.add(new Added(0, List.of(List.of(4), List.of(0, 2))));
            assertThat(result.added, equalTo(expected));
            add.finishMv();
            add.flushRemaining();
            assertThat(add.added(), equalTo(6L));
        }
        /*
         * We do *not* uselessly flush an empty Block of ordinals. Doing so would
         * be a slight performance hit, but, worse, makes testing harder to reason
         * about.
         */
        assertThat(result.added, equalTo(expected));
    }

    public void testMvPositionEndOnBatchBoundary() {
        TestAddInput result = new TestAddInput();
        List<Added> expected = new ArrayList<>();
        try (AddPage add = new AddPage(blockFactory, 4, result)) {
            add.appendOrdInMv(0, 0);
            add.appendOrdInMv(0, 2);
            add.appendOrdInMv(0, 3);
            add.appendOrdInMv(0, 4);
            expected.add(new Added(0, List.of(List.of(0, 2, 3, 4))));
            assertThat(result.added, equalTo(expected));
            add.finishMv();
            add.appendOrdInMv(1, 0);
            add.appendOrdInMv(1, 2);
            add.finishMv();
            add.flushRemaining();
            assertThat(add.added(), equalTo(6L));
        }
        // Because the first position ended on a block boundary we uselessly emit an empty position there
        expected.add(new Added(0, List.of(List.of(), List.of(0, 2))));
        assertThat(result.added, equalTo(expected));
    }

    public void testMv() {
        TestAddInput result = new TestAddInput();
        List<Added> expected = new ArrayList<>();
        try (AddPage add = new AddPage(blockFactory, 5, result)) {
            add.appendOrdInMv(0, 0);
            add.appendOrdInMv(0, 2);
            add.appendOrdInMv(0, 3);
            add.appendOrdInMv(0, 4);
            add.finishMv();
            add.appendOrdInMv(1, 0);
            expected.add(new Added(0, List.of(List.of(0, 2, 3, 4), List.of(0))));
            assertThat(result.added, equalTo(expected));
            add.appendOrdInMv(1, 2);
            add.finishMv();
            add.flushRemaining();
            assertThat(add.added(), equalTo(6L));
        }
        expected.add(new Added(1, List.of(List.of(2))));
        assertThat(result.added, equalTo(expected));
    }

    /**
     * Test that we can add more than {@link Integer#MAX_VALUE} values. That's
     * more than two billion values. We've made the call as fast as we can.
     * Locally this test takes about 40 seconds for Nik.
     */
    public void testMvBillions() {
        CountingAddInput counter = new CountingAddInput();
        try (AddPage add = new AddPage(blockFactory, 5, counter)) {
            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                add.appendOrdInMv(0, 0);
                assertThat(add.added(), equalTo((long) i + 1));
                if (i % 5 == 0) {
                    assertThat(counter.count, equalTo(i / 5));
                }
                if (i % 10_000_000 == 0) {
                    logger.info(String.format(Locale.ROOT, "Progress: %02.0f%%", 100 * ((double) i / Integer.MAX_VALUE)));
                }
            }
            add.finishMv();
            add.appendOrdInMv(1, 0);
            assertThat(add.added(), equalTo(Integer.MAX_VALUE + 1L));
            add.appendOrdInMv(1, 0);
            assertThat(add.added(), equalTo(Integer.MAX_VALUE + 2L));
            add.finishMv();
            add.flushRemaining();
            assertThat(counter.count, equalTo(Integer.MAX_VALUE / 5 + 1));
        }
    }

    @After
    public void breakerClear() {
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    record Added(int positionOffset, List<List<Integer>> ords) {}

    Added added(int positionOffset, int... ords) {
        return new Added(positionOffset, Arrays.stream(ords).mapToObj(List::of).toList());
    }

    private class TestAddInput implements GroupingAggregatorFunction.AddInput {
        private final List<Added> added = new ArrayList<>();

        @Override
        public void add(int positionOffset, IntBlock groupIds) {
            List<List<Integer>> result = new ArrayList<>(groupIds.getPositionCount());
            for (int p = 0; p < groupIds.getPositionCount(); p++) {
                int valueCount = groupIds.getValueCount(p);
                List<Integer> r = new ArrayList<>(valueCount);
                result.add(r);
                int start = groupIds.getFirstValueIndex(p);
                int end = valueCount + start;
                for (int i = start; i < end; i++) {
                    r.add(groupIds.getInt(i));
                }
            }
            added.add(new Added(positionOffset, result));
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
            add(positionOffset, groupIds.asBlock());
        }

        @Override
        public void close() {
            fail("shouldn't close");
        }
    }

    private class CountingAddInput implements GroupingAggregatorFunction.AddInput {
        private int count;

        @Override
        public void add(int positionOffset, IntBlock groupIds) {
            count++;
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
            count++;
        }

        @Override
        public void close() {}
    }
}
