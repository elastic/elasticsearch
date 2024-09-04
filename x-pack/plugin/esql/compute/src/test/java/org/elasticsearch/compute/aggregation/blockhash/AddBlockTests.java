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

import static org.hamcrest.Matchers.equalTo;

public class AddBlockTests extends ESTestCase {
    private final BlockFactory blockFactory = BlockFactoryTests.blockFactory(ByteSizeValue.ofGb(1));

    public void testSv() {
        TestAddInput result = new TestAddInput();
        List<Added> expected = new ArrayList<>();
        try (AddBlock add = new AddBlock(blockFactory, 3, result)) {
            add.appendOrdSv(0, 0);
            add.appendOrdSv(1, 2);
            add.appendOrdSv(2, 3);
            expected.add(added(0, 0, 2, 3));
            assertThat(result.added, equalTo(expected));
            add.appendOrdSv(3, 4);
            add.emitOrds();
        }
        expected.add(added(3, 4));
        assertThat(result.added, equalTo(expected));
    }

    public void testMvBlockEndsOnBatchBoundary() {
        TestAddInput result = new TestAddInput();
        List<Added> expected = new ArrayList<>();
        try (AddBlock add = new AddBlock(blockFactory, 3, result)) {
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
            add.emitOrds();
        }
        // We uselessly flush an empty position if emitBatchSize lines up with the total count
        expected.add(new Added(1, List.of(List.of())));
        assertThat(result.added, equalTo(expected));
    }

    public void testMvPositionEndOnBatchBoundary() {
        TestAddInput result = new TestAddInput();
        List<Added> expected = new ArrayList<>();
        try (AddBlock add = new AddBlock(blockFactory, 4, result)) {
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
            add.emitOrds();
        }
        // Because the first position ended on a block boundary we uselessly emit an empty position there
        expected.add(new Added(0, List.of(List.of(), List.of(0, 2))));
        assertThat(result.added, equalTo(expected));
    }

    public void testMv() {
        TestAddInput result = new TestAddInput();
        List<Added> expected = new ArrayList<>();
        try (AddBlock add = new AddBlock(blockFactory, 5, result)) {
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
            add.emitOrds();
        }
        expected.add(new Added(1, List.of(List.of(2))));
        assertThat(result.added, equalTo(expected));
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
    }
}
