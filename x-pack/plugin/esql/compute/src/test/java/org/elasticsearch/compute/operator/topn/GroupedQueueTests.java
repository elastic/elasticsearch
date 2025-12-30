/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class GroupedQueueTests extends ESTestCase {
    /** Maximum allowed difference between expected and actual ramBytesUsed() values. This is caused by the HashMap load factor. */
    public static final long MAX_DIFF = 32;

    private final BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(1));
    private final CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
    private final BlockFactory blockFactory = new BlockFactory(breaker, bigArrays);

    @After
    public void allMemoryReleased() throws Exception {
        MockBigArrays.ensureAllArraysAreReleased();

        assertThat("Not all memory was released", breaker.getUsed(), equalTo(0L));
        assertThat("Not all blocks were released", blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testCleanup() {
        int topCount = 5;
        try (GroupedQueue queue = GroupedQueue.build(breaker, topCount)) {
            assertThat(queue.size(), equalTo(0));

            for (int i = 0; i < topCount * 2; i++) {
                addRow(queue, i % 3, i * 10);
            }
        }
    }

    public void testAddWhenHeapNotFull() {
        int topCount = 5;
        try (GroupedQueue queue = GroupedQueue.build(breaker, topCount)) {
            for (int i = 0; i < topCount; i++) {
                Row row = createRow(breaker, i % 2, i * 10);
                Row result = queue.add(row);
                assertThat(result, nullValue());
                assertThat(queue.size(), equalTo(i + 1));
            }
        }
    }

    public void testAddWhenHeapFullAndRowQualifies() {
        int topCount = 3;
        try (GroupedQueue queue = GroupedQueue.build(breaker, topCount)) {
            fillQueueToCapacity(queue, topCount);

            Row topBefore = queue.pop();
            assertThat(topBefore, notNullValue());
            Row result = queue.add(topBefore);
            assertThat(result, nullValue());

            try (Row evicted = queue.add(createRow(breaker, 0, 5))) {
                assertThat(extractIntValue(evicted), equalTo(20));
            }
        }
    }

    public void testAddWhenHeapFullAndRowDoesNotQualify() {
        try (GroupedQueue queue = GroupedQueue.build(breaker, 3)) {
            addRows(queue, 0, 30, 40, 50);

            try (Row row = createRow(breaker, 0, 60)) {
                Row result = queue.add(row);
                assertThat(result, sameInstance(row));
            }
        }
    }

    public void testAddWithDifferentGroupKeys() {
        try (GroupedQueue queue = GroupedQueue.build(breaker, 2)) {
            assertThat(queue.add(createRow(breaker, 0, 10)), nullValue());
            assertThat(queue.add(createRow(breaker, 1, 20)), nullValue());
            assertThat(queue.add(createRow(breaker, 0, 30)), nullValue());
            assertThat(queue.add(createRow(breaker, 1, 40)), nullValue());
            assertThat(queue.size(), equalTo(4));

            try (Row evicted = queue.add(createRow(breaker, 0, 5))) {
                assertThat(evicted, notNullValue());
                assertThat(extractIntValue(evicted), equalTo(30));
            }
            try (Row evicted = queue.add(createRow(breaker, 1, 15))) {
                assertThat(evicted, notNullValue());
                assertThat(extractIntValue(evicted), equalTo(40));
            }
            assertThat(queue.size(), equalTo(4));

            try (Row row = queue.add(createRow(breaker, 0, 50))) {
                assertThat(row, notNullValue());
                assertThat(extractIntValue(row), equalTo(50));
            }
            try (Row row = queue.add(createRow(breaker, 1, 50))) {
                assertThat(row, notNullValue());
                assertThat(extractIntValue(row), equalTo(50));
            }
            assertThat(queue.size(), equalTo(4));
        }
    }

    public void testRamBytesUsedEmpty() {
        try (GroupedQueue queue = GroupedQueue.build(breaker, 5)) {
            assertRamUsageClose(queue);
        }
    }

    private void assertRamUsageClose(GroupedQueue queue) {
        long actual = queue.ramBytesUsed();
        long expected = expectedRamBytesUsed(queue);
        var msg = Strings.format("Expected a difference of at most %d bytes; reported: %d, RamUsageTester: %d", MAX_DIFF, actual, expected);
        assertThat(msg, Math.abs(actual - expected), lessThanOrEqualTo(MAX_DIFF));
    }

    public void testRamBytesUsedPartiallyFilled() {
        try (GroupedQueue queue = GroupedQueue.build(breaker, 5)) {
            addRows(queue, 0, 10, 20, 30);
            // addRows(queue, 1, 10, 20);
            assertRamUsageClose(queue);
        }
    }

    public void testRamBytesUsedAtCapacity() {
        try (GroupedQueue queue = GroupedQueue.build(breaker, 5)) {
            addRows(queue, 0, 10, 20, 30, 40, 50);
            addRows(queue, 1, 10, 20, 30, 40, 50);
            addRows(queue, 2, 10, 20, 30, 40, 50);
            assertRamUsageClose(queue);
        }
    }

    private Row createRow(CircuitBreaker breaker, int groupKey, int sortKey) {
        try (
            IntBlock groupKeyBlock = blockFactory.newIntBlockBuilder(1).appendInt(groupKey).build();
            IntBlock keyBlock = blockFactory.newIntBlockBuilder(1).appendInt(sortKey).build();
            IntBlock valueBlock = blockFactory.newIntBlockBuilder(1).appendInt(sortKey * 2).build()
        ) {
            Row row = new GroupedRow(new UngroupedRow(breaker, List.of(SORT_ORDER), 32, 64), 0);
            var filler = new GroupedRowFiller(
                List.of(ElementType.INT, ElementType.INT, ElementType.INT),
                List.of(TopNEncoder.DEFAULT_SORTABLE, TopNEncoder.DEFAULT_SORTABLE, TopNEncoder.DEFAULT_UNSORTABLE),
                List.of(SORT_ORDER),
                List.of(0),
                new Page(groupKeyBlock, keyBlock, valueBlock)
            );
            filler.writeKey(0, row);
            filler.writeValues(0, row);
            return row;
        }
    }

    private static int extractIntValue(Row row) {
        BytesRef keys = row.keys().bytesRefView();
        return TopNEncoder.DEFAULT_SORTABLE.decodeInt(new BytesRef(keys.bytes, keys.offset + 1, keys.length - 1));
    }

    private void addRow(GroupedQueue queue, int groupKey, int value) {
        Row row = createRow(breaker, groupKey, value);
        // This row is either the input or the evicted row, but either way it should be closed.
        Releasables.close(queue.add(row));
    }

    private void fillQueueToCapacity(GroupedQueue queue, int capacity) {
        addRows(queue, 0, IntStream.range(0, capacity).map(i -> i * 10).toArray());
    }

    private void addRows(GroupedQueue queue, int groupKey, int... values) {
        for (int value : values) {
            addRow(queue, groupKey, value);
        }
    }

    private static final TopNOperator.SortOrder SORT_ORDER = new TopNOperator.SortOrder(1, true, false);

    private long expectedRamBytesUsed(GroupedQueue queue) {
        long expected = RamUsageTester.ramUsed(queue);
        expected -= RamUsageTester.ramUsed(breaker);
        // RamUsageTester disagrees with the RamUsageEstimator on how much RAM an empty HashMap uses.
        expected -= RamUsageTester.ramUsed(new HashMap<BytesRef, UngroupedQueue>());
        expected += RamUsageEstimator.shallowSizeOfInstance(HashMap.class);
        if (queue.size() > 0) {
            var size = queue.size();
            Row rowSample = queue.pop();
            // FIXME(gal, NOCOMMIT) Reduce code duplication with UngroupedQueueTests.expectedRamBytesUsed
            expected -= size * (RamUsageTester.ramUsed(rowSample) - rowSample.ramBytesUsed());
            expected += size * RamUsageTester.ramUsed(breaker);
            expected += (size - 1) * (RamUsageTester.ramUsed(SORT_ORDER) + RamUsageTester.ramUsed("topn"));
            queue.add(rowSample);
        }
        return expected;
    }

    private static void assertSameSize(Row sample, GroupedQueue queue) {
        while (queue.size() > 0) {
            try (Row row = queue.pop()) {
                assertThat(row.ramBytesUsed(), equalTo(sample.ramBytesUsed()));
                assertThat(RamUsageTester.ramUsed(row), equalTo(RamUsageTester.ramUsed(sample)));
            }
        }
    }
}
