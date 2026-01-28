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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.operator.topn.GroupedQueue.HASH_MAP_NODE_SIZE;
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
        try (GroupedQueue queue = new GroupedQueue(breaker, topCount)) {
            assertThat(queue.size(), equalTo(0));

            for (int i = 0; i < topCount * 2; i++) {
                addRow(queue, i % 3, i * 10);
            }
        }
    }

    public void testAddWhenHeapNotFull() {
        int topCount = 5;
        try (GroupedQueue queue = new GroupedQueue(breaker, topCount)) {
            for (int i = 0; i < topCount; i++) {
                Row row = createRow(breaker, i % 2, i * 10);
                Row result = queue.addRow(row);
                assertThat(result, nullValue());
                assertThat(queue.size(), equalTo(i + 1));
            }
        }
    }

    public void testAddWhenHeapFullAndRowQualifies() {
        int topCount = 3;
        try (GroupedQueue queue = new GroupedQueue(breaker, topCount)) {
            fillQueueToCapacity(queue, topCount);

            try (Row evicted = queue.addRow(createRow(breaker, 0, 5))) {
                assertRowValues(evicted, 0, 20, 40);
            }
        }
    }

    public void testAddWhenHeapFullAndRowDoesNotQualify() {
        try (GroupedQueue queue = new GroupedQueue(breaker, 3)) {
            addRows(queue, 0, 30, 40, 50);

            try (Row row = createRow(breaker, 0, 60)) {
                Row result = queue.addRow(row);
                assertThat(result, sameInstance(row));
            }
        }
    }

    public void testAddWithDifferentGroupKeys() {
        try (GroupedQueue queue = new GroupedQueue(breaker, 2)) {
            assertThat(queue.addRow(createRow(breaker, 0, 10)), nullValue());
            assertThat(queue.addRow(createRow(breaker, 1, 20)), nullValue());
            assertThat(queue.addRow(createRow(breaker, 0, 30)), nullValue());
            assertThat(queue.addRow(createRow(breaker, 1, 40)), nullValue());
            assertThat(queue.size(), equalTo(4));

            try (Row evicted = queue.addRow(createRow(breaker, 0, 5))) {
                assertThat(evicted, notNullValue());
                assertRowValues(evicted, 0, 30, 60);
            }
            try (Row evicted = queue.addRow(createRow(breaker, 1, 15))) {
                assertThat(evicted, notNullValue());
                assertRowValues(evicted, 1, 40, 80);
            }
            assertThat(queue.size(), equalTo(4));

            try (Row row = queue.addRow(createRow(breaker, 0, 50))) {
                assertThat(row, notNullValue());
                assertRowValues(row, 0, 50, 100);
            }
            try (Row row = queue.addRow(createRow(breaker, 1, 50))) {
                assertThat(row, notNullValue());
                assertRowValues(row, 1, 50, 100);
            }
            assertThat(queue.size(), equalTo(4));
        }
    }

    public void testRamBytesUsedEmpty() {
        try (GroupedQueue queue = new GroupedQueue(breaker, 5)) {
            assertRamUsageClose(queue, 0);
        }
    }

    private void assertRamUsageClose(GroupedQueue queue, int numGroups) {
        long actual = queue.ramBytesUsed();
        long expected = expectedRamBytesUsed(queue, numGroups);
        var msg = Strings.format("Expected a difference of at most %d bytes; reported: %d, RamUsageTester: %d", MAX_DIFF, actual, expected);
        assertThat(msg, Math.abs(actual - expected), lessThanOrEqualTo(MAX_DIFF));
    }

    public void testRamBytesUsedPartiallyFilled() {
        try (GroupedQueue queue = new GroupedQueue(breaker, 5)) {
            addRows(queue, 0, 10, 20, 30);
            // addRows(queue, 1, 10, 20);
            assertRamUsageClose(queue, 1);
        }
    }

    public void testRamBytesUsedAtCapacity() {
        try (GroupedQueue queue = new GroupedQueue(breaker, 5)) {
            addRows(queue, 0, 10, 20, 30, 40, 50);
            addRows(queue, 1, 10, 20, 30, 40, 50);
            addRows(queue, 2, 10, 20, 30, 40, 50);
            assertRamUsageClose(queue, 3);
        }
    }

    public void testPopAllSortedBySortKey() {
        try (GroupedQueue queue = new GroupedQueue(breaker, 5)) {
            addRows(queue, 0, 30, 10, 50);
            addRows(queue, 1, 20, 40);
            addRows(queue, 2, 15, 25, 35);
            assertQueueContents(
                queue,
                List.of(
                    Tuple.tuple(0, 10),
                    Tuple.tuple(2, 15),
                    Tuple.tuple(1, 20),
                    Tuple.tuple(2, 25),
                    Tuple.tuple(0, 30),
                    Tuple.tuple(2, 35),
                    Tuple.tuple(1, 40),
                    Tuple.tuple(0, 50)
                )
            );
        }
    }

    private Row createRow(CircuitBreaker breaker, int groupKey, int sortKey) {
        try (
            IntBlock groupKeyBlock = blockFactory.newIntBlockBuilder(1).appendInt(groupKey).build();
            IntBlock keyBlock = blockFactory.newIntBlockBuilder(1).appendInt(sortKey).build();
            IntBlock valueBlock = blockFactory.newIntBlockBuilder(1).appendInt(sortKey * 2).build()
        ) {
            Row row = new GroupedRow(breaker, List.of(SORT_ORDER), 32, 64, 0);
            var filler = new GroupedRowFiller(
                List.of(ElementType.INT, ElementType.INT, ElementType.INT),
                List.of(TopNEncoder.DEFAULT_SORTABLE, TopNEncoder.DEFAULT_SORTABLE, TopNEncoder.DEFAULT_UNSORTABLE),
                List.of(SORT_ORDER),
                new int[] { 0 },
                new Page(groupKeyBlock, keyBlock, valueBlock)
            );
            filler.writeKey(0, row);
            filler.writeValues(0, row);
            return row;
        }
    }

    private static void assertRowValues(Row row, int expectedGroupKey, int expectedSortKey, int expectedValue) {
        BytesRef groupKey = ((GroupedRow) row).groupKey().bytesRefView();
        BytesRef groupKeyReader = new BytesRef(groupKey.bytes, groupKey.offset, groupKey.length);
        assertThat(TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(groupKeyReader), equalTo(1));
        assertThat(TopNEncoder.DEFAULT_UNSORTABLE.decodeInt(groupKeyReader), equalTo(expectedGroupKey));

        BytesRef keys = row.keys().bytesRefView();
        assertThat(
            TopNEncoder.DEFAULT_SORTABLE.decodeInt(new BytesRef(keys.bytes, keys.offset + 1, keys.length - 1)),
            equalTo(expectedSortKey)
        );

        BytesRef values = row.values().bytesRefView();
        BytesRef reader = new BytesRef(values.bytes, values.offset, values.length);
        assertThat(TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(reader), equalTo(1));
        TopNEncoder.DEFAULT_UNSORTABLE.decodeInt(reader);
        assertThat(TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(reader), equalTo(1));
        assertThat(TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(reader), equalTo(1));
        assertThat(TopNEncoder.DEFAULT_UNSORTABLE.decodeInt(reader), equalTo(expectedValue));
    }

    private void addRow(GroupedQueue queue, int groupKey, int value) {
        Row row = createRow(breaker, groupKey, value);
        // This row is either the input or the evicted row, but either way it should be closed.
        Releasables.close(queue.addRow(row));
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

    private static void assertQueueContents(GroupedQueue queue, List<Tuple<Integer, Integer>> groupAndSortKeys) {
        assertThat(queue.size(), equalTo(groupAndSortKeys.size()));
        List<Row> actual = queue.popAll();
        for (int i = 0; i < groupAndSortKeys.size(); i++) {
            Tuple<Integer, Integer> expectedTuple = groupAndSortKeys.get(i);
            assertRowValues(actual.get(i), expectedTuple.v1(), expectedTuple.v2(), expectedTuple.v2() * 2);
        }
        Releasables.close(actual);
    }

    private long expectedRamBytesUsed(GroupedQueue queue, int numGroups) {
        long expected = RamUsageTester.ramUsed(queue);
        expected -= RamUsageTester.ramUsed(breaker);
        // RamUsageTester disagrees with the RamUsageEstimator on how much RAM an empty HashMap uses.
        expected -= RamUsageTester.ramUsed(new HashMap<BytesRef, UngroupedQueue>());
        expected += RamUsageEstimator.shallowSizeOfInstance(HashMap.class);

        // RamUsageTester ignores the internal structure of the map (nodes and table) for JDK Maps.
        expected += numGroups * HASH_MAP_NODE_SIZE;

        if (queue.size() > 0) {
            var size = queue.size();
            List<Row> allRows = queue.popAll();
            Row rowSample = allRows.getFirst();
            // FIXME(gal, NOCOMMIT) Reduce code duplication with UngroupedQueueTests.expectedRamBytesUsed
            expected -= size * (RamUsageTester.ramUsed(rowSample) - rowSample.ramBytesUsed());
            expected += size * RamUsageTester.ramUsed(breaker);
            expected += (size - 1) * (RamUsageTester.ramUsed(SORT_ORDER) + RamUsageTester.ramUsed("topn"));
            allRows.forEach(Releasables::close);
        }
        return expected;
    }
}
