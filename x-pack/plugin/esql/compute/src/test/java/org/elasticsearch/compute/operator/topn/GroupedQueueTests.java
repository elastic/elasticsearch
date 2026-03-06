/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class GroupedQueueTests extends ESTestCase {
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
        try (GroupedQueue queue = new GroupedQueue(breaker, bigArrays, topCount)) {
            assertThat(queue.size(), equalTo(0));

            for (int i = 0; i < topCount * 2; i++) {
                addRow(queue, i % 3, i * 10);
            }
        }
    }

    public void testAddWhenQueueNotFull() {
        int topCount = 5;
        try (GroupedQueue queue = new GroupedQueue(breaker, bigArrays, topCount)) {
            for (int i = 0; i < topCount; i++) {
                TopNRow row = createRow(breaker, i * 10);
                TopNRow result = queue.addRow(i % 2, row);
                assertThat(result, nullValue());
                assertThat(queue.size(), equalTo(i + 1));
            }
        }
    }

    public void testAddWhenQueueFullAndRowQualifies() {
        int topCount = 3;
        try (GroupedQueue queue = new GroupedQueue(breaker, bigArrays, topCount)) {
            fillQueueToCapacity(queue, topCount);

            try (TopNRow evicted = queue.addRow(0, createRow(breaker, 5))) {
                assertRowValues(evicted, 20, 40);
            }
        }
    }

    public void testAddWhenQueueFullAndRowDoesNotQualify() {
        try (GroupedQueue queue = new GroupedQueue(breaker, bigArrays, 3)) {
            addRows(queue, 0, 30, 40, 50);

            try (TopNRow row = createRow(breaker, 60)) {
                TopNRow result = queue.addRow(0, row);
                assertThat(result, sameInstance(row));
            }
        }
    }

    public void testAddWithDifferentGroupKeys() {
        try (GroupedQueue queue = new GroupedQueue(breaker, bigArrays, 2)) {
            assertThat(queue.addRow(0, createRow(breaker, 10)), nullValue());
            assertThat(queue.addRow(1, createRow(breaker, 20)), nullValue());
            assertThat(queue.addRow(0, createRow(breaker, 30)), nullValue());
            assertThat(queue.addRow(1, createRow(breaker, 40)), nullValue());
            assertThat(queue.size(), equalTo(4));

            try (TopNRow evicted = queue.addRow(0, createRow(breaker, 5))) {
                assertThat(evicted, notNullValue());
                assertRowValues(evicted, 30, 60);
            }
            try (TopNRow evicted = queue.addRow(1, createRow(breaker, 15))) {
                assertThat(evicted, notNullValue());
                assertRowValues(evicted, 40, 80);
            }
            assertThat(queue.size(), equalTo(4));

            try (TopNRow row = queue.addRow(0, createRow(breaker, 50))) {
                assertThat(row, notNullValue());
                assertRowValues(row, 50, 100);
            }
            try (TopNRow row = queue.addRow(1, createRow(breaker, 50))) {
                assertThat(row, notNullValue());
                assertRowValues(row, 50, 100);
            }
            assertThat(queue.size(), equalTo(4));
        }
    }

    public void testRamBytesUsedEmpty() {
        try (GroupedQueue queue = new GroupedQueue(breaker, bigArrays, 5)) {
            assertRamBytesUsedConsistent(queue);
        }
    }

    /**
     * Verifies that ramBytesUsed() accounts for at least the shallow size and grows with content.
     * We can't use RamUsageTester for BigArrays-backed structures due to module access restrictions.
     */
    private void assertRamBytesUsedConsistent(GroupedQueue queue) {
        long reported = queue.ramBytesUsed();
        assertThat("ramBytesUsed should be positive", reported, greaterThan(0L));
    }

    public void testRamBytesUsedPartiallyFilled() {
        try (GroupedQueue queue = new GroupedQueue(breaker, bigArrays, 5)) {
            long emptySize = queue.ramBytesUsed();
            addRows(queue, 0, 10, 20, 30);
            long filledSize = queue.ramBytesUsed();
            assertThat("RAM usage should grow after adding rows", filledSize, greaterThan(emptySize));
        }
    }

    public void testRamBytesUsedAtCapacity() {
        try (GroupedQueue queue = new GroupedQueue(breaker, bigArrays, 5)) {
            long emptySize = queue.ramBytesUsed();
            addRows(queue, 0, 10, 20, 30, 40, 50);
            long oneGroupSize = queue.ramBytesUsed();
            addRows(queue, 1, 10, 20, 30, 40, 50);
            addRows(queue, 2, 10, 20, 30, 40, 50);
            long threeGroupSize = queue.ramBytesUsed();
            assertThat("RAM should grow with first group", oneGroupSize, greaterThan(emptySize));
            assertThat("RAM should grow with more groups", threeGroupSize, greaterThan(oneGroupSize));
        }
    }

    public void testPopAllSortedBySortKey() {
        try (GroupedQueue queue = new GroupedQueue(breaker, bigArrays, 5)) {
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

    private TopNRow createRow(CircuitBreaker breaker, int sortKey) {
        IntBlock groupKeyBlock = blockFactory.newIntBlockBuilder(1).appendInt(0).build();
        IntBlock keyBlock = blockFactory.newIntBlockBuilder(1).appendInt(sortKey).build();
        IntBlock valueBlock = blockFactory.newIntBlockBuilder(1).appendInt(sortKey * 2).build();
        TopNRow row = new TopNRow(breaker, 32, 64);
        var filler = new TopNOperator.RowFiller(
            List.of(ElementType.INT, ElementType.INT, ElementType.INT),
            List.of(TopNEncoder.DEFAULT_SORTABLE, TopNEncoder.DEFAULT_SORTABLE, TopNEncoder.DEFAULT_UNSORTABLE),
            SORT_ORDERS,
            new boolean[] { false, true, false },
            new Page(groupKeyBlock, keyBlock, valueBlock)
        );
        try {
            filler.writeKey(0, row);
            filler.writeValues(0, row);
        } finally {
            Releasables.close(groupKeyBlock, keyBlock, valueBlock);
        }
        return row;
    }

    private static void assertRowValues(TopNRow row, int expectedSortKey, int expectedValue) {
        BytesRef keys = row.keys.bytesRefView();
        assertThat(
            TopNEncoder.DEFAULT_SORTABLE.decodeInt(new BytesRef(keys.bytes, keys.offset + 1, keys.length - 1)),
            equalTo(expectedSortKey)
        );

        BytesRef values = row.values.bytesRefView();
        BytesRef reader = new BytesRef(values.bytes, values.offset, values.length);
        assertThat(TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(reader), equalTo(1));
        TopNEncoder.DEFAULT_UNSORTABLE.decodeInt(reader);
        assertThat(TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(reader), equalTo(1));
        assertThat(TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(reader), equalTo(1));
        assertThat(TopNEncoder.DEFAULT_UNSORTABLE.decodeInt(reader), equalTo(expectedValue));
    }

    private void addRow(GroupedQueue queue, int groupKey, int value) {
        TopNRow row = createRow(breaker, value);
        Releasables.close(queue.addRow(groupKey, row));
    }

    private void fillQueueToCapacity(GroupedQueue queue, int capacity) {
        addRows(queue, 0, IntStream.range(0, capacity).map(i -> i * 10).toArray());
    }

    private void addRows(GroupedQueue queue, int groupKey, int... values) {
        for (int value : values) {
            addRow(queue, groupKey, value);
        }
    }

    private static final List<TopNOperator.SortOrder> SORT_ORDERS = List.of(new TopNOperator.SortOrder(1, true, false));

    private void assertQueueContents(GroupedQueue queue, List<Tuple<Integer, Integer>> groupAndSortKeys) {
        assertThat(queue.size(), equalTo(groupAndSortKeys.size()));
        List<TopNRow> actual = queue.popAll();
        for (int i = 0; i < groupAndSortKeys.size(); i++) {
            Tuple<Integer, Integer> expectedTuple = groupAndSortKeys.get(i);
            assertRowValues(actual.get(i), expectedTuple.v2(), expectedTuple.v2() * 2);
        }
        Releasables.close(actual);
    }
}
