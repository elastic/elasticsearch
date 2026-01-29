/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.tests.util.RamUsageTester;
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
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class UngroupedQueueTests extends ESTestCase {
    private final BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(1));
    private final CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
    private final BlockFactory blockFactory = new BlockFactory(breaker, bigArrays);

    @After
    public void allMemoryReleased() throws Exception {
        MockBigArrays.ensureAllArraysAreReleased();
        assertThat("Not all memory was released", breaker.getUsed(), equalTo(0L));
        assertThat("Not all blocks were released", blockFactory.breaker().getUsed(), equalTo(0L));
    }

    // This test will fail if cleanup is not done properly.
    public void testCleanup() {
        int topCount = 5;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            assertThat(queue.size(), equalTo(0));

            for (int i = 0; i < topCount * 2; i++) {
                addRow(queue, i * 10);
            }
        }
    }

    public void testAddWhenHeapNotFull() {
        int topCount = 5;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            for (int i = 0; i < topCount; i++) {
                Row result = queue.addRow(createRow(breaker, i * 10));
                assertThat(result, nullValue());
                assertThat(queue.size(), equalTo(i + 1));
            }
            assertQueueContents(queue, List.of(0, 10, 20, 30, 40));
        }
    }

    public void testAddWhenHeapFullAndRowQualifies() {
        int topCount = 3;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            fillQueueToCapacity(queue, topCount);

            try (Row evicted = queue.addRow(createRow(breaker, 5))) {
                assertRowValues(evicted, 20, 40);
            }
            assertQueueContents(queue, List.of(0, 5, 10));
        }
    }

    public void testAddWhenHeapFullAndRowDoesNotQualify() {
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, 3)) {
            addRows(queue, 30, 40, 50);

            try (Row row = createRow(breaker, 60)) {
                Row result = queue.addRow(row);
                assertThat(result, sameInstance(row));
            }
            assertQueueContents(queue, List.of(30, 40, 50));
        }
    }

    public void testRamBytesUsedEmpty() {
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, 5)) {
            long actual = queue.ramBytesUsed();
            assertThat(actual, equalTo(expectedRamBytesUsed(queue)));
        }
    }

    public void testRamBytesUsedPartiallyFilled() {
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, 5)) {
            addRows(queue, 10, 20, 30);
            long actual = queue.ramBytesUsed();
            assertThat(actual, equalTo(expectedRamBytesUsed(queue)));
        }
    }

    public void testRamBytesUsedAtCapacity() {
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, 5)) {
            addRows(queue, 10, 20, 30, 40, 50);
            long actual = queue.ramBytesUsed();
            assertThat(actual, equalTo(expectedRamBytesUsed(queue)));
        }
    }

    private Row createRow(CircuitBreaker breaker, int value) {
        try (
            IntBlock keyBlock = blockFactory.newIntBlockBuilder(1).appendInt(value).build();
            IntBlock valueBlock = blockFactory.newIntBlockBuilder(1).appendInt(value * 2).build()
        ) {
            Row row = new UngroupedRow(breaker, SORT_ORDERS, 32, 64);
            var filler = new UngroupedRowFiller(
                List.of(ElementType.INT, ElementType.INT),
                List.of(TopNEncoder.DEFAULT_SORTABLE, TopNEncoder.DEFAULT_UNSORTABLE),
                SORT_ORDERS,
                new Page(keyBlock, valueBlock)
            );
            filler.writeKey(0, row);
            filler.writeValues(0, row);
            return row;
        }
    }

    private static void assertRowValues(Row row, int expectedSortKey, int expectedValue) {
        BytesRef keys = row.keys().bytesRefView();
        assertThat(
            TopNEncoder.DEFAULT_SORTABLE.decodeInt(new BytesRef(keys.bytes, keys.offset + 1, keys.length - 1)),
            equalTo(expectedSortKey)
        );

        BytesRef values = row.values().bytesRefView();
        BytesRef reader = new BytesRef(values.bytes, values.offset, values.length);
        assertThat(TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(reader), equalTo(1));
        assertThat(TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(reader), equalTo(1));
        assertThat(TopNEncoder.DEFAULT_UNSORTABLE.decodeInt(reader), equalTo(expectedValue));
    }

    private void addRow(UngroupedQueue queue, int value) {
        Row result = queue.addRow(createRow(breaker, value));
        // This row is either the input or the evicted row, but either way it should be closed.
        Releasables.close(result);
    }

    private void fillQueueToCapacity(UngroupedQueue queue, int capacity) {
        addRows(queue, IntStream.range(0, capacity).map(i -> i * 10).toArray());
    }

    private void addRows(UngroupedQueue queue, int... values) {
        for (int value : values) {
            addRow(queue, value);
        }
        assertThat(queue.size(), equalTo(values.length));
    }

    private static final List<TopNOperator.SortOrder> SORT_ORDERS = List.of(new TopNOperator.SortOrder(0, true, false));

    private static void assertQueueContents(UngroupedQueue queue, List<Integer> expectedKeys) {
        assertThat(queue.size(), equalTo(expectedKeys.size()));
        List<Row> actual = queue.popAll();
        for (int i = 0; i < expectedKeys.size(); i++) {
            int expectedKey = expectedKeys.get(i);
            assertRowValues(actual.get(i), expectedKey, expectedKey * 2);
        }
        Releasables.close(actual);
    }

    private long expectedRamBytesUsed(UngroupedQueue queue) {
        long expected = RamUsageTester.ramUsed(queue);
        expected -= RamUsageTester.ramUsed(breaker);
        if (queue.size() > 0) {
            List<Row> allRows = queue.popAll();
            Row rowSample = allRows.getFirst();
            expected -= UngroupedRowTests.sharedRowBytes(rowSample);
            expected += allRows.stream().mapToLong(UngroupedRowTests::undercountBytesPerRow).sum();
            allRows.forEach(queue::addRow);
        }
        return expected;
    }

}
