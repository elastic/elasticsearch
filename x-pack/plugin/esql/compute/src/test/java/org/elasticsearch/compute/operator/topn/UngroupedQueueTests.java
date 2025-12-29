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

import java.util.ArrayList;
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
                Row result = queue.add(createRow(breaker, i * 10));
                assertThat(result, nullValue());
                assertThat(queue.size(), equalTo(i + 1));
            }
            assertQueueContents(queue, List.of(40, 30, 20, 10, 0));
        }
    }

    public void testAddWhenHeapFullAndRowQualifies() {
        int topCount = 3;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            fillQueueToCapacity(queue, topCount);

            Row result = queue.add(queue.pop());
            assertThat(result, nullValue());

            Row evicted = queue.add(createRow(breaker, 5));
            assertThat(extractIntValue(evicted), equalTo(20));
            Releasables.close(evicted);
            assertQueueContents(queue, List.of(10, 5, 0));
        }
    }

    public void testAddWhenHeapFullAndRowDoesNotQualify() {
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, 3)) {
            addRows(queue, 30, 40, 50);

            Row row = createRow(breaker, 60);
            Row result = queue.add(row);
            assertThat(result, sameInstance(row));
            assertThat(extractIntValue(result), equalTo(60));
            Releasables.close(result);
            assertQueueContents(queue, List.of(50, 40, 30));
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

    public void testCloseReleasesAllMemory() {
        UngroupedQueue queue = UngroupedQueue.build(breaker, 5);
        addRows(queue, 10, 20, 30, 40, 50);
        long ramBytesUsed = queue.ramBytesUsed();
        long usedBefore = breaker.getUsed();
        queue.close();
        assertThat("Memory should be released after close", breaker.getUsed(), equalTo(usedBefore - ramBytesUsed));
    }

    private Row createRow(CircuitBreaker breaker, int value) {
        try (
            IntBlock keyBlock = blockFactory.newIntBlockBuilder(1).appendInt(value).build();
            IntBlock valueBlock = blockFactory.newIntBlockBuilder(1).appendInt(value * 2).build()
        ) {
            Row row = new UngroupedRow(breaker, List.of(UngroupedQueueTests.SORT_ORDER), 32, 64);
            var filler = new UngroupedRowFiller(
                List.of(ElementType.INT, ElementType.INT),
                List.of(TopNEncoder.DEFAULT_SORTABLE, TopNEncoder.DEFAULT_UNSORTABLE),
                List.of(SORT_ORDER),
                new Page(keyBlock, valueBlock)
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

    private void addRow(UngroupedQueue queue, int value) {
        Row result = queue.add(createRow(breaker, value));
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

    private static final TopNOperator.SortOrder SORT_ORDER = new TopNOperator.SortOrder(0, true, false);

    private static void assertQueueContents(UngroupedQueue queue, List<Integer> expected) {
        assertThat(queue.size(), equalTo(expected.size()));
        var actual = new ArrayList<Integer>();
        while (queue.size() > 0) {
            try (Row row = queue.pop()) {
                actual.add(extractIntValue(row));
            }
        }
        assertThat(actual, equalTo(expected));
    }

    private long expectedRamBytesUsed(UngroupedQueue queue) {
        long expected = RamUsageTester.ramUsed(queue);
        expected -= RamUsageTester.ramUsed(breaker);
        if (queue.size() > 0) {
            var size = queue.size();
            // Account for the discrepancy for each row.
            Row rowSample = queue.pop();
            expected -= size * (RamUsageTester.ramUsed(rowSample) - rowSample.ramBytesUsed());
            // The breaker is already accounted for in for each RamUsageTester.ramUsed(rowSample), but is only counted once by ramBytesUsed.
            expected += size * RamUsageTester.ramUsed(breaker);
            // These are shared, but are only counted once by ramBytesUsed.
            // FIXME(gal, NOCOMMIT) Reduce duplication with UngroupedRowTests
            expected += (size - 1) * (RamUsageTester.ramUsed(SORT_ORDER) + RamUsageTester.ramUsed("topn"));
            queue.add(rowSample);
        }
        return expected;
    }
}
