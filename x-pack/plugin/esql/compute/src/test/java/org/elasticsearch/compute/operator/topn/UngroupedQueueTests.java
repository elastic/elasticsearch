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
import org.elasticsearch.compute.operator.topn.TopNQueue.AddResult;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

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

            List<TopNOperator.SortOrder> sortOrders = ascendingSortOrder();

            for (int i = 0; i < topCount * 2; i++) {
                addRow(queue, sortOrders, i * 10);
            }
        }
    }

    public void testAddWhenHeapNotFull() {
        int topCount = 5;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            List<TopNOperator.SortOrder> sortOrders = ascendingSortOrder();

            for (int i = 0; i < topCount; i++) {
                AddResult result = addRow(queue, sortOrders, i * 10);
                assertThat(result.evictedRow(), nullValue());
                assertThat(queue.size(), equalTo(i + 1));
            }
            assertQueueContents(queue, List.of(40, 30, 20, 10, 0));
        }
    }

    public void testAddWhenHeapFullAndRowQualifies() {
        int topCount = 3;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            List<TopNOperator.SortOrder> sortOrders = ascendingSortOrder();
            fillQueueToCapacity(queue, sortOrders, topCount);

            Row topBefore = queue.pop();
            RowFiller rowFillerTop = createRowFiller(sortOrders, extractIntValue(topBefore));
            queue.add(rowFillerTop, 0, topBefore, 0);

            try (AddResult result = addRow(queue, sortOrders, 5)) {
                assertThat(extractIntValue(result.evictedRow()), equalTo(20));
            }
            assertQueueContents(queue, List.of(10, 5, 0));
        }
    }

    public void testAddWhenHeapFullAndRowDoesNotQualify() {
        int topCount = 3;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            List<TopNOperator.SortOrder> sortOrders = ascendingSortOrder();
            addRows(queue, sortOrders, 30, 40, 50);

            AddResult result = addRow(queue, sortOrders, 60);
            assertThat(result, nullValue());
            assertQueueContents(queue, List.of(50, 40, 30));
        }
    }

    public void testAddSpareValuesPreAllocSize() {
        int topCount = 2;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            var sortOrders = ascendingSortOrder();

            Row row1 = createRow(breaker, sortOrders, 10);
            for (int i = 0; i < 3; i++) {
                row1.values().append(randomByte());
            }
            RowFiller rowFiller1 = createRowFiller(sortOrders, 10);
            AddResult result1 = queue.add(rowFiller1, 0, row1, 0);
            int valuesLengthAfterAdd = row1.values().length();
            assertThat(result1.spareValuesPreAllocSize(), equalTo(Math.max(valuesLengthAfterAdd, 0)));

            Row row2 = createRow(breaker, sortOrders, 20);
            for (int i = 0; i < 3; i++) {
                row2.values().append(randomByte());
            }
            int valuesLength2 = row2.values().length();
            RowFiller rowFiller2 = createRowFiller(sortOrders, 20);
            AddResult result2 = queue.add(rowFiller2, 0, row2, 100);
            assertThat(result2.spareValuesPreAllocSize(), equalTo(Math.max(valuesLength2, 50)));

            Row row3 = createRow(breaker, sortOrders, 30);
            RowFiller rowFiller3 = createRowFiller(sortOrders, 30);
            queue.add(rowFiller3, 0, row3, 0);

            try (var betterRow = createRow(breaker, sortOrders, 5)) {
                betterRow.values().append(randomByte());
                int betterValuesLength = betterRow.values().length();
                RowFiller rowFillerBetter = createRowFiller(sortOrders, 5);
                try (AddResult result3 = queue.add(rowFillerBetter, 0, betterRow, 200)) {
                    assertThat(result3.spareValuesPreAllocSize(), equalTo(Math.max(betterValuesLength, 100)));
                }
            }
        }
    }

    private RowFiller createRowFiller(List<TopNOperator.SortOrder> sortOrders, int value) {
        try (IntBlock keyBlock = blockFactory.newIntBlockBuilder(1).appendInt(value).build()) {
            return new UngroupedRowFiller(List.of(ElementType.INT), List.of(TopNEncoder.DEFAULT_SORTABLE), sortOrders, new Page(keyBlock));
        }
    }

    private Row createRow(CircuitBreaker breaker, List<TopNOperator.SortOrder> sortOrders, int value) {
        RowFiller rowFiller = createRowFiller(sortOrders, value);
        Row row = new UngroupedRow(breaker, sortOrders, 0, 0);
        rowFiller.writeKey(0, row);
        rowFiller.writeValues(0, row);
        return row;
    }

    private static int extractIntValue(Row row) {
        BytesRef keys = row.keys().bytesRefView();
        return TopNEncoder.DEFAULT_SORTABLE.decodeInt(new BytesRef(keys.bytes, keys.offset + 1, keys.length - 1));
    }

    private AddResult addRow(UngroupedQueue queue, List<TopNOperator.SortOrder> sortOrders, int value) {
        Row row = createRow(breaker, sortOrders, value);
        AddResult result = queue.add(createRowFiller(sortOrders, value), 0, row, 0);
        if (result == null) {
            row.close();
        }
        return result;
    }

    private void fillQueueToCapacity(UngroupedQueue queue, List<TopNOperator.SortOrder> sortOrders, int capacity) {
        addRows(queue, sortOrders, IntStream.range(0, capacity).map(i -> i * 10).toArray());
    }

    private void addRows(UngroupedQueue queue, List<TopNOperator.SortOrder> sortOrders, int... values) {
        for (int value : values) {
            addRow(queue, sortOrders, value);
        }
        assertThat(queue.size(), equalTo(values.length));
    }

    private static List<TopNOperator.SortOrder> ascendingSortOrder() {
        return List.of(new TopNOperator.SortOrder(0, true, false));
    }

    private static List<TopNOperator.SortOrder> descendingSortOrder() {
        return List.of(new TopNOperator.SortOrder(0, false, false));
    }

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
}
