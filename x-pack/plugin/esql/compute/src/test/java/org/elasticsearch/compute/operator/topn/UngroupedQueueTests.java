/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.topn.TopNQueue.AddResult;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class UngroupedQueueTests extends ESTestCase {
    private final CircuitBreaker breaker = new NoneCircuitBreakerService().getBreaker("test");
    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    private static List<TopNOperator.SortOrder> ascendingSortOrder() {
        return List.of(new TopNOperator.SortOrder(0, true, false));
    }

    private static List<TopNOperator.SortOrder> descendingSortOrder() {
        return List.of(new TopNOperator.SortOrder(0, false, false));
    }

    public void testAddWhenHeapNotFull() {
        int topCount = 5;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            assertThat(queue.size(), equalTo(0));

            List<TopNOperator.SortOrder> sortOrders = ascendingSortOrder();

            for (int i = 0; i < topCount; i++) {
                AddResult result = addRow(queue, sortOrders, i * 10);
                assertThat(result.evictedRow(), is(nullValue()));
                assertThat(queue.size(), equalTo(i + 1));
            }
        }
    }

    public void testAddWhenHeapFullAndRowQualifies() {
        int topCount = 3;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            List<TopNOperator.SortOrder> sortOrders = ascendingSortOrder();
            fillQueueToCapacity(queue, sortOrders, topCount);

            Row topBefore = queue.pop();
            RowFiller rowFillerTop = createRowFiller(sortOrders, extractIntValue(topBefore, sortOrders));
            queue.add(rowFillerTop, 0, topBefore, 0);
            topBefore.close();

            AddResult result = addRow(queue, sortOrders, 5);
            assertThat(extractIntValue(result.evictedRow(), sortOrders), equalTo(20));
            assertThat(queue.size(), equalTo(topCount));
            result.evictedRow().close();
        }
    }

    public void testAddWhenHeapFullAndRowDoesNotQualify() {
        int topCount = 3;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            List<TopNOperator.SortOrder> sortOrders = ascendingSortOrder();
            addRows(queue, sortOrders, 30, 40, 50);

            AddResult result = addRow(queue, sortOrders, 60);
            assertThat(result, is(nullValue()));
            assertThat(queue.size(), equalTo(topCount));
        }
    }

    public void testAddWithDescendingOrder() {
        int topCount = 3;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            List<TopNOperator.SortOrder> sortOrders = descendingSortOrder();
            addRows(queue, sortOrders, 50, 40, 30);
            assertThat(queue.size(), equalTo(topCount));

            AddResult result = addRow(queue, sortOrders, 60);
            assertThat(extractIntValue(result.evictedRow(), sortOrders), equalTo(30));
            result.evictedRow().close();

            AddResult result2 = addRow(queue, sortOrders, 5);
            assertThat(result2, is(nullValue()));
        }
    }

    public void testAddSpareValuesPreAllocSize() {
        int topCount = 2;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            List<TopNOperator.SortOrder> sortOrders = List.of(new TopNOperator.SortOrder(0, true, false));

            Row row1 = createRow(breaker, sortOrders, 10);
            row1.values().append(randomByte());
            row1.values().append(randomByte());
            row1.values().append(randomByte());
            RowFiller rowFiller1 = createRowFiller(sortOrders, 10);
            AddResult result1 = queue.add(rowFiller1, 0, row1, 0);
            int valuesLengthAfterAdd = row1.values().length();
            assertThat(result1.spareValuesPreAllocSize(), equalTo(Math.max(valuesLengthAfterAdd, 0)));

            Row row2 = createRow(breaker, sortOrders, 20);
            row2.values().append(randomByte());
            row2.values().append(randomByte());
            row2.values().append(randomByte());
            int valuesLength2 = row2.values().length();
            RowFiller rowFiller2 = createRowFiller(sortOrders, 20);
            AddResult result2 = queue.add(rowFiller2, 0, row2, 100);
            assertThat(result2.spareValuesPreAllocSize(), equalTo(Math.max(valuesLength2, 50)));

            Row row3 = createRow(breaker, sortOrders, 30);
            RowFiller rowFiller3 = createRowFiller(sortOrders, 30);
            queue.add(rowFiller3, 0, row3, 0);

            Row betterRow = createRow(breaker, sortOrders, 5);
            betterRow.values().append(randomByte());
            int betterValuesLength = betterRow.values().length();
            RowFiller rowFillerBetter = createRowFiller(sortOrders, 5);
            AddResult result3 = queue.add(rowFillerBetter, 0, betterRow, 200);
            assertThat(result3.spareValuesPreAllocSize(), equalTo(Math.max(betterValuesLength, 100)));
            result3.evictedRow().close();
            betterRow.close();
        }
    }

    public void testAddMultipleRowsAndVerifyOrder() {
        int topCount = 5;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            List<TopNOperator.SortOrder> sortOrders = ascendingSortOrder();

            List<Integer> values = List.of(50, 10, 30, 20, 40, 5, 60, 15, 25, 35);
            for (int value : values) {
                AddResult result = addRow(queue, sortOrders, value);
                if (result != null && result.evictedRow() != null) {
                    result.evictedRow().close();
                }
            }

            assertThat(queue.size(), equalTo(topCount));

            List<Integer> poppedValues = new ArrayList<>();
            while (queue.size() > 0) {
                Row popped = queue.pop();
                poppedValues.add(extractIntValue(popped, sortOrders));
                popped.close();
            }

            Collections.sort(poppedValues);
            assertThat(poppedValues, equalTo(List.of(5, 10, 15, 20, 25)));
        }
    }

    public void testAddWithEviction() {
        int topCount = 3;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            List<TopNOperator.SortOrder> sortOrders = ascendingSortOrder();
            fillQueueToCapacity(queue, sortOrders, topCount);

            Row evictedRow = null;
            for (int i = 4; i <= 10; i++) {
                int value = i * 10;
                AddResult result = addRow(queue, sortOrders, value);
                if (result != null && result.evictedRow() != null) {
                    if (evictedRow != null) {
                        evictedRow.close();
                    }
                    evictedRow = result.evictedRow();
                }
            }

            assertThat(queue.size(), equalTo(topCount));

            if (evictedRow != null) {
                evictedRow.close();
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

    private static int extractIntValue(Row row, List<TopNOperator.SortOrder> sortOrders) {
        BytesRef keys = row.keys().bytesRefView();
        return TopNEncoder.DEFAULT_SORTABLE.decodeInt(new BytesRef(keys.bytes, keys.offset + 1, keys.length - 1));
    }

    private AddResult addRow(UngroupedQueue queue, List<TopNOperator.SortOrder> sortOrders, int value) {
        Row row = createRow(breaker, sortOrders, value);
        return queue.add(createRowFiller(sortOrders, value), 0, row, 0);
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
}
