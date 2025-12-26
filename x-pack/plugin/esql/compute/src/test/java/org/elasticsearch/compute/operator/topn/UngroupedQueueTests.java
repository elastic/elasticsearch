/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.topn.TopNQueue.AddResult;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class UngroupedQueueTests extends ESTestCase {
    private final CircuitBreaker breaker = new NoopCircuitBreaker(CircuitBreaker.REQUEST);
    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    public void testAddWhenHeapNotFull() {
        int topCount = 5;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            assertThat(queue.size(), equalTo(0));

            List<TopNOperator.SortOrder> sortOrders = List.of(new TopNOperator.SortOrder(0, true, false));

            Row row1 = createRow(breaker, sortOrders, 10L);
            RowFiller rowFiller1 = createRowFiller(sortOrders, 10L);
            AddResult result1 = queue.add(rowFiller1, 0, row1, 0);
            assertThat(result1, is(notNullValue()));
            assertThat(result1.evictedRow(), is(nullValue()));
            assertThat(queue.size(), equalTo(1));

            Row row2 = createRow(breaker, sortOrders, 20L);
            RowFiller rowFiller2 = createRowFiller(sortOrders, 20L);
            AddResult result2 = queue.add(rowFiller2, 0, row2, 0);
            assertThat(result2, is(notNullValue()));
            assertThat(result2.evictedRow(), is(nullValue()));
            assertThat(queue.size(), equalTo(2));

            for (int i = 3; i <= topCount; i++) {
                Row row = createRow(breaker, sortOrders, (long) i * 10);
                RowFiller rowFiller = createRowFiller(sortOrders, (long) i * 10);
                AddResult result = queue.add(rowFiller, 0, row, 0);
                assertThat(result, is(notNullValue()));
                assertThat(result.evictedRow(), is(nullValue()));
                assertThat(queue.size(), equalTo(i));
            }
        }
    }

    public void testAddWhenHeapFullAndRowQualifies() {
        int topCount = 3;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            List<TopNOperator.SortOrder> sortOrders = List.of(new TopNOperator.SortOrder(0, true, false));

            Row row1 = createRow(breaker, sortOrders, 10L);
            RowFiller rowFiller1 = createRowFiller(sortOrders, 10L);
            queue.add(rowFiller1, 0, row1, 0);
            Row row2 = createRow(breaker, sortOrders, 20L);
            RowFiller rowFiller2 = createRowFiller(sortOrders, 20L);
            queue.add(rowFiller2, 0, row2, 0);
            Row row3 = createRow(breaker, sortOrders, 30L);
            RowFiller rowFiller3 = createRowFiller(sortOrders, 30L);
            queue.add(rowFiller3, 0, row3, 0);

            assertThat(queue.size(), equalTo(topCount));

            Row topBefore = queue.pop();
            RowFiller rowFillerTop = createRowFiller(sortOrders, extractLongValue(topBefore, sortOrders));
            queue.add(rowFillerTop, 0, topBefore, 0);
            topBefore.close();

            Row betterRow = createRow(breaker, sortOrders, 5L);
            RowFiller rowFillerBetter = createRowFiller(sortOrders, 5L);
            AddResult result = queue.add(rowFillerBetter, 0, betterRow, 0);
            assertThat(result, is(notNullValue()));
            assertThat(result.evictedRow(), is(notNullValue()));
            assertThat(queue.size(), equalTo(topCount));

            Row evicted = result.evictedRow();
            assertThat(evicted, is(notNullValue()));
            evicted.close();
            betterRow.close();
        }
    }

    public void testAddWhenHeapFullAndRowDoesNotQualify() {
        int topCount = 3;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            List<TopNOperator.SortOrder> sortOrders = List.of(new TopNOperator.SortOrder(0, true, false));

            Row row1 = createRow(breaker, sortOrders, 30L);
            RowFiller rowFiller1 = createRowFiller(sortOrders, 30L);
            queue.add(rowFiller1, 0, row1, 0);
            Row row2 = createRow(breaker, sortOrders, 40L);
            RowFiller rowFiller2 = createRowFiller(sortOrders, 40L);
            queue.add(rowFiller2, 0, row2, 0);
            Row row3 = createRow(breaker, sortOrders, 50L);
            RowFiller rowFiller3 = createRowFiller(sortOrders, 50L);
            queue.add(rowFiller3, 0, row3, 0);

            assertThat(queue.size(), equalTo(topCount));

            Row worseRow = createRow(breaker, sortOrders, 60L);
            RowFiller rowFillerWorse = createRowFiller(sortOrders, 60L);
            AddResult result = queue.add(rowFillerWorse, 0, worseRow, 0);
            assertThat(result, is(nullValue()));
            assertThat(queue.size(), equalTo(topCount));
            worseRow.close();
        }
    }

    public void testAddWithDescendingOrder() {
        int topCount = 3;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            List<TopNOperator.SortOrder> sortOrders = List.of(new TopNOperator.SortOrder(0, false, false));

            Row row1 = createRow(breaker, sortOrders, 50L);
            RowFiller rowFiller1 = createRowFiller(sortOrders, 50L);
            queue.add(rowFiller1, 0, row1, 0);
            Row row2 = createRow(breaker, sortOrders, 40L);
            RowFiller rowFiller2 = createRowFiller(sortOrders, 40L);
            queue.add(rowFiller2, 0, row2, 0);
            Row row3 = createRow(breaker, sortOrders, 30L);
            RowFiller rowFiller3 = createRowFiller(sortOrders, 30L);
            queue.add(rowFiller3, 0, row3, 0);

            assertThat(queue.size(), equalTo(topCount));

            Row betterRow = createRow(breaker, sortOrders, 60L);
            RowFiller rowFillerBetter = createRowFiller(sortOrders, 60L);
            AddResult result = queue.add(rowFillerBetter, 0, betterRow, 0);
            assertThat(result, is(notNullValue()));
            assertThat(result.evictedRow(), is(notNullValue()));
            result.evictedRow().close();
            betterRow.close();

            Row worseRow = createRow(breaker, sortOrders, 5L);
            RowFiller rowFillerWorse = createRowFiller(sortOrders, 5L);
            AddResult result2 = queue.add(rowFillerWorse, 0, worseRow, 0);
            assertThat(result2, is(nullValue()));
            worseRow.close();
        }
    }

    public void testAddSpareValuesPreAllocSize() {
        int topCount = 2;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            List<TopNOperator.SortOrder> sortOrders = List.of(new TopNOperator.SortOrder(0, true, false));

            Row row1 = createRow(breaker, sortOrders, 10L);
            row1.values().append(randomByte());
            row1.values().append(randomByte());
            row1.values().append(randomByte());
            RowFiller rowFiller1 = createRowFiller(sortOrders, 10L);
            AddResult result1 = queue.add(rowFiller1, 0, row1, 0);
            int valuesLengthAfterAdd = row1.values().length();
            assertThat(result1.spareValuesPreAllocSize(), equalTo(Math.max(valuesLengthAfterAdd, 0)));

            Row row2 = createRow(breaker, sortOrders, 20L);
            row2.values().append(randomByte());
            row2.values().append(randomByte());
            row2.values().append(randomByte());
            int valuesLength2 = row2.values().length();
            RowFiller rowFiller2 = createRowFiller(sortOrders, 20L);
            AddResult result2 = queue.add(rowFiller2, 0, row2, 100);
            assertThat(result2.spareValuesPreAllocSize(), equalTo(Math.max(valuesLength2, 50)));

            Row row3 = createRow(breaker, sortOrders, 30L);
            RowFiller rowFiller3 = createRowFiller(sortOrders, 30L);
            queue.add(rowFiller3, 0, row3, 0);

            Row betterRow = createRow(breaker, sortOrders, 5L);
            betterRow.values().append(randomByte());
            int betterValuesLength = betterRow.values().length();
            RowFiller rowFillerBetter = createRowFiller(sortOrders, 5L);
            AddResult result3 = queue.add(rowFillerBetter, 0, betterRow, 200);
            assertThat(result3.spareValuesPreAllocSize(), equalTo(Math.max(betterValuesLength, 100)));
            result3.evictedRow().close();
            betterRow.close();
        }
    }

    public void testAddMultipleRowsAndVerifyOrder() {
        int topCount = 5;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            List<TopNOperator.SortOrder> sortOrders = List.of(new TopNOperator.SortOrder(0, true, false));

            List<Long> values = List.of(50L, 10L, 30L, 20L, 40L, 5L, 60L, 15L, 25L, 35L);
            for (Long value : values) {
                Row row = createRow(breaker, sortOrders, value);
                RowFiller rowFiller = createRowFiller(sortOrders, value);
                AddResult result = queue.add(rowFiller, 0, row, 0);
                if (result != null && result.evictedRow() != null) {
                    result.evictedRow().close();
                }
                if (result == null) {
                    row.close();
                }
            }

            assertThat(queue.size(), equalTo(topCount));

            List<Long> poppedValues = new ArrayList<>();
            while (queue.size() > 0) {
                Row popped = queue.pop();
                poppedValues.add(extractLongValue(popped, sortOrders));
                popped.close();
            }

            poppedValues.sort(Long::compareTo);
            assertThat(poppedValues, equalTo(List.of(5L, 10L, 15L, 20L, 25L)));
        }
    }

    public void testAddWithEviction() {
        int topCount = 3;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            List<TopNOperator.SortOrder> sortOrders = List.of(new TopNOperator.SortOrder(0, true, false));

            Row row1 = createRow(breaker, sortOrders, 10L);
            RowFiller rowFiller1 = createRowFiller(sortOrders, 10L);
            queue.add(rowFiller1, 0, row1, 0);
            Row row2 = createRow(breaker, sortOrders, 20L);
            RowFiller rowFiller2 = createRowFiller(sortOrders, 20L);
            queue.add(rowFiller2, 0, row2, 0);
            Row row3 = createRow(breaker, sortOrders, 30L);
            RowFiller rowFiller3 = createRowFiller(sortOrders, 30L);
            queue.add(rowFiller3, 0, row3, 0);

            Row evictedRow = null;
            for (int i = 4; i <= 10; i++) {
                long value = (long) i * 10;
                Row newRow = createRow(breaker, sortOrders, value);
                RowFiller rowFiller = createRowFiller(sortOrders, value);
                AddResult result = queue.add(rowFiller, 0, newRow, 0);
                if (result != null && result.evictedRow() != null) {
                    if (evictedRow != null) {
                        evictedRow.close();
                    }
                    evictedRow = result.evictedRow();
                } else if (newRow != evictedRow) {
                    newRow.close();
                }
            }

            assertThat(queue.size(), equalTo(topCount));

            if (evictedRow != null) {
                evictedRow.close();
            }
        }
    }

    private RowFiller createRowFiller(List<TopNOperator.SortOrder> sortOrders, Long value) {
        try (LongBlock keyBlock = blockFactory.newLongBlockBuilder(1).appendLong(value).build()) {
            Page page = new Page(keyBlock);
            return new UngroupedRowFiller(
                List.of(ElementType.LONG),
                List.of(TopNEncoder.DEFAULT_SORTABLE),
                sortOrders,
                page
            );
        }
    }

    private Row createRow(CircuitBreaker breaker, List<TopNOperator.SortOrder> sortOrders, Long value) {
        Row row = new UngroupedRow(breaker, sortOrders, 0, 0);
        RowFiller rowFiller = createRowFiller(sortOrders, value);
        rowFiller.writeKey(0, row);
        rowFiller.writeValues(0, row);
        return row;
    }

    private Long extractLongValue(Row row, List<TopNOperator.SortOrder> sortOrders) {
        BytesRef keys = row.keys().bytesRefView();
        if (keys.length == 0) {
            return null;
        }
        int offset = keys.offset;
        if (keys.bytes[offset] == sortOrders.get(0).nonNul()) {
            offset++;
            BytesRef keyBytes = new BytesRef(keys.bytes, offset, keys.length - 1);
            return TopNEncoder.DEFAULT_SORTABLE.decodeLong(keyBytes);
        }
        return null;
    }
}

