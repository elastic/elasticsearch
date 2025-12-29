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
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
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
        try (TopNQueue queue = GroupedQueue.build(breaker, topCount)) {
            assertThat(queue.size(), equalTo(0));

            for (int i = 0; i < topCount * 2; i++) {
                addRow(queue, SORT_ORDER, i % 3, i * 10);
            }
        }
    }

    public void testAddWhenHeapNotFull() {
        int topCount = 5;
        try (TopNQueue queue = GroupedQueue.build(breaker, topCount)) {
            for (int i = 0; i < topCount; i++) {
                Row row = createRow(breaker, SORT_ORDER, i % 2, i * 10);
                Row result = queue.add(row);
                assertThat(result, nullValue());
                assertThat(queue.size(), equalTo(i + 1));
            }
        }
    }

    public void testAddWhenHeapFullAndRowQualifies() {
        int topCount = 3;
        try (TopNQueue queue = GroupedQueue.build(breaker, topCount)) {
            TopNOperator.SortOrder sortOrder = SORT_ORDER;
            fillQueueToCapacity(queue, sortOrder, topCount);

            Row topBefore = queue.pop();
            assertThat(topBefore, notNullValue());
            Row result = queue.add(topBefore);
            assertThat(result, nullValue());

            try (Row evicted = queue.add(createRow(breaker, sortOrder, 0, 5))) {
                assertThat(extractIntValue(evicted), equalTo(20));
            }
        }
    }

    public void testAddWhenHeapFullAndRowDoesNotQualify() {
        try (TopNQueue queue = GroupedQueue.build(breaker, 3)) {
            addRows(queue, SORT_ORDER, 0, 30, 40, 50);

            try (Row row = createRow(breaker, SORT_ORDER, 0, 60)) {
                Row result = queue.add(row);
                assertThat(result, sameInstance(row));
                assertThat(extractIntValue(result), equalTo(60));
            }
        }
    }

    public void testAddWithDifferentGroupKeys() {
        try (TopNQueue queue = GroupedQueue.build(breaker, 2)) {
            assertThat(queue.add(createRow(breaker, SORT_ORDER, 0, 10)), nullValue());
            assertThat(queue.add(createRow(breaker, SORT_ORDER, 1, 20)), nullValue());
            assertThat(queue.add(createRow(breaker, SORT_ORDER, 0, 30)), nullValue());
            assertThat(queue.add(createRow(breaker, SORT_ORDER, 1, 40)), nullValue());
            assertThat(queue.size(), equalTo(4));
        }
    }

    private Row createRow(CircuitBreaker breaker, TopNOperator.SortOrder sortOrder, int groupKey, int value) {
        UngroupedRow ungroupedRow = new UngroupedRow(breaker, List.of(sortOrder), 0, 0);
        ungroupedRow.keys().append(sortOrder.nonNul());
        TopNEncoder.DEFAULT_SORTABLE.encodeInt(value, ungroupedRow.keys());
        ungroupedRow.bytesOrder().endOffsets[0] = ungroupedRow.keys().length() - 1;

        GroupedRow groupedRow = new GroupedRow(ungroupedRow, 0);
        TopNEncoder.DEFAULT_SORTABLE.encodeInt(groupKey, groupedRow.groupKey());
        return groupedRow;
    }

    private static int extractIntValue(Row row) {
        BytesRef keys = row.keys().bytesRefView();
        return TopNEncoder.DEFAULT_SORTABLE.decodeInt(new BytesRef(keys.bytes, keys.offset + 1, keys.length - 1));
    }

    private void addRow(TopNQueue queue, TopNOperator.SortOrder sortOrder, int groupKey, int value) {
        Row row = createRow(breaker, sortOrder, groupKey, value);
        // This row is either the input or the evicted row, but either way it should be closed.
        Releasables.close(queue.add(row));
    }

    private void fillQueueToCapacity(TopNQueue queue, TopNOperator.SortOrder sortOrder, int capacity) {
        addRows(queue, sortOrder, 0, IntStream.range(0, capacity).map(i -> i * 10).toArray());
    }

    private void addRows(TopNQueue queue, TopNOperator.SortOrder sortOrder, int groupKey, int... values) {
        for (int value : values) {
            addRow(queue, sortOrder, groupKey, value);
        }
    }

    private static final TopNOperator.SortOrder SORT_ORDER = new TopNOperator.SortOrder(0, true, false);
}
