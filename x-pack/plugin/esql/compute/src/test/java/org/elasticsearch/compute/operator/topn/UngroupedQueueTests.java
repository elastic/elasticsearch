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
                addRow(queue, SORT_ORDER, i * 10);
            }
        }
    }

    public void testAddWhenHeapNotFull() {
        int topCount = 5;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            for (int i = 0; i < topCount; i++) {
                Row result = queue.add(createRow(breaker, SORT_ORDER, i * 10));
                assertThat(result, nullValue());
                assertThat(queue.size(), equalTo(i + 1));
            }
            assertQueueContents(queue, List.of(40, 30, 20, 10, 0));
        }
    }

    public void testAddWhenHeapFullAndRowQualifies() {
        int topCount = 3;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            TopNOperator.SortOrder sortOrder = SORT_ORDER;
            fillQueueToCapacity(queue, sortOrder, topCount);

            Row result = queue.add(queue.pop());
            assertThat(result, nullValue());

            Row evicted = queue.add(createRow(breaker, sortOrder, 5));
            assertThat(extractIntValue(evicted), equalTo(20));
            Releasables.close(evicted);
            assertQueueContents(queue, List.of(10, 5, 0));
        }
    }

    public void testAddWhenHeapFullAndRowDoesNotQualify() {
        int topCount = 3;
        try (UngroupedQueue queue = UngroupedQueue.build(breaker, topCount)) {
            addRows(queue, SORT_ORDER, 30, 40, 50);

            Row row = createRow(breaker, SORT_ORDER, 60);
            Row result = queue.add(row);
            assertThat(result, sameInstance(row));
            assertThat(extractIntValue(result), equalTo(60));
            Releasables.close(result);
            assertQueueContents(queue, List.of(50, 40, 30));
        }
    }

    private Row createRow(CircuitBreaker breaker, TopNOperator.SortOrder sortOrder, int value) {
        Row row = new UngroupedRow(breaker, List.of(sortOrder), 0, 0);
        row.keys().append(sortOrder.nonNul());
        TopNEncoder.DEFAULT_SORTABLE.encodeInt(value, row.keys());
        row.bytesOrder().endOffsets[0] = row.keys().length() - 1;
        return row;
    }

    private static int extractIntValue(Row row) {
        BytesRef keys = row.keys().bytesRefView();
        return TopNEncoder.DEFAULT_SORTABLE.decodeInt(new BytesRef(keys.bytes, keys.offset + 1, keys.length - 1));
    }

    private Row addRow(UngroupedQueue queue, TopNOperator.SortOrder sortOrder, int value) {
        Row row = createRow(breaker, sortOrder, value);
        Row result = queue.add(row);
        if (result == row) {
            row.close();
            return null;
        }
        return result;
    }

    private void fillQueueToCapacity(UngroupedQueue queue, TopNOperator.SortOrder sortOrder, int capacity) {
        addRows(queue, sortOrder, IntStream.range(0, capacity).map(i -> i * 10).toArray());
    }

    private void addRows(UngroupedQueue queue, TopNOperator.SortOrder sortOrder, int... values) {
        for (int value : values) {
            addRow(queue, sortOrder, value);
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
}
