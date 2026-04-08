/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class GroupedQueueTests extends ESTestCase {
    private final CircuitBreakerService breakerService = newLimitedBreakerService(ByteSizeValue.ofMb(1));
    private final CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
    private final BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService);
    private final BlockFactory blockFactory = new BlockFactory(breaker, bigArrays);

    public void testGroupIsolation() {
        try (GroupedQueue queue = new GroupedQueue(breaker, bigArrays, 2)) {
            addRows(queue, 0, 10, 30, 5);
            addRows(queue, 1, 20, 40, 15);
            assertThat(queue.size(), equalTo(4));
            assertQueueContents(queue, List.of(5, 10, 15, 20));
        }
    }

    public void testRamBytesUsed() {
        try (GroupedQueue queue = new GroupedQueue(breaker, bigArrays, 5)) {
            long emptySize = queue.ramBytesUsed();
            assertThat("ramBytesUsed should be positive", emptySize, greaterThan(0L));

            addRows(queue, 0, 10, 20, 30);
            long oneGroupSize = queue.ramBytesUsed();
            assertThat("RAM should grow with first group", oneGroupSize, greaterThan(emptySize));

            addRows(queue, 1, 10, 20, 30);
            assertThat("RAM should grow with more groups", queue.ramBytesUsed(), greaterThan(oneGroupSize));
        }
    }

    public void testPopAllSortedBySortKey() {
        try (GroupedQueue queue = new GroupedQueue(breaker, bigArrays, 5)) {
            addRows(queue, 0, 30, 10, 50);
            addRows(queue, 1, 20, 40);
            addRows(queue, 2, 15, 25, 35);
            assertQueueContents(queue, List.of(10, 15, 20, 25, 30, 35, 40, 50));
        }
    }

    private TopNRow createRow(CircuitBreaker breaker, int sortKey) {
        IntBlock groupKeyBlock = blockFactory.newIntBlockBuilder(1).appendInt(0).build();
        IntBlock keyBlock = blockFactory.newIntBlockBuilder(1).appendInt(sortKey).build();
        IntBlock valueBlock = blockFactory.newIntBlockBuilder(1).appendInt(sortKey * 2).build();
        TopNRow row = new TopNRow(breaker, bigArrays.recycler(), 32, 64);
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

    private void addRows(GroupedQueue queue, int groupKey, int... values) {
        for (int value : values) {
            addRow(queue, groupKey, value);
        }
    }

    private void addRow(GroupedQueue queue, int groupKey, int value) {
        TopNRow row = createRow(breaker, value);
        Releasables.close(queue.getOrCreateQueue(groupKey).addRow(row));
    }

    private static final List<TopNOperator.SortOrder> SORT_ORDERS = List.of(new TopNOperator.SortOrder(1, true, false));

    private void assertQueueContents(GroupedQueue queue, List<Integer> expectedSortKeys) {
        assertThat(queue.size(), equalTo(expectedSortKeys.size()));
        List<TopNRow> actual = queue.popAll();
        for (int i = 0; i < expectedSortKeys.size(); i++) {
            int sortKey = expectedSortKeys.get(i);
            assertRowValues(actual.get(i), sortKey, sortKey * 2);
        }
        Releasables.close(actual);
    }

    private static void assertRowValues(TopNRow row, int expectedSortKey, int expectedValue) {
        PagedBytesCursor keysCursor = row.keys.view().cursor(new PagedBytesCursor());
        keysCursor.readByte(); // skip null sentinel
        assertThat(TopNEncoder.DEFAULT_SORTABLE.decodeInt(keysCursor), equalTo(expectedSortKey));

        PagedBytesCursor cursor = row.values.view().cursor(new PagedBytesCursor());
        assertThat(cursor.readVInt(), equalTo(1));
        TopNEncoder.DEFAULT_UNSORTABLE.decodeInt(cursor);
        assertThat(cursor.readVInt(), equalTo(1));
        assertThat(cursor.readVInt(), equalTo(1));
        assertThat(TopNEncoder.DEFAULT_UNSORTABLE.decodeInt(cursor), equalTo(expectedValue));
    }
}
