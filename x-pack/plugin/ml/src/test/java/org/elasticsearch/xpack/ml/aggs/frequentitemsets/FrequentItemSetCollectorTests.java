/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.lucene.util.LongsRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.FrequentItemSetCollector.FrequentItemSetPriorityQueue;
import org.junit.After;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class FrequentItemSetCollectorTests extends ESTestCase {

    static BigArrays mockBigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    private HashBasedTransactionStore transactionStore = null;

    @After
    public void closeReleasables() throws IOException {
        Releasables.close(transactionStore);
    }

    public void testQueue() {
        transactionStore = new HashBasedTransactionStore(mockBigArrays());

        FrequentItemSetCollector collector = new FrequentItemSetCollector(transactionStore, 5, Long.MAX_VALUE);

        assertEquals(Long.MAX_VALUE, collector.add(longsRef(1L, 2L, 3L, 4L), 10L));
        assertEquals(Long.MAX_VALUE, collector.add(longsRef(5L, 6L, 7L, 8L), 11L));
        assertEquals(Long.MAX_VALUE, collector.add(longsRef(11L, 12L, 13L, 14L), 9L));
        assertEquals(Long.MAX_VALUE, collector.add(longsRef(21L, 2L, 3L, 4L), 13L));

        // queue should be full, drop weakest element
        assertEquals(9L, collector.add(longsRef(31L, 2L, 3L, 4L), 14L));
        assertEquals(10L, collector.add(longsRef(41L, 2L, 3L, 4L), 15L));
        assertEquals(11L, collector.add(longsRef(51L, 2L, 3L, 4L), 16L));

        // check that internal data has been removed as well
        assertEquals(5, collector.getFrequentItemsByCount().size());

        // fill slots with same doc count
        assertEquals(13L, collector.add(longsRef(61L, 2L, 3L, 4L), 20L));
        assertEquals(14L, collector.add(longsRef(71L, 2L, 3L, 4L), 20L));
        assertEquals(15L, collector.add(longsRef(81L, 2L, 3L, 4L), 20L));
        assertEquals(16L, collector.add(longsRef(91L, 2L, 3L, 4L), 20L));
        assertEquals(20L, collector.add(longsRef(101L, 2L, 3L, 4L), 20L));

        // check that internal map has only 1 key
        assertEquals(1, collector.getFrequentItemsByCount().size());

        // ignore set below current weakest one
        assertEquals(20L, collector.add(longsRef(111L, 2L, 3L, 4L), 1L));

        FrequentItemSetPriorityQueue queue = collector.getQueue();

        assertThat(queue.pop().getItems().longs, equalTo(new long[] { 61L, 2L, 3L, 4L }));
        assertThat(queue.pop().getItems().longs, equalTo(new long[] { 71L, 2L, 3L, 4L }));
        assertThat(queue.pop().getItems().longs, equalTo(new long[] { 81L, 2L, 3L, 4L }));
        assertThat(queue.pop().getItems().longs, equalTo(new long[] { 91L, 2L, 3L, 4L }));
        assertThat(queue.pop().getItems().longs, equalTo(new long[] { 101L, 2L, 3L, 4L }));

        assertEquals(0, collector.size());
    }

    public void testClosedSetSkipping() {
        transactionStore = new HashBasedTransactionStore(mockBigArrays());

        FrequentItemSetCollector collector = new FrequentItemSetCollector(transactionStore, 5, Long.MAX_VALUE);

        assertEquals(Long.MAX_VALUE, collector.add(longsRef(1L, 2L, 3L, 4L), 10L));
        assertEquals(Long.MAX_VALUE, collector.add(longsRef(5L, 6L, 7L, 8L), 11L));
        assertEquals(Long.MAX_VALUE, collector.add(longsRef(11L, 12L, 13L, 14L), 12L));
        assertEquals(Long.MAX_VALUE, collector.add(longsRef(21L, 2L, 3L, 4L), 13L));

        // add a subset of the 1st entry, it should be ignored
        assertEquals(Long.MAX_VALUE, collector.add(longsRef(1L, 2L, 3L), 10L));

        // fill slots with same doc count
        assertEquals(10L, collector.add(longsRef(61L, 2L, 3L, 4L), 20L));
        assertEquals(11L, collector.add(longsRef(71L, 2L, 3L, 4L), 20L));
        assertEquals(12L, collector.add(longsRef(81L, 2L, 3L, 4L), 20L));
        assertEquals(13L, collector.add(longsRef(91L, 2L, 3L, 4L), 20L));

        // add a subset of an entry, it should be ignored
        assertEquals(13L, collector.add(longsRef(81L, 2L, 4L), 20L));

        FrequentItemSetPriorityQueue queue = collector.getQueue();

        assertThat(queue.pop().getItems().longs, equalTo(new long[] { 21L, 2L, 3L, 4L }));
        assertThat(queue.pop().getItems().longs, equalTo(new long[] { 61L, 2L, 3L, 4L }));
        assertThat(queue.pop().getItems().longs, equalTo(new long[] { 71L, 2L, 3L, 4L }));
        assertThat(queue.pop().getItems().longs, equalTo(new long[] { 81L, 2L, 3L, 4L }));
        assertThat(queue.pop().getItems().longs, equalTo(new long[] { 91L, 2L, 3L, 4L }));

        assertEquals(0, collector.size());
    }

    public void testCopyOnAdd() {
        transactionStore = new HashBasedTransactionStore(mockBigArrays());

        FrequentItemSetCollector collector = new FrequentItemSetCollector(transactionStore, 5, Long.MAX_VALUE);
        LongsRef itemSet = longsRef(1L, 2L, 3L, 4L, 5L);

        assertEquals(Long.MAX_VALUE, collector.add(itemSet, 10L));
        itemSet.longs[0] = 42L;
        itemSet.longs[4] = 42L;

        FrequentItemSetPriorityQueue queue = collector.getQueue();

        assertThat(queue.pop().getItems().longs, equalTo(new long[] { 1L, 2L, 3L, 4L, 5L }));
    }

    public void testLargerItemSetsPreference() {
        transactionStore = new HashBasedTransactionStore(mockBigArrays());

        FrequentItemSetCollector collector = new FrequentItemSetCollector(transactionStore, 5, Long.MAX_VALUE);

        assertEquals(Long.MAX_VALUE, collector.add(longsRef(1L, 2L, 3L, 4L), 10L));
        assertEquals(Long.MAX_VALUE, collector.add(longsRef(5L, 6L, 7L, 8L), 11L));
        assertEquals(Long.MAX_VALUE, collector.add(longsRef(11L, 12L, 13L, 14L), 9L));
        assertEquals(Long.MAX_VALUE, collector.add(longsRef(21L, 2L, 3L, 4L), 13L));

        // queue should be full, drop weakest element
        assertEquals(9L, collector.add(longsRef(31L, 2L, 3L, 4L), 14L));

        assertEquals(9L, collector.getLastSet().getDocCount());
        assertEquals(4, collector.getLastSet().size());

        // ignore set with same doc count but fewer items
        assertEquals(9L, collector.add(longsRef(22L, 23L, 24L), 9L));

        assertEquals(9L, collector.getLastSet().getDocCount());
        assertEquals(4, collector.getLastSet().size());

        // take set with same doc count but more items
        assertEquals(9L, collector.add(longsRef(25L, 26L, 27L, 28L, 29L), 9L));

        assertEquals(9L, collector.getLastSet().getDocCount());
        assertEquals(5, collector.getLastSet().size());

        FrequentItemSetPriorityQueue queue = collector.getQueue();

        assertThat(queue.pop().getItems().longs, equalTo(new long[] { 25L, 26L, 27L, 28L, 29L }));
        assertThat(queue.pop().getItems().longs, equalTo(new long[] { 1L, 2L, 3L, 4L }));
        assertThat(queue.pop().getItems().longs, equalTo(new long[] { 5L, 6L, 7L, 8L }));
        assertThat(queue.pop().getItems().longs, equalTo(new long[] { 21L, 2L, 3L, 4L }));
        assertThat(queue.pop().getItems().longs, equalTo(new long[] { 31L, 2L, 3L, 4L }));

        assertEquals(0, collector.size());
    }

    private static LongsRef longsRef(long l1, long l2, long l3) {
        return new LongsRef(new long[] { l1, l2, l3 }, 0, 3);
    }

    private static LongsRef longsRef(long l1, long l2, long l3, long l4) {
        return new LongsRef(new long[] { l1, l2, l3, l4 }, 0, 4);
    }

    private static LongsRef longsRef(long l1, long l2, long l3, long l4, long l5) {
        return new LongsRef(new long[] { l1, l2, l3, l4, l5 }, 0, 5);
    }
}
