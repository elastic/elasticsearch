/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class TopNRowTests extends ESTestCase {
    private final CircuitBreaker breaker = new NoopCircuitBreaker(CircuitBreaker.REQUEST);
    private final PageCacheRecycler recycler = PageCacheRecycler.NON_RECYCLING_INSTANCE;

    public void testRamBytesUsedEmpty() {
        TopNRow row = new TopNRow(breaker, recycler, 0, 0);
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    public void testRamBytesUsedSmall() {
        TopNRow row = new TopNRow(new NoopCircuitBreaker(CircuitBreaker.REQUEST), recycler, 0, 0);
        row.keys.append(randomByte());
        row.values.append(randomByte());
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    /**
     * Tests the row size as measured by MAT in a heap dump from a failing test. All the
     * magic numbers come from the heap dump. They came from the running {@link TopNOperator}'s
     * size estimates from previous rows.
     */
    public void testFromHeapDump1() {
        TopNRow row = new TopNRow(new NoopCircuitBreaker(CircuitBreaker.REQUEST), recycler, 56, 24);
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    /**
     * Tests the row size as measured by MAT in a heap dump from a failing test. All the
     * magic numbers come from the heap dump. They came from the running {@link TopNOperator}'s
     * size estimates from previous rows.
     */
    public void testFromHeapDump2() {
        TopNRow row = new TopNRow(new NoopCircuitBreaker(CircuitBreaker.REQUEST), recycler, 1160, 1_153_096);
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    public void testRamBytesUsedBig() {
        TopNRow row = new TopNRow(new NoopCircuitBreaker(CircuitBreaker.REQUEST), recycler, 0, 0);
        for (int i = 0; i < 10000; i++) {
            row.keys.append(randomByte());
            row.values.append(randomByte());
        }
        // PagedBytesBuilder in paged mode has a small per-page undercount: Recycler.V wrappers
        // are counted by RamUsageTester but not by ramBytesUsed(). Accept small positive slack.
        assertThat(row.ramBytesUsed(), lessThanOrEqualTo(expectedRamBytesUsed(row)));
        assertThat(expectedRamBytesUsed(row) - row.ramBytesUsed(), lessThanOrEqualTo(100L));
    }

    public void testRamBytesUsedPreAllocated() {
        TopNRow row = new TopNRow(new NoopCircuitBreaker(CircuitBreaker.REQUEST), recycler, 64, 128);
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    private long expectedRamBytesUsed(TopNRow row) {
        long expected = RamUsageTester.ramUsed(row);
        // The breaker and recycler are shared infrastructure so we don't count them but RamUsageTester does
        expected -= RamUsageTester.ramUsed(breaker);
        expected -= RamUsageTester.ramUsed("topn");
        expected -= RamUsageTester.ramUsed(recycler);
        return expected;
    }
}
