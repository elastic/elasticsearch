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
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class TopNRowTests extends ESTestCase {
    private final CircuitBreaker breaker = new NoopCircuitBreaker(CircuitBreaker.REQUEST);

    public void testRamBytesUsedEmpty() {
        TopNOperator.Row row = new TopNOperator.Row(breaker, sortOrders(between(1, 10)), 0, 0);
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    public void testRamBytesUsedSmall() {
        TopNOperator.Row row = new TopNOperator.Row(new NoopCircuitBreaker(CircuitBreaker.REQUEST), sortOrders(between(1, 10)), 0, 0);
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
        TopNOperator.Row row = new TopNOperator.Row(new NoopCircuitBreaker(CircuitBreaker.REQUEST), sortOrders(5), 56, 24);
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
        assertThat(row.ramBytesUsed(), equalTo(304L)); // 304 is measured debugging a heap dump
    }

    /**
     * Tests the row size as measured by MAT in a heap dump from a failing test. All the
     * magic numbers come from the heap dump. They came from the running {@link TopNOperator}'s
     * size estimates from previous rows.
     */
    public void testFromHeapDump2() {
        TopNOperator.Row row = new TopNOperator.Row(new NoopCircuitBreaker(CircuitBreaker.REQUEST), sortOrders(1), 1160, 1_153_096);
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
        assertThat(row.ramBytesUsed(), equalTo(1_154_464L)); // 1,154,464 is measured debugging a heap dump
    }

    public void testRamBytesUsedBig() {
        TopNOperator.Row row = new TopNOperator.Row(new NoopCircuitBreaker(CircuitBreaker.REQUEST), sortOrders(between(1, 10)), 0, 0);
        for (int i = 0; i < 10000; i++) {
            row.keys.append(randomByte());
            row.values.append(randomByte());
        }
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    public void testRamBytesUsedPreAllocated() {
        TopNOperator.Row row = new TopNOperator.Row(new NoopCircuitBreaker(CircuitBreaker.REQUEST), sortOrders(between(1, 10)), 64, 128);
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    private static List<TopNOperator.SortOrder> sortOrders(int count) {
        return IntStream.range(0, count)
            .mapToObj(i -> new TopNOperator.SortOrder(randomNonNegativeInt(), randomBoolean(), randomBoolean()))
            .toList();
    }

    private long expectedRamBytesUsed(TopNOperator.Row row) {
        long expected = RamUsageTester.ramUsed(row);
        if (row.values.bytes().length == 0) {
            // We double count the shared empty array for empty rows. This overcounting is *fine*, but throws off the test.
            expected += RamUsageTester.ramUsed(new byte[0]);
        }
        // The breaker is shared infrastructure so we don't count it but RamUsageTester does
        expected -= RamUsageTester.ramUsed(breaker);
        expected -= RamUsageTester.ramUsed("topn");
        // the sort orders are shared
        expected -= RamUsageTester.ramUsed(row.bytesOrder.sortOrders);
        return expected;
    }
}
