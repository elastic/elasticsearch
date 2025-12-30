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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class UngroupedRowTests extends ESTestCase {
    private final CircuitBreaker breaker = new NoopCircuitBreaker(CircuitBreaker.REQUEST);

    public void testRamBytesUsedEmpty() {
        Row row = new UngroupedRow(breaker, sortOrders(), 0, 0);
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    public void testRamBytesUsedSmall() {
        Row row = new UngroupedRow(new NoopCircuitBreaker(CircuitBreaker.REQUEST), sortOrders(), 0, 0);
        row.keys().append(randomByte());
        row.values().append(randomByte());
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    public void testRamBytesUsedBig() {
        Row row = new UngroupedRow(new NoopCircuitBreaker(CircuitBreaker.REQUEST), sortOrders(), 0, 0);
        for (int i = 0; i < 10000; i++) {
            row.keys().append(randomByte());
            row.values().append(randomByte());
        }
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    public void testRamBytesUsedPreAllocated() {
        Row row = new UngroupedRow(new NoopCircuitBreaker(CircuitBreaker.REQUEST), sortOrders(), 64, 128);
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    public void testCloseReleasesAllTestsNoPreAllocation() throws Exception {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(1));
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        var row = new UngroupedRow(breaker, sortOrders(), 0, 0);
        row.close();
        MockBigArrays.ensureAllArraysAreReleased();
        assertThat("Not all memory was released", breaker.getUsed(), equalTo(0L));
    }

    public void testCloseReleasesAllTestsWithPreAllocation() throws Exception {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(1));
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        var row = new UngroupedRow(breaker, sortOrders(), 16, 32);
        row.close();
        MockBigArrays.ensureAllArraysAreReleased();
        assertThat("Not all memory was released", breaker.getUsed(), equalTo(0L));
    }

    private static List<TopNOperator.SortOrder> sortOrders() {
        return List.of(
            new TopNOperator.SortOrder(randomNonNegativeInt(), randomBoolean(), randomBoolean()),
            new TopNOperator.SortOrder(randomNonNegativeInt(), randomBoolean(), randomBoolean())
        );
    }

    private long expectedRamBytesUsed(Row row) {
        long expected = RamUsageTester.ramUsed(row);
        if (row.values().bytes().length == 0) {
            // We double count the shared empty array for empty rows. This overcounting is *fine*, but throws off the test.
            expected += RamUsageTester.ramUsed(new byte[0]);
        }
        // The breaker is shared infrastructure so we don't count it but RamUsageTester does
        expected -= RamUsageTester.ramUsed(breaker);
        expected -= RamUsageTester.ramUsed("topn");
        // the sort orders are shared
        expected -= RamUsageTester.ramUsed(sortOrders());
        return expected;
    }
}
