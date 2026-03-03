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
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class GroupedRowTests extends ESTestCase {
    private final CircuitBreaker breaker = new NoopCircuitBreaker(CircuitBreaker.REQUEST);

    public void testCloseReleasesAllTestsNoPreAllocation() throws Exception {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(1));
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        var row = new GroupedRow(breaker, 0, 0);
        row.close();
        MockBigArrays.ensureAllArraysAreReleased();
        assertThat("Not all memory was released", breaker.getUsed(), equalTo(0L));
    }

    public void testCloseReleasesAllTestsWithPreAllocation() throws Exception {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(1));
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        var row = new GroupedRow(breaker, 16, 32);
        row.close();
        MockBigArrays.ensureAllArraysAreReleased();
        assertThat("Not all memory was released", breaker.getUsed(), equalTo(0L));
    }

    public void testRamBytesUsedEmpty() {
        var row = new GroupedRow(breaker, 0, 0);
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    public void testRamBytesUsedSmall() {
        var row = new GroupedRow(breaker, 0, 0);
        row.keys().append(randomByte());
        row.values().append(randomByte());
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    public void testRamBytesUsedBig() {
        var row = new GroupedRow(breaker, 0, 0);
        for (int i = 0; i < 10000; i++) {
            row.keys().append(randomByte());
            row.values().append(randomByte());
        }
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    public void testRamBytesUsedPreAllocated() {
        var row = new GroupedRow(breaker, 64, 128);
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    private long expectedRamBytesUsed(GroupedRow row) {
        var expected = RamUsageTester.ramUsed(row);
        expected -= RamUsageTester.ramUsed(breaker);
        expected -= sharedRowBytes();
        expected += undercountedBytesForRow(row);
        return expected;
    }

    private static long sharedRowBytes() {
        return RamUsageTester.ramUsed("topn");
    }

    static long undercountedBytesForRow(GroupedRow row) {
        return emptyByteArrayOverhead(row.values());
    }

    private static long emptyByteArrayOverhead(BreakingBytesRefBuilder builder) {
        return builder.bytes().length == 0 ? RamUsageTester.ramUsed(new byte[0]) : 0L;
    }
}
