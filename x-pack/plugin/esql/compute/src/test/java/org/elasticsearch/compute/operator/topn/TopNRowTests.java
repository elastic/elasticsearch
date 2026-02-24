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

import static org.hamcrest.Matchers.equalTo;

public class TopNRowTests extends ESTestCase {
    private final CircuitBreaker breaker = new NoopCircuitBreaker(CircuitBreaker.REQUEST);

    public void testRamBytesUsedEmpty() {
        TopNOperator.Row row = new TopNOperator.Row(breaker, 0, 0);
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    public void testRamBytesUsedSmall() {
        TopNOperator.Row row = new TopNOperator.Row(new NoopCircuitBreaker(CircuitBreaker.REQUEST), 0, 0);
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
        TopNOperator.Row row = new TopNOperator.Row(new NoopCircuitBreaker(CircuitBreaker.REQUEST), 56, 24);
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
        // 304 was measured debugging a heap dump and we've since shrunk
        assertThat(row.ramBytesUsed(), equalTo(240L));
    }

    /**
     * Tests the row size as measured by MAT in a heap dump from a failing test. All the
     * magic numbers come from the heap dump. They came from the running {@link TopNOperator}'s
     * size estimates from previous rows.
     */
    public void testFromHeapDump2() {
        TopNOperator.Row row = new TopNOperator.Row(new NoopCircuitBreaker(CircuitBreaker.REQUEST), 1160, 1_153_096);
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
        // 1,154,464 is measured debugging a heap dump and we've since shrunk
        assertThat(row.ramBytesUsed(), equalTo(1_154_416L));
    }

    public void testRamBytesUsedBig() {
        TopNOperator.Row row = new TopNOperator.Row(new NoopCircuitBreaker(CircuitBreaker.REQUEST), 0, 0);
        for (int i = 0; i < 10000; i++) {
            row.keys.append(randomByte());
            row.values.append(randomByte());
        }
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    public void testRamBytesUsedPreAllocated() {
        TopNOperator.Row row = new TopNOperator.Row(new NoopCircuitBreaker(CircuitBreaker.REQUEST), 64, 128);
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
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
        return expected;
    }
}
