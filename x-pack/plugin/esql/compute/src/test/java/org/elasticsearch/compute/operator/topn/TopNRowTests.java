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

import static org.hamcrest.Matchers.equalTo;

public class TopNRowTests extends ESTestCase {
    private final CircuitBreaker breaker = new NoopCircuitBreaker(CircuitBreaker.REQUEST);

    public void testRamBytesUsedEmpty() {
        TopNOperator.Row row = new TopNOperator.Row(breaker, sortOrders(), 0, 0);
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    public void testRamBytesUsedSmall() {
        TopNOperator.Row row = new TopNOperator.Row(new NoopCircuitBreaker(CircuitBreaker.REQUEST), sortOrders(), 0, 0);
        row.keys.append(randomByte());
        row.values.append(randomByte());
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    public void testRamBytesUsedBig() {
        TopNOperator.Row row = new TopNOperator.Row(new NoopCircuitBreaker(CircuitBreaker.REQUEST), sortOrders(), 0, 0);
        for (int i = 0; i < 10000; i++) {
            row.keys.append(randomByte());
            row.values.append(randomByte());
        }
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    public void testRamBytesUsedPreAllocated() {
        TopNOperator.Row row = new TopNOperator.Row(new NoopCircuitBreaker(CircuitBreaker.REQUEST), sortOrders(), 64, 128);
        assertThat(row.ramBytesUsed(), equalTo(expectedRamBytesUsed(row)));
    }

    private static List<TopNOperator.SortOrder> sortOrders() {
        return List.of(
            new TopNOperator.SortOrder(randomNonNegativeInt(), randomBoolean(), randomBoolean()),
            new TopNOperator.SortOrder(randomNonNegativeInt(), randomBoolean(), randomBoolean())
        );
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
        expected -= RamUsageTester.ramUsed(sortOrders());
        // expected -= RamUsageTester.ramUsed(row.docVector);
        return expected;
    }
}
