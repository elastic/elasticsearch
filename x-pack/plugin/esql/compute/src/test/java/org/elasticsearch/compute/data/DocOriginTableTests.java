/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class DocOriginTableTests extends ESTestCase {
    public void testNumbersEachOriginOnce() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        try (DocOriginTable table = new DocOriginTable(breaker)) {
            DocOrigin a = RandomBlock.randomDocOrigin();
            DocOrigin b = randomValueOtherThan(a, RandomBlock::randomDocOrigin);
            int ordinalA = table.ordinal(a);
            int ordinalB = table.ordinal(b);
            assertThat(table.ordinal(a), equalTo(ordinalA));
            assertThat(table.get(ordinalA), equalTo(a));
            assertThat(table.get(ordinalB), equalTo(b));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testReturnsItsBytesWhenClosed() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        DocOriginTable table = new DocOriginTable(breaker);
        DocOrigin known = RandomBlock.randomDocOrigin();
        table.ordinal(known);
        for (int i = 0; i < 10; i++) {
            table.ordinal(RandomBlock.randomDocOrigin());
        }
        assertThat(table.ramBytesUsed(), greaterThan(0L));
        assertThat(breaker.getUsed(), equalTo(table.ramBytesUsed()));

        table.close();
        assertThat(breaker.getUsed(), equalTo(0L));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> table.ordinal(RandomBlock.randomDocOrigin()));
        assertThat(e.getMessage(), equalTo("the document origin table is closed"));
        assertThat("known origins keep their number", table.ordinal(known), equalTo(0));
        assertThat(breaker.getUsed(), equalTo(0L));
        table.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testBreaks() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(100));
        try (DocOriginTable table = new DocOriginTable(breaker)) {
            expectThrows(CircuitBreakingException.class, () -> table.ordinal(RandomBlock.randomDocOrigin()));
            assertThat(table.ramBytesUsed(), equalTo(0L));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * The drivers of a query share its table.
     */
    public void testConcurrentNumbering() throws Exception {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(10));
        List<DocOrigin> origins = RandomBlock.randomDocOrigins(50);
        try (DocOriginTable table = new DocOriginTable(breaker)) {
            int threadCount = between(2, 8);
            CyclicBarrier start = new CyclicBarrier(threadCount);
            List<Thread> threads = new ArrayList<>();
            List<int[]> ordinals = new ArrayList<>();
            for (int t = 0; t < threadCount; t++) {
                int[] seen = new int[origins.size()];
                ordinals.add(seen);
                List<Integer> order = new ArrayList<>();
                for (int i = 0; i < origins.size(); i++) {
                    order.add(i);
                }
                Collections.shuffle(order, random());
                Thread thread = new Thread(() -> {
                    safeAwait(start);
                    for (int i : order) {
                        seen[i] = table.ordinal(origins.get(i));
                    }
                });
                threads.add(thread);
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
            for (int i = 0; i < origins.size(); i++) {
                int ordinal = ordinals.get(0)[i];
                for (int[] seen : ordinals) {
                    assertThat("every thread sees one number per origin", seen[i], equalTo(ordinal));
                }
                assertThat(table.get(ordinal), equalTo(origins.get(i)));
            }
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }
}
