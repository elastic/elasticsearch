/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class SharedNumericThresholdTests extends ComputeTestCase {
    public void testAscendingThresholdTightensToMinimumOffer() {
        try (SharedNumericThreshold threshold = new SharedNumericThreshold.Supplier(true, false).get()) {
            assertFalse(threshold.dominates(Long.MAX_VALUE, Long.MAX_VALUE));
            threshold.offer(10);
            threshold.offer(20);
            threshold.offer(5);
            assertThat(threshold.current(), equalTo(5L));
            assertFalse(threshold.dominates(5, 9));
            assertTrue(threshold.dominates(6, 9));
        }
    }

    public void testDescendingThresholdTightensToMaximumOffer() {
        try (SharedNumericThreshold threshold = new SharedNumericThreshold.Supplier(false, false).get()) {
            assertFalse(threshold.dominates(Long.MIN_VALUE, Long.MIN_VALUE));
            threshold.offer(10);
            threshold.offer(20);
            threshold.offer(5);
            assertThat(threshold.current(), equalTo(20L));
            assertFalse(threshold.dominates(11, 20));
            assertTrue(threshold.dominates(11, 19));
        }
    }

    public void testConcurrentOffers() throws Exception {
        boolean ascending = randomBoolean();
        SharedNumericThreshold.Supplier supplier = new SharedNumericThreshold.Supplier(ascending, randomBoolean());
        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<Future<?>> futures = new ArrayList<>();
        long expected = ascending ? Long.MAX_VALUE : Long.MIN_VALUE;
        try (SharedNumericThreshold threshold = supplier.get()) {
            for (int t = 0; t < 4; t++) {
                long[] values = randomLongs().limit(1_000).toArray();
                for (long value : values) {
                    expected = ascending ? Math.min(expected, value) : Math.max(expected, value);
                }
                futures.add(executor.submit(() -> {
                    for (long value : values) {
                        threshold.offer(value);
                    }
                }));
            }
            for (Future<?> future : futures) {
                future.get(1, TimeUnit.MINUTES);
            }
            assertThat(threshold.current(), equalTo(expected));
            assertThat(threshold.offeredCount(), equalTo(4_000L));
        } finally {
            executor.shutdown();
        }
    }

    public void testMarkNoFurtherCandidatesCollapsesThreshold() {
        try (
            SharedNumericThreshold asc = new SharedNumericThreshold.Supplier(true, true).get();
            SharedNumericThreshold desc = new SharedNumericThreshold.Supplier(false, true).get()
        ) {
            asc.markNoFurtherCandidates();
            desc.markNoFurtherCandidates();
            assertTrue(asc.noFurtherCandidates());
            assertTrue(desc.noFurtherCandidates());
            assertThat(asc.current(), equalTo(Long.MIN_VALUE));
            assertThat(desc.current(), equalTo(Long.MAX_VALUE));
            assertTrue(asc.dominates(0, 1));
            assertTrue(desc.dominates(0, 1));
        }
    }

    public void testSupplierSharesUntilLastCloseThenRebuilds() {
        SharedNumericThreshold.Supplier supplier = new SharedNumericThreshold.Supplier(true, randomBoolean());
        SharedNumericThreshold first = supplier.get();
        SharedNumericThreshold second = supplier.get();
        assertThat(second, sameInstance(first));
        first.close();
        second.close();
        SharedNumericThreshold rebuilt = supplier.get();
        try {
            assertThat(rebuilt, not(sameInstance(first)));
        } finally {
            rebuilt.close();
        }
    }
}
