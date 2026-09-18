/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Pins the contract {@link InformationalWarningBudget} exists to enforce: across all callers sharing one
 * budget it admits at most {@code cap} distinct payloads plus a single overflow marker, deduplicates by
 * value, spends no cap slot on a relayed per-collector overflow marker, and stays within that ceiling
 * under concurrent access from many threads (the multi-driver, multi-segment fan-out it guards).
 */
public class InformationalWarningBudgetTests extends ESTestCase {

    public void testAdmitsUpToCapDistinctThenOneOverflowMarker() {
        int cap = 20;
        InformationalWarningBudget budget = new InformationalWarningBudget(cap);
        List<String> survivors = new ArrayList<>();
        for (int i = 0; i < cap + 15; i++) {
            String out = budget.accept("warning-" + i);
            if (out != null) {
                survivors.add(out);
            }
        }
        // cap distinct payloads plus exactly one overflow marker.
        assertThat(survivors.size(), equalTo(cap + 1));
        assertThat(survivors.subList(0, cap), equalTo(distinctInputs(cap)));
        assertThat(survivors.get(cap), equalTo(SkipWarnings.overflowMessage()));
    }

    public void testDuplicatesAreDropped() {
        InformationalWarningBudget budget = new InformationalWarningBudget(20);
        assertThat(budget.accept("same"), equalTo("same"));
        assertNull("a repeated value costs no slot and is dropped", budget.accept("same"));
        assertNull(budget.accept("same"));
        assertThat("a distinct value still admits", budget.accept("other"), equalTo("other"));
    }

    public void testOverflowMarkerEmittedExactlyOnce() {
        int cap = 5;
        InformationalWarningBudget budget = new InformationalWarningBudget(cap);
        int markers = 0;
        for (int i = 0; i < cap + 50; i++) {
            if (SkipWarnings.overflowMessage().equals(budget.accept("w-" + i))) {
                markers++;
            }
        }
        assertThat(markers, equalTo(1));
    }

    public void testLocalOverflowMarkerDoesNotSpendACapSlot() {
        int cap = 5;
        InformationalWarningBudget budget = new InformationalWarningBudget(cap);
        // A chunk that hit its own per-instance MAX_ADDED_WARNINGS cap relays this exact text; relaying
        // it must not cost one of the cap slots reserved for real summary/detail content.
        assertThat(budget.accept(SkipWarnings.overflowMessage()), equalTo(SkipWarnings.overflowMessage()));
        // A second, independent chunk hitting its own cap relays the identical text; it costs nothing
        // further (already seen) rather than spending a second slot.
        assertNull(budget.accept(SkipWarnings.overflowMessage()));
        // All `cap` real slots are still available afterward.
        List<String> survivors = new ArrayList<>();
        for (int i = 0; i < cap; i++) {
            String out = budget.accept("warning-" + i);
            if (out != null) {
                survivors.add(out);
            }
        }
        assertThat(survivors, equalTo(distinctInputs(cap)));
        // The cap is now exhausted by real content alone; the next distinct warning triggers overflow.
        assertThat(budget.accept("one-too-many"), equalTo(SkipWarnings.overflowMessage()));
    }

    public void testConstructorRejectsNonPositiveCap() {
        expectThrows(IllegalArgumentException.class, () -> new InformationalWarningBudget(0));
        expectThrows(IllegalArgumentException.class, () -> new InformationalWarningBudget(-1));
    }

    public void testAcceptRejectsNull() {
        expectThrows(NullPointerException.class, () -> new InformationalWarningBudget(20).accept(null));
    }

    public void testConcurrentAcceptAdmitsAtMostCapPlusOne() throws Exception {
        int cap = 20;
        InformationalWarningBudget budget = new InformationalWarningBudget(cap);
        int threads = 8;
        int perThread = 50;
        Set<String> survivors = ConcurrentHashMap.newKeySet();
        CyclicBarrier barrier = new CyclicBarrier(threads);
        List<Thread> workers = new ArrayList<>(threads);
        for (int t = 0; t < threads; t++) {
            final int base = t * perThread;
            Thread worker = new Thread(() -> {
                safeAwait(barrier);
                for (int i = 0; i < perThread; i++) {
                    String out = budget.accept("warning-" + (base + i));
                    if (out != null) {
                        survivors.add(out);
                    }
                }
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        // At most cap distinct payloads plus the single overflow marker survive, regardless of races.
        assertThat(survivors.size(), lessThanOrEqualTo(cap + 1));
        assertTrue("the overflow marker must be emitted once the cap is exceeded", survivors.contains(SkipWarnings.overflowMessage()));
    }

    private static List<String> distinctInputs(int count) {
        List<String> expected = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            expected.add("warning-" + i);
        }
        return expected;
    }
}
