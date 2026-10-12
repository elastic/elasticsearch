/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.sameInstance;

public class SuppressedFailuresTests extends ESTestCase {

    public void testAttachesADistinctFailure() {
        Exception carrier = new RuntimeException("carrier");
        Exception other = new RuntimeException("other");
        assertTrue(SuppressedFailures.attach(carrier, other));
        assertThat(carrier.getSuppressed(), arrayWithSize(1));
        assertThat(carrier.getSuppressed()[0], sameInstance(other));
    }

    public void testSkipsNullAndSelf() {
        Exception carrier = new RuntimeException("carrier");
        assertFalse(SuppressedFailures.attach(carrier, null));
        assertFalse(SuppressedFailures.attach(carrier, carrier));
        assertThat(carrier.getSuppressed(), arrayWithSize(0));
    }

    public void testSkipsAFailureAttachedTwice() {
        Exception carrier = new RuntimeException("carrier");
        Exception other = new RuntimeException("other");
        assertTrue(SuppressedFailures.attach(carrier, other));
        assertFalse(SuppressedFailures.attach(carrier, other));
        assertThat(carrier.getSuppressed(), arrayWithSize(1));
    }

    public void testSkipsAFailureAlreadyReachableThroughACause() {
        Exception root = new RuntimeException("root");
        Exception carrier = new RuntimeException("carrier", root);
        assertFalse(SuppressedFailures.attach(carrier, root));
        assertThat(carrier.getSuppressed(), arrayWithSize(0));
    }

    public void testSkipsAFailureThatReachesTheCarrier() {
        Exception a = new RuntimeException("a");
        Exception b = new RuntimeException("b");
        assertTrue(SuppressedFailures.attach(a, b));
        assertFalse("attaching a to b would close a loop", SuppressedFailures.attach(b, a));
        Exception wrapper = new RuntimeException("wrapper", a);
        assertFalse("wrapper reaches b through its cause, so attaching it to b would loop", SuppressedFailures.attach(b, wrapper));
        assertThat(b.getSuppressed(), arrayWithSize(0));
        assertNoRepeats(a);
        assertNoRepeats(wrapper);
    }

    public void testReachesIsSafeOnAGraphThatAlreadyLoops() {
        Exception a = new RuntimeException("a");
        Exception b = new RuntimeException("b");
        a.addSuppressed(b);
        b.addSuppressed(a);
        assertTrue(SuppressedFailures.reaches(a, b));
        assertTrue(SuppressedFailures.reaches(b, a));
        assertFalse(SuppressedFailures.reaches(a, new RuntimeException("unrelated")));
    }

    /**
     * Many threads attach the same shared failures to one another in random orders, the way several collectors would
     * when the same instances are live in many splits. Whatever the interleaving, no failure may end up reachable
     * from itself.
     */
    public void testConcurrentAttachesInOppositeOrdersNeverLoop() throws Exception {
        for (int round = 0; round < 20; round++) {
            List<Exception> shared = new ArrayList<>();
            int sharedCount = between(2, 6);
            for (int i = 0; i < sharedCount; i++) {
                shared.add(new RuntimeException("shared-" + i));
            }
            Thread[] threads = new Thread[between(2, 8)];
            CyclicBarrier barrier = new CyclicBarrier(threads.length);
            for (int t = 0; t < threads.length; t++) {
                threads[t] = new Thread(() -> {
                    try {
                        barrier.await(10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                    for (int i = 0; i < 20; i++) {
                        SuppressedFailures.attach(randomFrom(shared), randomFrom(shared));
                    }
                });
                threads[t].start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
            for (Exception e : shared) {
                for (Throwable suppressed : e.getSuppressed()) {
                    assertFalse("loop through " + e.getMessage(), SuppressedFailures.reaches(suppressed, e));
                }
            }
        }
    }

    /**
     * Asserts that no exception is reachable from {@code root} along more than one path through cause and suppressed
     * links, which also rules out loops.
     */
    static void assertNoRepeats(Throwable root) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        assertNoRepeats(root, seen);
    }

    private static void assertNoRepeats(Throwable current, Set<Throwable> seen) {
        assertTrue("reached [" + current + "] more than once", seen.add(current));
        if (current.getCause() != null) {
            assertNoRepeats(current.getCause(), seen);
        }
        for (Throwable suppressed : current.getSuppressed()) {
            assertNoRepeats(suppressed, seen);
        }
    }
}
