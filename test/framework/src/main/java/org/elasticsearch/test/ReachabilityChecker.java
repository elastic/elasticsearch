/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Utility class for checking that objects become unreachable when expected.
 *
 * Registered objects become unreachable only when the GC notices them. This class attempts to trigger the GC at various points but does not
 * guarantee a full GC. If we're unlucky, some of these objects may move to old enough heap generations that they are not subject to our GC
 * attempts, resulting in test flakiness. Time will tell whether this is a problem in practice or not; if it is then we'll have to introduce
 * some extra measures here.
 */
public class ReachabilityChecker {

    private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private final Queue<Registered> references = ConcurrentCollections.newQueue();

    public ReachabilityChecker() {
        memoryMXBean.gc();
    }

    /**
     * Register the given target object for reachability checks.
     *
     * @return the given target object.
     */
    public <T> T register(T target) {
        var referenceQueue = new ReferenceQueue<>();
        references.add(
            new Registered(target.toString(), new PhantomReference<>(Objects.requireNonNull(target), referenceQueue), referenceQueue)
        );
        return target;
    }

    /**
     * Ensure that all registered objects have become unreachable.
     */
    public void ensureUnreachable() {
        ensureUnreachable(TimeUnit.SECONDS.toMillis(10));
    }

    void ensureUnreachable(long timeoutMillis) {
        Registered registered;
        while ((registered = references.poll()) != null) {
            registered.assertReferenceEnqueuedForCollection(memoryMXBean, timeoutMillis);
        }
    }

    /**
     * From the objects registered since the most recent call to {@link #ensureUnreachable()} (or since the construction of this {@link
     * ReachabilityChecker} if {@link #ensureUnreachable()} has not been called) this method chooses one at random and verifies that it has
     * not yet become unreachable.
     */
    public void checkReachable() {
        if (references.peek() == null) {
            throw new AssertionError("no references registered");
        }

        var target = Randomness.get().nextInt(references.size());
        var iterator = references.iterator();
        for (int i = 0; i < target; i++) {
            assertTrue(iterator.hasNext());
            assertNotNull(iterator.next());
        }

        assertTrue(iterator.hasNext());
        iterator.next().assertReferenceNotEnqueuedForCollection(memoryMXBean);
    }

    private static final class Registered {
        private final String description;
        private final PhantomReference<?> phantomReference;
        private final ReferenceQueue<?> referenceQueue;

        Registered(String description, PhantomReference<?> phantomReference, ReferenceQueue<?> referenceQueue) {
            this.description = description;
            this.phantomReference = phantomReference;
            this.referenceQueue = referenceQueue;
        }

        /**
         * Attempts to trigger the GC repeatedly until the {@link ReferenceQueue} yields a reference.
         */
        public void assertReferenceEnqueuedForCollection(MemoryMXBean memoryMXBean, long timeoutMillis) {
            try {
                final var timeoutAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
                while (true) {
                    memoryMXBean.gc();
                    final var ref = referenceQueue.remove(500);
                    if (ref != null) {
                        ref.clear();
                        return;
                    }
                    assertTrue("still reachable: " + description, System.nanoTime() < timeoutAt);
                    assertNull(phantomReference.get()); // always succeeds, we're just doing this to use the phantomReference for something
                }
            } catch (Exception e) {
                throw new AssertionError("unexpected", e);
            }
        }

        /**
         * Attempts to trigger the GC and verifies that the {@link ReferenceQueue} does not yield a reference.
         */
        public void assertReferenceNotEnqueuedForCollection(MemoryMXBean memoryMXBean) {
            try {
                memoryMXBean.gc();
                assertNull("became unreachable: " + description, referenceQueue.remove(100));
            } catch (Exception e) {
                throw new AssertionError("unexpected", e);
            }
        }
    }
}
