/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests {@link AbstractBlockRefCounted}'s non-atomic-by-default / promote-to-atomic design directly,
 * independent of any concrete {@link Block} or {@link Vector}.
 */
public class AbstractBlockRefCountedTests extends ESTestCase {

    /** {@link AbstractBlockRefCounted} is abstract; this is the minimal concrete shape needed to exercise it. */
    private static class TestRefCounted extends AbstractBlockRefCounted {
        private final AtomicInteger closeCount = new AtomicInteger();

        @Override
        protected void closeInternal() {
            closeCount.incrementAndGet();
        }
    }

    public void testClosesOnFinalDecRefBeforePromotion() {
        var counted = new TestRefCounted();
        counted.incRef();
        assertFalse(counted.decRef());
        assertThat(counted.closeCount.get(), equalTo(0));
        assertTrue(counted.decRef());
        assertThat(counted.closeCount.get(), equalTo(1));
    }

    public void testPromotionPreservesRefCount() {
        var counted = new TestRefCounted();
        int extraRefs = randomIntBetween(1, 20);
        for (int i = 0; i < extraRefs; i++) {
            counted.incRef();
        }
        counted.makeRefCountsThreadSafe();

        for (int i = 0; i < extraRefs; i++) {
            assertFalse(counted.decRef());
        }
        assertThat(counted.closeCount.get(), equalTo(0));

        assertTrue(counted.decRef());
        assertThat(counted.closeCount.get(), equalTo(1));
    }

    public void testMakeRefCountsAtomicIsIdempotent() {
        var counted = new TestRefCounted();
        counted.incRef();
        counted.makeRefCountsThreadSafe();
        counted.makeRefCountsThreadSafe(); // must be a no-op -- not reset or re-seed the count

        assertFalse(counted.decRef());
        assertThat(counted.closeCount.get(), equalTo(0));
        assertTrue(counted.decRef());
        assertThat(counted.closeCount.get(), equalTo(1));
    }

    public void testUnpromotedIncRefAfterReleaseThrows() {
        var counted = new TestRefCounted();
        assertTrue(counted.decRef());
        assertFalse(counted.tryIncRef());
        expectThrows(IllegalStateException.class, counted::incRef);
    }

    /**
     * The whole point of promotion: once atomic, concurrent decRef from multiple threads racing to zero
     * must close exactly once, matching elastic/elasticsearch#152904.
     */
    public void testPromotedRefCountIsThreadSafe() {
        var counted = new TestRefCounted();
        int refs = randomIntBetween(2, 20);
        for (int i = 0; i < refs - 1; i++) {
            counted.incRef();
        }
        counted.makeRefCountsThreadSafe();

        AtomicInteger closingDecRefs = new AtomicInteger();
        startInParallel(refs, i -> {
            if (counted.decRef()) {
                closingDecRefs.incrementAndGet();
            }
        });

        assertThat(closingDecRefs.get(), equalTo(1));
        assertThat(counted.closeCount.get(), equalTo(1));
        assertFalse(counted.hasReferences());
    }
}
