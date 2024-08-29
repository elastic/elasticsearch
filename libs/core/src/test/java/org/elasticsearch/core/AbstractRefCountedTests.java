/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.core;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class AbstractRefCountedTests extends ESTestCase {

    public void testRefCount() {
        final RefCounted counted = createRefCounted();

        int incs = randomIntBetween(1, 100);
        for (int i = 0; i < incs; i++) {
            if (randomBoolean()) {
                counted.incRef();
            } else {
                assertTrue(counted.tryIncRef());
            }
            assertTrue(counted.hasReferences());
        }

        for (int i = 0; i < incs; i++) {
            counted.decRef();
            assertTrue(counted.hasReferences());
        }

        counted.incRef();
        counted.decRef();
        for (int i = 0; i < incs; i++) {
            if (randomBoolean()) {
                counted.incRef();
            } else {
                assertTrue(counted.tryIncRef());
            }
            assertTrue(counted.hasReferences());
        }

        for (int i = 0; i < incs; i++) {
            counted.decRef();
            assertTrue(counted.hasReferences());
        }

        counted.decRef();
        assertFalse(counted.tryIncRef());
        assertThat(
            expectThrows(IllegalStateException.class, counted::incRef).getMessage(),
            equalTo(AbstractRefCounted.ALREADY_CLOSED_MESSAGE)
        );
        assertFalse(counted.hasReferences());
    }

    public void testMultiThreaded() throws InterruptedException {
        final AbstractRefCounted counted = createRefCounted();
        startInParallel(randomIntBetween(2, 5), i -> {
            try {
                for (int j = 0; j < 10000; j++) {
                    assertTrue(counted.hasReferences());
                    if (randomBoolean()) {
                        counted.incRef();
                    } else {
                        assertTrue(counted.tryIncRef());
                    }
                    assertTrue(counted.hasReferences());
                    counted.decRef();
                }
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        counted.decRef();
        assertFalse(counted.hasReferences());
        assertThat(
            expectThrows(IllegalStateException.class, counted::incRef).getMessage(),
            equalTo(AbstractRefCounted.ALREADY_CLOSED_MESSAGE)
        );
        assertThat(counted.refCount(), is(0));
        assertFalse(counted.hasReferences());
    }

    public void testToString() {
        assertEquals("refCounted[runnable description]", createRefCounted().toString());
    }

    public void testNullCheck() {
        expectThrows(NullPointerException.class, () -> AbstractRefCounted.of(null));
    }

    private static AbstractRefCounted createRefCounted() {
        final var closed = new AtomicBoolean();
        return AbstractRefCounted.of(new Runnable() {
            @Override
            public void run() {
                assertTrue(closed.compareAndSet(false, true));
            }

            @Override
            public String toString() {
                return "runnable description";
            }
        });
    }
}
