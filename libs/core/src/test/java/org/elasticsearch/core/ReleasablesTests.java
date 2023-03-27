/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.core;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ReachabilityChecker;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ReleasablesTests extends ESTestCase {

    public void testReleaseOnce() {
        AtomicInteger count = new AtomicInteger(0);
        Releasable releasable = Releasables.releaseOnce(count::incrementAndGet);
        assertEquals(0, count.get());
        releasable.close();
        assertEquals(1, count.get());
        releasable.close();
        assertEquals(1, count.get());
    }

    public void testReleaseOnceReleasesDelegate() {
        final var reachabilityChecker = new ReachabilityChecker();
        final var releaseOnce = Releasables.releaseOnce(reachabilityChecker.register(this::noop));
        reachabilityChecker.checkReachable();
        releaseOnce.close();
        reachabilityChecker.ensureUnreachable();
        assertEquals("releaseOnce[null]", releaseOnce.toString());
    }

    private void noop() {}

    public void testAssertOnceReleaseTwice() {
        assumeTrue("assertions must be enabled for this test to make sense", Assertions.ENABLED);

        class CloserWithIdentifiableMethodNames {
            static void closeMethod1(Releasable releasable) {
                releasable.close();
            }

            static void closeMethod2(Releasable releasable) {
                releasable.close();
            }
        }

        final var releasable = Releasables.assertOnce(new Releasable() {
            @Override
            public void close() {}

            @Override
            public String toString() {
                return "example releasable";
            }
        });

        CloserWithIdentifiableMethodNames.closeMethod1(releasable);
        final var assertionError = expectThrows(AssertionError.class, () -> CloserWithIdentifiableMethodNames.closeMethod2(releasable));
        assertEquals("example releasable", assertionError.getCause().getMessage());
        assertTrue(
            Arrays.stream(assertionError.getStackTrace())
                .anyMatch(ste -> ste.toString().contains("CloserWithIdentifiableMethodNames.closeMethod2"))
        );
        assertTrue(
            Arrays.stream(assertionError.getCause().getStackTrace())
                .anyMatch(ste -> ste.toString().contains("CloserWithIdentifiableMethodNames.closeMethod1"))
        );
    }

    public void testWrap() {
        final var count = new AtomicInteger(0);
        final Releasable releasable = new Releasable() {
            @Override
            public void close() {
                count.incrementAndGet();
            }

            @Override
            public String toString() {
                return "increment count";
            }
        };

        final var wrapVarArgs = Releasables.wrap(releasable, releasable);
        assertEquals("wrapped[increment count, increment count]", wrapVarArgs.toString());
        wrapVarArgs.close();
        assertEquals(2, count.get());

        final var wrapIterable = Releasables.wrap(new Iterable<>() {
            @Override
            public Iterator<Releasable> iterator() {
                return List.of(releasable, releasable, releasable).iterator();
            }

            @Override
            public String toString() {
                return "list";
            }
        });
        assertEquals("wrapped[list]", wrapIterable.toString());
        wrapIterable.close();
        assertEquals(5, count.get());
    }
}
