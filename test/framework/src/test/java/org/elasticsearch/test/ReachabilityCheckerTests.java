/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.hamcrest.Matchers;

public class ReachabilityCheckerTests extends ESTestCase {

    public void testSuccess() {
        final var reachabilityChecker = new ReachabilityChecker();
        var target = reachabilityChecker.register(createTarget());
        reachabilityChecker.checkReachable();
        target = null;
        reachabilityChecker.ensureUnreachable();
        assertNull(target);
    }

    public void testBecomesUnreachable() {
        final var reachabilityChecker = new ReachabilityChecker();
        var target = reachabilityChecker.register(createTarget());
        reachabilityChecker.checkReachable();
        target = null;
        assertThat(
            expectThrows(AssertionError.class, reachabilityChecker::checkReachable).getMessage(),
            Matchers.startsWith("became unreachable: test object")
        );
        assertNull(target);
    }

    public void testStaysReachable() {
        final var reachabilityChecker = new ReachabilityChecker();
        var target = reachabilityChecker.register(createTarget());
        reachabilityChecker.checkReachable();
        assertThat(
            expectThrows(AssertionError.class, () -> reachabilityChecker.ensureUnreachable(500)).getMessage(),
            Matchers.startsWith("still reachable: test object")
        );
        assertNotNull(target);
    }

    private static Object createTarget() {
        return new Object() {
            @Override
            public String toString() {
                return "test object";
            }
        };
    }
}
