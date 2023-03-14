/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ReachabilityChecker;

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
}
