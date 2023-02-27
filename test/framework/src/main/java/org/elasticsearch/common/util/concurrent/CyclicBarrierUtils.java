/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

public class CyclicBarrierUtils {
    private CyclicBarrierUtils() {
        // no instances
    }

    /**
     * Await the given {@link CyclicBarrier}, failing the test after 10s with an {@link AssertionError}. Tests should not wait forever, so
     * a timed wait is always appropriate.
     */
    public static void await(CyclicBarrier cyclicBarrier) {
        try {
            cyclicBarrier.await(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("unexpected", e);
        }
    }
}
