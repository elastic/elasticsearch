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

/**
 * A {@link CyclicBarrier} whose {@link #await} methods throw an {@link AssertionError} instead of any checked exceptions, for use in tests.
 */
public class TestBarrier extends CyclicBarrier {
    public TestBarrier(int parties) {
        super(parties);
    }

    @Deprecated(since = "it's probably a mistake to use this in a test because tests should not be waiting forever")
    public int awaitForever() {
        try {
            return super.await();
        } catch (Exception e) {
            throw new AssertionError("unexpected", e);
        }
    }

    /**
     * {@link #await} with a 30s timeout.
     */
    public int awaitLong() {
        return await(30, TimeUnit.SECONDS);
    }

    @Override
    public int await() {
        // in general tests should not wait forever, so this method imposes a default timeout of 10s
        return await(10, TimeUnit.SECONDS);
    }

    @Override
    public int await(long timeout, TimeUnit unit) {
        try {
            return super.await(timeout, unit);
        } catch (Exception e) {
            throw new AssertionError("unexpected", e);
        }
    }
}
