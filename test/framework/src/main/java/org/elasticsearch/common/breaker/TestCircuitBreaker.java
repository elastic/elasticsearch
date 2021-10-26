/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.breaker;

import java.util.concurrent.atomic.AtomicBoolean;

public class TestCircuitBreaker extends NoopCircuitBreaker {

    private final AtomicBoolean shouldBreak = new AtomicBoolean(false);

    public TestCircuitBreaker() {
        super("test");
    }

    @Override
    public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        if (shouldBreak.get()) {
            throw new CircuitBreakingException("broken", getDurability());
        }
    }

    public void startBreaking() {
        shouldBreak.set(true);
    }

    public void stopBreaking() {
        shouldBreak.set(false);
    }
}
