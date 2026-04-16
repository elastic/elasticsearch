/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A simple {@link CircuitBreaker} that throws when incremented above its limit.
 */
public class LimitedBreaker extends NoopCircuitBreaker {
    /**
     * A {@link CircuitBreakerService} that always returns a limited breaker.
     */
    public static CircuitBreakerService service(String name, ByteSizeValue max) {
        /*
         * We used to use mockito for this, and it slowed down ESQL tests by like
         * 7%. Wasn't worth it.
         */
        return new Service(new LimitedBreaker(name, max));
    }

    private final AtomicLong used = new AtomicLong();
    private final ByteSizeValue max;

    public LimitedBreaker(String name, ByteSizeValue max) {
        super(name);
        this.max = max;
    }

    @Override
    public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        while (true) {
            long old = used.get();
            long total = old + bytes;
            if (total < 0) {
                throw new AssertionError("total must be >= 0 but was [" + total + "]");
            }
            if (total > max.getBytes()) {
                throw new CircuitBreakingException(MockBigArrays.ERROR_MESSAGE, bytes, max.getBytes(), Durability.TRANSIENT);
            }
            if (used.compareAndSet(old, total)) {
                break;
            }
        }
    }

    @Override
    public void addWithoutBreaking(long bytes) {
        long total = used.addAndGet(bytes);
        if (total < 0) {
            throw new AssertionError("total must be >= 0 but was [" + total + "]");
        }
    }

    @Override
    public long getUsed() {
        return used.get();
    }

    @Override
    public String toString() {
        long u = used.get();
        return "LimitedBreaker[" + u + "/" + max.getBytes() + "][" + ByteSizeValue.ofBytes(u) + "/" + max + "]";
    }

    private static class Service extends CircuitBreakerService {
        private final LimitedBreaker breaker;

        private Service(LimitedBreaker breaker) {
            this.breaker = breaker;
        }

        @Override
        public CircuitBreaker getBreaker(String name) {
            return breaker;
        }

        @Override
        public AllCircuitBreakerStats stats() {
            return null;
        }

        @Override
        public CircuitBreakerStats stats(String name) {
            return null;
        }
    }
}
