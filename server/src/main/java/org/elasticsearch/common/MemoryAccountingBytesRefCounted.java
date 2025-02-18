/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.AbstractRefCounted;

/**
 * A ref counted object that accounts for memory usage in bytes and releases the
 * accounted memory from the circuit breaker when the reference count reaches zero.
 */
public final class MemoryAccountingBytesRefCounted extends AbstractRefCounted {

    private int bytes;
    private final CircuitBreaker breaker;

    private MemoryAccountingBytesRefCounted(int bytes, CircuitBreaker breaker) {
        this.bytes = bytes;
        this.breaker = breaker;
    }

    public static MemoryAccountingBytesRefCounted create(CircuitBreaker breaker) {
        return new MemoryAccountingBytesRefCounted(0, breaker);
    }

    public void account(int bytes, String label) {
        breaker.addEstimateBytesAndMaybeBreak(bytes, label);
        this.bytes += bytes;
    }

    @Override
    protected void closeInternal() {
        breaker.addWithoutBreaking(-bytes);
    }
}
