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

public final class MemoryAccountingBytesRefCounted extends AbstractRefCounted {

    private long bytes;
    private final CircuitBreaker breaker;

    private MemoryAccountingBytesRefCounted(long bytes, CircuitBreaker breaker) {
        this.bytes = bytes;
        this.breaker = breaker;
    }

    public static MemoryAccountingBytesRefCounted create(CircuitBreaker breaker) {
        return new MemoryAccountingBytesRefCounted(0L, breaker);
    }

    public void account(long bytes, String label) {
        breaker.addEstimateBytesAndMaybeBreak(bytes, label);
        this.bytes += bytes;
    }

    @Override
    protected void closeInternal() {
        breaker.addWithoutBreaking(-bytes);
    }
}
