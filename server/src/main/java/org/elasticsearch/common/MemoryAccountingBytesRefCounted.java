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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.AbstractRefCounted;

public final class MemoryAccountingBytesRefCounted extends AbstractRefCounted {

    private BytesReference bytes;
    private CircuitBreaker breaker;

    private MemoryAccountingBytesRefCounted(BytesReference bytes, CircuitBreaker breaker) {
        this.bytes = bytes;
        this.breaker = breaker;
    }

    public static MemoryAccountingBytesRefCounted createAndAccountForBytes(
        BytesReference bytes,
        CircuitBreaker breaker,
        String memAccountingLabel
    ) {
        breaker.addEstimateBytesAndMaybeBreak(bytes.length(), memAccountingLabel);
        return new MemoryAccountingBytesRefCounted(bytes, breaker);
    }

    @Override
    protected void closeInternal() {
        breaker.addWithoutBreaking(-bytes.length());
    }
}
