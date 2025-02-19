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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.test.ESTestCase;

public class MemoryAccountingBytesRefCountedTests extends ESTestCase {

    public void testNoMemoryAccounted() {
        CircuitBreaker breaker = new MockBigArrays.LimitedBreaker("test", ByteSizeValue.ofGb(1));
        MemoryAccountingBytesRefCounted refCounted = MemoryAccountingBytesRefCounted.create(breaker);
        refCounted.decRef();
        assertEquals(0, breaker.getUsed());
    }

    public void testMemoryAccounted() {
        CircuitBreaker breaker = new MockBigArrays.LimitedBreaker("test", ByteSizeValue.ofGb(1));
        MemoryAccountingBytesRefCounted refCounted = MemoryAccountingBytesRefCounted.create(breaker);
        refCounted.setBytesAndAccount(10, "test");
        assertEquals(10, breaker.getUsed());
    }

    public void testCloseInternalDecrementsBreaker() {
        CircuitBreaker breaker = new MockBigArrays.LimitedBreaker("test", ByteSizeValue.ofGb(1));
        MemoryAccountingBytesRefCounted refCounted = MemoryAccountingBytesRefCounted.create(breaker);
        refCounted.setBytesAndAccount(10, "test");
        refCounted.decRef();
        assertEquals(0, breaker.getUsed());
    }

    public void testBreakerNotDecrementedIfRefsRemaining() {
        CircuitBreaker breaker = new MockBigArrays.LimitedBreaker("test", ByteSizeValue.ofGb(1));
        MemoryAccountingBytesRefCounted refCounted = MemoryAccountingBytesRefCounted.create(breaker);
        refCounted.setBytesAndAccount(10, "test");
        refCounted.incRef(); // 2 refs
        assertEquals(10, breaker.getUsed());
        refCounted.decRef(); // 1 ref remaining so no decrementing is executed
        assertEquals(10, breaker.getUsed());
    }
}
