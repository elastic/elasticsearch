/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicInteger;

public class RowGroupIoTests extends ESTestCase {

    public void testCancelRunsEveryRegisteredWake() {
        RowGroupIo lease = new RowGroupIo();
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();
        lease.setWake(first::incrementAndGet);
        lease.setWake(second::incrementAndGet);
        lease.cancel();
        assertEquals(1, first.get());
        assertEquals(1, second.get());
        lease.cancel();
        assertEquals(1, first.get());
        assertEquals(1, second.get());
    }

    public void testSetWakeOnCancelledLeaseRunsImmediately() {
        RowGroupIo lease = new RowGroupIo();
        lease.cancel();
        AtomicInteger ran = new AtomicInteger();
        lease.setWake(ran::incrementAndGet);
        assertEquals(1, ran.get());
    }
}
