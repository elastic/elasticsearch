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

    public void testCancelRunsOneWakePerOwner() {
        RowGroupIo lease = new RowGroupIo();
        AtomicInteger bytes = new AtomicInteger();
        AtomicInteger budget = new AtomicInteger();
        lease.setWake("bytes", bytes::incrementAndGet);
        lease.setWake("bytes", bytes::incrementAndGet);
        lease.setWake("budget", budget::incrementAndGet);
        lease.cancel();
        assertEquals(1, bytes.get());
        assertEquals(1, budget.get());
        lease.cancel();
        assertEquals(1, bytes.get());
        assertEquals(1, budget.get());
    }

    public void testSetWakeOnCancelledLeaseDoesNotRunInline() {
        RowGroupIo lease = new RowGroupIo();
        lease.cancel();
        AtomicInteger ran = new AtomicInteger();
        lease.setWake("late", ran::incrementAndGet);
        assertEquals(0, ran.get());
    }
}
