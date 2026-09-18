/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.esql.heap_attack;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

final class PausableField {
    private static final AtomicReference<CountDownLatch> blockingRef = new AtomicReference<>();
    private static final Logger LOGGER = LogManager.getLogger(PausableField.class);

    private PausableField() {}

    static void blockExecution() {
        CountDownLatch prev = blockingRef.getAndSet(new CountDownLatch(1));
        if (prev != null) {
            prev.countDown();
        }
    }

    static void unblockExecution() {
        CountDownLatch prev = blockingRef.getAndSet(null);
        if (prev != null) {
            prev.countDown();
        }
    }

    static void waitForExecutionPermit() {
        CountDownLatch latch = blockingRef.get();
        if (latch != null) {
            LOGGER.info("--> waiting for the latch of pausable field");
            try {
                if (latch.await(5, TimeUnit.MINUTES) == false) {
                    throw new AssertionError("timed out waiting for the latch of a pausable field");
                }
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
    }
}
