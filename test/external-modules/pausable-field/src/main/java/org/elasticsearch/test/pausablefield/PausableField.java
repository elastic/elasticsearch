/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.pausablefield;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

final class PausableField {
    private static final AtomicReference<CountDownLatch> blockingRef = new AtomicReference<>();
    private static final AtomicInteger parked = new AtomicInteger();
    private static final AtomicReference<Semaphore> gateRef = new AtomicReference<>();
    private static final AtomicInteger gated = new AtomicInteger();
    private static final Logger LOGGER = LogManager.getLogger(PausableField.class);

    private PausableField() {}

    /** Blocks execution: every query on a pausable field parks once it runs, until {@link #unblock}. */
    static void blockExecution() {
        CountDownLatch prev = blockingRef.getAndSet(new CountDownLatch(1));
        if (prev != null) {
            prev.countDown();
        }
    }

    /**
     * Blocks execution like {@link #blockExecution}, and also closes the admission gate: every query on a pausable field then waits at
     * the gate while it is being built, before anything after it in the query is built, until {@link #admit} lets it in. This serializes
     * whatever the queries build after the pausable field across shards and requests.
     */
    static void stop() {
        Semaphore prev = gateRef.getAndSet(new Semaphore(0));
        openGate(prev);
        blockExecution();
    }

    /** Lets exactly one query waiting at the admission gate, any of them, in. If none is waiting, the next one to arrive goes in. */
    static void admit() {
        Semaphore gate = gateRef.get();
        if (gate != null) {
            gate.release();
        }
    }

    /** Removes the admission gate, letting every query waiting there in, and releases every query parked by the execution block. */
    static void unblock() {
        openGate(gateRef.getAndSet(null));
        CountDownLatch prev = blockingRef.getAndSet(null);
        if (prev != null) {
            prev.countDown();
        }
    }

    private static void openGate(Semaphore gate) {
        if (gate != null) {
            // More permits than there can ever be threads waiting.
            gate.release(1_000_000);
        }
    }

    static void waitForAdmission() {
        Semaphore gate = gateRef.get();
        if (gate != null) {
            gated.incrementAndGet();
            try {
                if (gate.tryAcquire(5, TimeUnit.MINUTES) == false) {
                    throw new AssertionError("timed out waiting at the admission gate of a pausable field");
                }
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            } finally {
                gated.decrementAndGet();
            }
        }
    }

    static void waitForExecutionPermit() {
        CountDownLatch latch = blockingRef.get();
        if (latch != null) {
            LOGGER.info("--> waiting for the latch of pausable field");
            parked.incrementAndGet();
            try {
                if (latch.await(5, TimeUnit.MINUTES) == false) {
                    throw new AssertionError("timed out waiting for the latch of a pausable field");
                }
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            } finally {
                parked.decrementAndGet();
            }
        }
    }

    /** The number of threads currently blocked on the latch, so that a test can wait for its requests to actually be held. */
    static int parkedCount() {
        return parked.get();
    }

    /** The number of threads currently waiting at the admission gate. */
    static int gatedCount() {
        return gated.get();
    }
}
