/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.lease.Releasable;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Container that represents a resource with reference counting capabilities. Provides operations to suspend acquisition of new references.
 * This is useful for resource management when resources are intermittently unavailable.
 *
 * Assumes less than Integer.MAX_VALUE references are concurrently being held at one point in time.
 */
public final class SuspendableRefContainer {
    private static final int TOTAL_PERMITS = Integer.MAX_VALUE;
    private final Semaphore semaphore;

    public SuspendableRefContainer() {
        // fair semaphore to ensure that blockAcquisition() does not starve under thread contention
        this.semaphore = new Semaphore(TOTAL_PERMITS, true);
    }

    /**
     * Tries acquiring a reference. Returns reference holder if reference acquisition is not blocked at the time of invocation (see
     * {@link #blockAcquisition()}). Returns null if reference acquisition is blocked at the time of invocation.
     *
     * @return reference holder if reference acquisition is not blocked, null otherwise
     * @throws InterruptedException if the current thread is interrupted
     */
    public Releasable tryAcquire() throws InterruptedException {
        if (semaphore.tryAcquire(1, 0, TimeUnit.SECONDS)) { // the untimed tryAcquire methods do not honor the fairness setting
            return idempotentRelease(1);
        } else {
            return null;
        }
    }

    /**
     * Acquires a reference. Blocks if reference acquisition is blocked at the time of invocation.
     *
     * @return reference holder
     * @throws InterruptedException if the current thread is interrupted
     */
    public Releasable acquire() throws InterruptedException {
        semaphore.acquire();
        return idempotentRelease(1);
    }

    /**
     * Acquires a reference. Blocks if reference acquisition is blocked at the time of invocation.
     *
     * @return reference holder
     */
    public Releasable acquireUninterruptibly() {
        semaphore.acquireUninterruptibly();
        return idempotentRelease(1);
    }

    /**
     * Disables reference acquisition and waits until all existing references are released.
     * When released, reference acquisition is enabled again.
     * This guarantees that between successful acquisition and release, no one is holding a reference.
     *
     * @return references holder to all references
     */
    public Releasable blockAcquisition() {
        semaphore.acquireUninterruptibly(TOTAL_PERMITS);
        return idempotentRelease(TOTAL_PERMITS);
    }

    /**
     * Helper method that ensures permits are only released once
     *
     * @return reference holder
     */
    private Releasable idempotentRelease(int permits) {
        AtomicBoolean closed = new AtomicBoolean();
        return () -> {
            if (closed.compareAndSet(false, true)) {
                semaphore.release(permits);
            }
        };
    }

    /**
     * Returns the number of references currently being held.
     */
    public int activeRefs() {
        int availablePermits = semaphore.availablePermits();
        if (availablePermits == 0) {
            // when blockAcquisition is holding all permits
            return 0;
        } else {
            return TOTAL_PERMITS - availablePermits;
        }
    }
}
