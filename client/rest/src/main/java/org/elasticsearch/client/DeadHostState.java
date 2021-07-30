/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Holds the state of a dead connection to a host. Keeps track of how many failed attempts were performed and
 * when the host should be retried (based on number of previous failed attempts).
 * Class is immutable, a new copy of it should be created each time the state has to be changed.
 */
final class DeadHostState implements Comparable<DeadHostState> {

    private static final long MIN_CONNECTION_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(1);
    static final long MAX_CONNECTION_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(30);
    static final Supplier<Long> DEFAULT_TIME_SUPPLIER = System::nanoTime;

    private final int failedAttempts;
    private final long deadUntilNanos;
    private final Supplier<Long> timeSupplier;

    /**
     * Build the initial dead state of a host. Useful when a working host stops functioning
     * and needs to be marked dead after its first failure. In such case the host will be retried after a minute or so.
     *
     * @param timeSupplier a way to supply the current time and allow for unit testing
     */
    DeadHostState(Supplier<Long> timeSupplier) {
        this.failedAttempts = 1;
        this.deadUntilNanos = timeSupplier.get() + MIN_CONNECTION_TIMEOUT_NANOS;
        this.timeSupplier = timeSupplier;
    }

    /**
     * Build the dead state of a host given its previous dead state. Useful when a host has been failing before, hence
     * it already failed for one or more consecutive times. The more failed attempts we register the longer we wait
     * to retry that same host again. Minimum is 1 minute (for a node the only failed once created
     * through {@link #DeadHostState(Supplier)}), maximum is 30 minutes (for a node that failed more than 10 consecutive times)
     *
     * @param previousDeadHostState the previous state of the host which allows us to increase the wait till the next retry attempt
     */
    DeadHostState(DeadHostState previousDeadHostState) {
        long timeoutNanos = (long)Math.min(MIN_CONNECTION_TIMEOUT_NANOS * 2 * Math.pow(2, previousDeadHostState.failedAttempts * 0.5 - 1),
                MAX_CONNECTION_TIMEOUT_NANOS);
        this.deadUntilNanos = previousDeadHostState.timeSupplier.get() + timeoutNanos;
        this.failedAttempts = previousDeadHostState.failedAttempts + 1;
        this.timeSupplier = previousDeadHostState.timeSupplier;
    }

    /**
     * Indicates whether it's time to retry to failed host or not.
     *
     * @return true if the host should be retried, false otherwise
     */
    boolean shallBeRetried() {
        return timeSupplier.get() - deadUntilNanos > 0;
    }

    /**
     * Returns the timestamp (nanos) till the host is supposed to stay dead without being retried.
     * After that the host should be retried.
     */
    long getDeadUntilNanos() {
        return deadUntilNanos;
    }

    int getFailedAttempts() {
        return failedAttempts;
    }

    @Override
    public int compareTo(DeadHostState other) {
        if (timeSupplier != other.timeSupplier) {
            throw new IllegalArgumentException("can't compare DeadHostStates holding different time suppliers as they may " +
                "be based on different clocks");
        }
        return Long.compare(deadUntilNanos, other.deadUntilNanos);
    }

    @Override
    public String toString() {
        return "DeadHostState{" +
                "failedAttempts=" + failedAttempts +
                ", deadUntilNanos=" + deadUntilNanos +
                ", timeSupplier=" + timeSupplier +
                '}';
    }
}
