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

package org.elasticsearch.client;

import java.util.concurrent.TimeUnit;

/**
 * Holds the state of a dead connection to a host. Keeps track of how many failed attempts were performed and
 * when the host should be retried (based on number of previous failed attempts).
 * Class is immutable, a new copy of it should be created each time the state has to be changed.
 */
final class DeadHostState {

    private static final long MIN_CONNECTION_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(1);
    private static final long MAX_CONNECTION_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(30);

    static final DeadHostState INITIAL_DEAD_STATE = new DeadHostState();

    private final int failedAttempts;
    private final long deadUntilNanos;

    private DeadHostState() {
        this.failedAttempts = 1;
        this.deadUntilNanos = System.nanoTime() + MIN_CONNECTION_TIMEOUT_NANOS;
    }

    /**
     * We keep track of how many times a certain node fails consecutively. The higher that number is the longer we will wait
     * to retry that same node again. Minimum is 1 minute (for a node the only failed once), maximum is 30 minutes (for a node
     * that failed many consecutive times).
     */
    DeadHostState(DeadHostState previousDeadHostState) {
        long timeoutNanos = (long)Math.min(MIN_CONNECTION_TIMEOUT_NANOS * 2 * Math.pow(2, previousDeadHostState.failedAttempts * 0.5 - 1),
                MAX_CONNECTION_TIMEOUT_NANOS);
        this.deadUntilNanos = System.nanoTime() + timeoutNanos;
        this.failedAttempts = previousDeadHostState.failedAttempts + 1;
    }

    /**
     * Returns the timestamp (nanos) till the host is supposed to stay dead without being retried.
     * After that the host should be retried.
     */
    long getDeadUntilNanos() {
        return deadUntilNanos;
    }

    @Override
    public String toString() {
        return "DeadHostState{" +
                "failedAttempts=" + failedAttempts +
                ", deadUntilNanos=" + deadUntilNanos +
                '}';
    }
}
