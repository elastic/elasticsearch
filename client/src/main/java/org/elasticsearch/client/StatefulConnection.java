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
 * {@link Connection} subclass that has a mutable state, based on previous usage.
 * When first created, a connection is in unknown state, till it is used for the first time
 * and marked either dead or alive based on the outcome of the first usage.
 * Should be marked alive when properly working.
 * Should be marked dead when it caused a failure, in which case the connection may be retried some time later,
 * as soon as {@link #shouldBeRetried()} returns true, which depends on how many consecutive failed attempts
 * were counted and when the last one was registered.
 * Should be marked resurrected if in dead state, as last resort in case there are no live connections available
 * and none of the dead ones are ready to be retried yet. When marked resurrected, the number of failed attempts
 * and its timeout is not reset so that if it gets marked dead again it returns to the exact state before resurrection.
 */
public final class StatefulConnection extends Connection {
    //TODO make these values configurable through the connection pool?
    private static final long DEFAULT_CONNECTION_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(1);
    private static final long MAX_CONNECTION_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(30);

    private volatile State state = State.UNKNOWN;
    private volatile int failedAttempts = -1;
    private volatile long deadUntil = -1;

    /**
     * Creates a new mutable connection pointing to the provided {@link Node} argument
     */
    public StatefulConnection(Node node) {
        super(node);
    }

    /**
     * Marks connection as dead. Should be called in case the corresponding node is not responding or caused failures.
     * Once marked dead, the number of failed attempts will be incremented on each call to this method. A dead connection
     * should be retried once {@link #shouldBeRetried()} returns true, which depends on the number of previous failed attempts
     * and when the last failure was registered.
     */
    void markDead() {
        synchronized (this) {
            int failedAttempts = Math.max(this.failedAttempts, 0);
            long timeoutMillis = (long)Math.min(DEFAULT_CONNECTION_TIMEOUT_MILLIS * 2 * Math.pow(2, failedAttempts * 0.5 - 1),
                    MAX_CONNECTION_TIMEOUT_MILLIS);
            this.deadUntil = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            this.failedAttempts = ++failedAttempts;
            this.state = State.DEAD;
        }
    }

    /**
     * Marks this connection alive. Should be called when the corresponding node is working properly.
     * Will reset the number of failed attempts that were counted in case the connection was previously dead,
     * as well as its dead timeout.
     */
    void markAlive() {
        if (this.state != State.ALIVE) {
            synchronized (this) {
                this.deadUntil = -1;
                this.failedAttempts = 0;
                this.state = State.ALIVE;
            }
        }
    }

    /**
     * Resets the connection to its initial state, so it will be retried. To be called when all the connections in the pool
     * are dead, so that one connection can be retried. Note that calling this method only changes the state of the connection,
     * it doesn't reset its failed attempts and dead until timestamp. That way if the connection goes back to dead straightaway
     * all of its previous failed attempts are taken into account.
     */
    void markResurrected() {
        if (this.state == State.DEAD) {
            synchronized (this) {
                this.state = State.UNKNOWN;
            }
        }
    }

    /**
     * Returns the timestamp till the connection is supposed to stay dead till it can be retried
     */
    public long getDeadUntil() {
        return deadUntil;
    }

    /**
     * Returns true if the connection is alive, false otherwise.
     */
    public boolean isAlive() {
        return state == State.ALIVE;
    }

    /**
     * Returns true in case the connection is not alive but should be used/retried, false otherwise.
     * Returns true in case the connection is in unknown state (never used before) or resurrected. When the connection is dead,
     * returns true when it is time to retry it, depending on how many failed attempts were registered and when the last failure
     * happened (minimum 1 minute, maximum 30 minutes).
     */
    public boolean shouldBeRetried() {
        return state == State.UNKNOWN || (state == State.DEAD && System.nanoTime() - deadUntil >= 0);
    }

    private enum State {
        UNKNOWN, DEAD, ALIVE
    }
}
