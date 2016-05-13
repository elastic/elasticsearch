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

import org.apache.http.HttpHost;

import java.util.concurrent.TimeUnit;

/**
 * Represents a connection to a host. It holds the host that the connection points to and the state of the connection to it.
 */
public class Connection {
    private static final long DEFAULT_CONNECTION_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(1);
    private static final long MAX_CONNECTION_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(30);
    private final HttpHost host;
    private volatile int failedAttempts = 0;
    private volatile long deadUntil = -1;

    /**
     * Creates a new connection pointing to the provided {@link HttpHost} argument
     */
    public Connection(HttpHost host) {
            this.host = host;
        }

    /**
     * Returns the {@link HttpHost} that the connection points to
     */
    public HttpHost getHost() {
            return host;
        }

    /**
     * Marks connection as dead. Should be called in case the corresponding node is not responding or caused failures.
     * Once marked dead, the number of failed attempts will be incremented on each call to this method. A dead connection
     * should be retried once {@link #isBlacklisted()} returns true, which depends on the number of previous failed attempts
     * and when the last failure was registered.
     */
    void markDead() {
        synchronized (this) {
            int failedAttempts = Math.max(this.failedAttempts, 0);
            long timeoutMillis = (long)Math.min(DEFAULT_CONNECTION_TIMEOUT_MILLIS * 2 * Math.pow(2, failedAttempts * 0.5 - 1),
                    MAX_CONNECTION_TIMEOUT_MILLIS);
            this.deadUntil = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            this.failedAttempts = ++failedAttempts;
        }
    }

    /**
     * Marks this connection alive. Should be called when the corresponding node is working properly.
     * Will reset the number of failed attempts that were counted in case the connection was previously dead, as well as its timeout.
     */
    void markAlive() {
        if (this.failedAttempts > 0) {
            synchronized (this) {
                this.deadUntil = -1;
                this.failedAttempts = 0;
            }
        }
    }

    /**
     * Returns the timestamp till the connection is supposed to stay dead. After that moment the connection should be retried
     */
    public long getDeadUntil() {
        return deadUntil;
    }

    /**
     * Returns true when the connection should be skipped due to previous failures, false in case the connection is alive
     * or dead but ready to be retried. When the connection is dead, returns false when it is time to retry it, depending
     * on how many failed attempts were registered and when the last failure happened (minimum 1 minute, maximum 30 minutes).
     */
    public boolean isBlacklisted() {
        return failedAttempts > 0 && System.nanoTime() - deadUntil < 0;
    }
}
