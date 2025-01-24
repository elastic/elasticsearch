/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Throttles tracked operations based on a time interval, restricting them to 1 per N seconds.
 */
enum IntervalThrottler {
    DOCUMENT_PARSING_FAILURE(60);

    static final int MILLISECONDS_IN_SECOND = 1000;

    private final Acceptor acceptor;

    IntervalThrottler(long intervalSeconds) {
        acceptor = new Acceptor(intervalSeconds * MILLISECONDS_IN_SECOND);
    }

    /**
     * @return true if the operation gets accepted, false if throttled.
     */
    boolean accept() {
        return acceptor.accept();
    }

    // Defined separately for testing.
    static class Acceptor {
        private final long intervalMillis;
        private final AtomicBoolean lastAcceptedGuard = new AtomicBoolean(false);
        private volatile long lastAcceptedTimeMillis = 0;

        Acceptor(long intervalMillis) {
            this.intervalMillis = intervalMillis;
        }

        boolean accept() {
            final long now = System.currentTimeMillis();
            // Check without guarding first, to reduce contention.
            if (now - lastAcceptedTimeMillis > intervalMillis) {
                // Check if another concurrent operation succeeded.
                if (lastAcceptedGuard.compareAndSet(false, true)) {
                    try {
                        // Repeat check under guard protection, so that only one message gets written per interval.
                        if (now - lastAcceptedTimeMillis > intervalMillis) {
                            lastAcceptedTimeMillis = now;
                            return true;
                        }
                    } finally {
                        // Reset guard.
                        lastAcceptedGuard.set(false);
                    }
                }
            }
            return false;
        }
    }
}
