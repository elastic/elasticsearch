/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.grok;

import org.joni.Matcher;

import java.util.concurrent.TimeUnit;

/**
 * Provides an interface for protecting code that uses joni's {@link org.joni.Matcher}
 * against long-running operations.
 * <p>
 * Some implementations of this interface may use threads and timeouts, but the default implementations
 * here are simpler: there's a no-op implementation, and there's an implementation that relies on
 * the {@link org.joni.Matcher#setTimeout(long)} method.
 */
public interface MatcherWatchdog {

    /**
     * Registers a matcher.
     *
     * @param matcher The matcher to register
     */
    void register(Matcher matcher);

    /**
     * @return The maximum allowed time in milliseconds for a thread to invoke {@link #unregister(Matcher)}
     *         after {@link #register(Matcher)} has been invoked.
     */
    long maxExecutionTimeInMillis();

    /**
     * Unregisters a matcher.
     *
     * @param matcher The matcher to unregister
     */
    void unregister(Matcher matcher);

    /**
     * Returns an implementation that relies on the {@link org.joni.Matcher#setTimeout(long)} method.
     *
     * @param maxExecutionTime The time in millis that a matcher has to execute an operation.
     */
    static MatcherWatchdog newInstance(long maxExecutionTime) {
        return new Default(maxExecutionTime);
    }

    /**
     * @return A noop implementation that does not interrupt threads and is useful for testing and pre-defined grok expressions.
     */
    static MatcherWatchdog noop() {
        return Noop.INSTANCE;
    }

    final class Noop implements MatcherWatchdog {

        private static final Noop INSTANCE = new Noop();

        private Noop() {}

        @Override
        public void register(Matcher matcher) {}

        @Override
        public long maxExecutionTimeInMillis() {
            return Long.MAX_VALUE;
        }

        @Override
        public void unregister(Matcher matcher) {}
    }

    final class Default implements MatcherWatchdog {

        // duplicated from org.elasticsearch.core.TimeValue because we don't have access to that here
        private static final long NSEC_PER_MSEC = TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS);

        private final long maxExecutionTimeMillis;

        private Default(long maxExecutionTimeMillis) {
            this.maxExecutionTimeMillis = maxExecutionTimeMillis;
        }

        @Override
        public void register(Matcher matcher) {
            if (maxExecutionTimeMillis > 0) {
                matcher.setTimeout(maxExecutionTimeMillis * NSEC_PER_MSEC);
            } else {
                matcher.setTimeout(-1); // disable timeouts
            }
        }

        @Override
        public long maxExecutionTimeInMillis() {
            return maxExecutionTimeMillis;
        }

        @Override
        public void unregister(Matcher matcher) {
            // noop
        }
    }

}
