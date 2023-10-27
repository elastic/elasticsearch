/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.logging;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.TimeValue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

public class ThrottledLogger {
    private final Logger logger;
    private final int threshold;
    private final Duration durationToWait;
    private int logCalls = 0;
    private final Clock clock;
    private Instant lastLoggedMessage;

    /**
     * A wrapper around a {@link Logger} that throttles logging calls to avoid too many log messages.
     *
     * @param logger a logger scoped to a class
     * @param threshold the number of messages to permit before beginning to throttle them
     * @param durationToWait the amount of time to wait to pass before logging a message after the threshold
     *                       is reached
     */
    public ThrottledLogger(Logger logger, int threshold, TimeValue durationToWait) {
        this(logger, threshold, durationToWait, Clock.systemUTC());
    }

    /**
     * This should only be used directly for testing.
     */
    ThrottledLogger(Logger logger, int threshold, TimeValue durationToWait, Clock clock) {
        this.logger = logger;
        this.threshold = threshold;
        this.durationToWait = Duration.ofMillis(durationToWait.millis());
        this.clock = clock;
        // Initializing to avoid null pointers
        this.lastLoggedMessage = Instant.now(clock);
    }

    public void warn(String message, Throwable e) {
        doLog(() -> logger.warn(message, e));
    }

    private synchronized void doLog(Runnable runner) {
        Instant now = Instant.now(clock);

        if (logCalls < threshold) {
            logCalls++;
            this.lastLoggedMessage = now;
            runner.run();
        } else if (now.isAfter(lastLoggedMessage.plus(this.durationToWait))) {
            this.lastLoggedMessage = now;
            runner.run();
        }
    }
}
