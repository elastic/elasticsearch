/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.core.TimeValue;

import java.util.concurrent.TimeUnit;

/**
 * A simple, thread-safe stop watch for timing a single action.
 * Allows to stop the time for building a response and to log it at a later point.
 */
class ThreadSafeStopWatch {
    /**
     * Start time of the watch
     */
    private final long startTimeNS = System.nanoTime();

    /**
     * End time of the watch
     */
    private long endTimeNS;

    /**
     * Is the stop watch currently running?
     */
    private boolean running = true;

    /**
     * Starts the {@link ThreadSafeStopWatch} immediately after construction.
     */
    ThreadSafeStopWatch() {}

    /**
     * Stop the stop watch (or do nothing if it was already stopped) and return the elapsed time since starting.
     * @return the elapsed time since starting the watch
     */
    public TimeValue stop() {
        synchronized (this) {
            if (running) {
                endTimeNS = System.nanoTime();
                running = false;
            }

            return new TimeValue(endTimeNS - startTimeNS, TimeUnit.NANOSECONDS);
        }
    }
}
