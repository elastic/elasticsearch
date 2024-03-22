/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

/**
 * A class that throttles calls to a logger. If a log call is made during the throttle period a counter is incremented.
 * If a log call occurs after the throttle period, then the call will proceed, and it will include a message like
 * "repeated X times" to indicate how often the message was attempting to be logged.
 */
public class Throttler implements Closeable {

    private static final Logger classLogger = LogManager.getLogger(Throttler.class);

    private final TimeValue resetInterval;
    private Duration durationToWait;
    private final Clock clock;
    private final ConcurrentMap<String, LogExecutor> logExecutors;
    private final AtomicReference<Scheduler.Cancellable> cancellableTask = new AtomicReference<>();
    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    /**
     * Constructs the throttler and kicks of a scheduled tasks to clear the internal stats.
     *
     * @param resetInterval the frequency for clearing the internal stats. This protects against an ever growing
     *                      cache
     * @param durationToWait the amount of time to wait before logging a message after the threshold
     *                       is reached
     * @param threadPool a thread pool for running a scheduled task to clear the internal stats
     */
    public Throttler(TimeValue resetInterval, TimeValue durationToWait, ThreadPool threadPool) {
        this(resetInterval, durationToWait, Clock.systemUTC(), threadPool, new ConcurrentHashMap<>());
    }

    /**
     * This should only be used directly for testing.
     */
    Throttler(
        TimeValue resetInterval,
        TimeValue durationToWait,
        Clock clock,
        ThreadPool threadPool,
        ConcurrentMap<String, LogExecutor> logExecutors
    ) {
        Objects.requireNonNull(durationToWait);
        Objects.requireNonNull(threadPool);

        this.resetInterval = Objects.requireNonNull(resetInterval);
        this.durationToWait = Duration.ofMillis(durationToWait.millis());
        this.clock = Objects.requireNonNull(clock);
        this.logExecutors = Objects.requireNonNull(logExecutors);

        this.cancellableTask.set(startResetTask(threadPool));
    }

    private Scheduler.Cancellable startResetTask(ThreadPool threadPool) {
        classLogger.debug(() -> format("Reset task scheduled with interval [%s]", resetInterval));

        return threadPool.scheduleWithFixedDelay(logExecutors::clear, resetInterval, threadPool.executor(UTILITY_THREAD_POOL_NAME));
    }

    public void setDurationToWait(TimeValue durationToWait) {
        this.durationToWait = Duration.ofMillis(durationToWait.millis());
    }

    public void execute(String message, Consumer<String> consumer) {
        if (isRunning.get() == false) {
            return;
        }

        LogExecutor logExecutor = logExecutors.compute(message, (key, value) -> {
            if (value == null) {
                return new LogExecutor(clock, consumer);
            }

            return value.compute(consumer, durationToWait);
        });

        // This executes an internal consumer that wraps the passed in one, it will either log the message passed here
        // unchanged, do nothing if it is in the throttled period, or log this message + some text saying how many times it was repeated
        logExecutor.log(message);
    }

    @Override
    public void close() {
        isRunning.set(false);
        cancellableTask.get().cancel();
        logExecutors.clear();
    }

    private static class LogExecutor {
        private final long skippedLogCalls;
        private final Instant timeOfLastLogCall;
        private final Clock clock;
        private final Consumer<String> consumer;

        LogExecutor(Clock clock, Consumer<String> throttledConsumer) {
            this(clock, 0, throttledConsumer);
        }

        LogExecutor(Clock clock, long skippedLogCalls, Consumer<String> consumer) {
            this.skippedLogCalls = skippedLogCalls;
            this.clock = Objects.requireNonNull(clock);
            timeOfLastLogCall = Instant.now(this.clock);
            this.consumer = Objects.requireNonNull(consumer);
        }

        void log(String message) {
            this.consumer.accept(message);
        }

        LogExecutor compute(Consumer<String> executor, Duration durationToWait) {
            if (hasDurationExpired(durationToWait)) {
                String messageToAppend = "";
                if (this.skippedLogCalls == 1) {
                    messageToAppend = ", repeated 1 time";
                } else if (this.skippedLogCalls > 1) {
                    messageToAppend = format(", repeated %s times", this.skippedLogCalls);
                }

                final String stringToAppend = messageToAppend;
                return new LogExecutor(this.clock, 0, (message) -> executor.accept(message.concat(stringToAppend)));
            }

            // This creates a consumer that won't do anything because the original consumer is being throttled
            return new LogExecutor(this.clock, this.skippedLogCalls + 1, (message) -> {});
        }

        private boolean hasDurationExpired(Duration durationToWait) {
            Instant now = Instant.now(clock);
            return now.isAfter(timeOfLastLogCall.plus(durationToWait));
        }
    }
}
