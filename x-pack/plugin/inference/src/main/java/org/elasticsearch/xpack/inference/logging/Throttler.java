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

    public void warn(Logger logger, String message, Throwable e) {
        Objects.requireNonNull(message);
        Objects.requireNonNull(e);

        if (isRunning.get()) {
            logHelper(message, msgToAppend -> logger.warn(message.concat(msgToAppend), e));
        }
    }

    private void logHelper(String message, Consumer<String> executor) {
        LogExecutor logExecutor = logExecutors.compute(message, (key, value) -> {
            if (value == null) {
                return new LogExecutor(clock, executor);
            }

            return value.compute(executor, durationToWait);
        });

        logExecutor.log();
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
        private final Runnable logRunner;

        LogExecutor(Clock clock, Consumer<String> logAppendedMessage) {
            skippedLogCalls = 0;
            timeOfLastLogCall = Instant.now(clock);
            this.clock = clock;
            // The first log message can log the original message without waiting
            this.logRunner = () -> logAppendedMessage.accept("");
        }

        LogExecutor(Clock clock, long skippedLogCalls, Runnable logRunner) {
            this.skippedLogCalls = skippedLogCalls;
            timeOfLastLogCall = Instant.now(clock);
            this.clock = clock;
            this.logRunner = logRunner;
        }

        void log() {
            this.logRunner.run();
        }

        LogExecutor compute(Consumer<String> executor, Duration durationToWait) {
            if (hasDurationExpired(durationToWait)) {
                String msg = "";
                if (this.skippedLogCalls == 1) {
                    msg = ", repeated 1 time";
                } else if (this.skippedLogCalls > 1) {
                    msg = format(", repeated %s times", this.skippedLogCalls);
                }

                String finalMsg = msg;
                return new LogExecutor(this.clock, 0, () -> executor.accept(finalMsg));
            }

            return new LogExecutor(this.clock, this.skippedLogCalls + 1, () -> {});
        }

        private boolean hasDurationExpired(Duration durationToWait) {
            Instant now = Instant.now(clock);
            return now.isAfter(timeOfLastLogCall.plus(durationToWait));
        }
    }
}
