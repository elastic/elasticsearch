/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

/**
 * A class that throttles calls to a logger. The first unique log message is permitted to emit a message. Any subsequent log messages
 * matching a message that has already been emitted will only increment a counter. A thread runs on an interval
 * to emit any log messages that have been repeated beyond the initial emitted message. Once the thread emits a repeated
 * message the counter is reset. If another message is received matching a previously emitted message by the thread, it will be consider
 * the first time a unique message is received and will be logged.
 */
public class Throttler implements Closeable {

    private static final Logger classLogger = LogManager.getLogger(Throttler.class);

    private final TimeValue loggingInterval;
    private final Clock clock;
    private final ConcurrentMap<String, LogExecutor> logExecutors;
    private final AtomicReference<Scheduler.Cancellable> cancellableTask = new AtomicReference<>();
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final ThreadPool threadPool;
    // This lock governs the ability of the utility thread to get exclusive access to remove entries
    // from the map
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * @param loggingInterval the frequency to run a task to emit repeated log messages
     * @param threadPool a thread pool for running a scheduled task to clear the internal stats
     */
    public Throttler(TimeValue loggingInterval, ThreadPool threadPool) {
        this(loggingInterval, Clock.systemUTC(), threadPool, new ConcurrentHashMap<>());
    }

    /**
     * @param oldThrottler a previous throttler that is being replaced
     * @param loggingInterval the frequency to run a task to emit repeated log messages
     */
    public Throttler(Throttler oldThrottler, TimeValue loggingInterval) {
        this(loggingInterval, oldThrottler.clock, oldThrottler.threadPool, new ConcurrentHashMap<>(oldThrottler.logExecutors));
    }

    /**
     * This should only be used directly for testing.
     */
    Throttler(TimeValue loggingInterval, Clock clock, ThreadPool threadPool, ConcurrentMap<String, LogExecutor> logExecutors) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.loggingInterval = Objects.requireNonNull(loggingInterval);
        this.clock = Objects.requireNonNull(clock);
        this.logExecutors = Objects.requireNonNull(logExecutors);
    }

    public void init() {
        cancellableTask.set(startRepeatingLogEmitter());
    }

    private Scheduler.Cancellable startRepeatingLogEmitter() {
        classLogger.debug(() -> Strings.format("Scheduling repeating log emitter with interval [%s]", loggingInterval));

        return threadPool.scheduleWithFixedDelay(this::emitRepeatedLogs, loggingInterval, threadPool.executor(UTILITY_THREAD_POOL_NAME));
    }

    private void emitRepeatedLogs() {
        if (isRunning.get() == false) {
            return;
        }

        final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

        writeLock.lock();
        try {
            for (var iter = logExecutors.values().iterator(); iter.hasNext();) {
                var executor = iter.next();
                executor.logRepeatedMessages();
                iter.remove();
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void execute(Logger logger, Level level, String message, Throwable t) {
        executeInternal(logger, level, message, t);
    }

    public void execute(Logger logger, Level level, String message) {
        executeInternal(logger, level, message, null);
    }

    private void executeInternal(Logger logger, Level level, String message, Throwable throwable) {
        if (isRunning.get() == false) {
            return;
        }

        final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

        readLock.lock();
        try {
            var logExecutor = logExecutors.compute(
                message,
                (key, value) -> Objects.requireNonNullElseGet(value, () -> new LogExecutor(clock, logger, level, message, throwable))
            );

            logExecutor.logFirstMessage();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void close() {
        isRunning.set(false);
        if (cancellableTask.get() != null) {
            cancellableTask.get().cancel();
        }

        clearLogExecutors();
    }

    private void clearLogExecutors() {
        final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            logExecutors.clear();
        } finally {
            writeLock.unlock();
        }
    }

    private static class LogExecutor {
        // -1 here because we need to determine if we haven't logged the first time
        // After the first time we'll set it to 0, then the thread that runs on an interval
        // needs to know if there are any repeated message, if it sees 0, it knows there are none
        // and skips emitting the message again.
        private static final long INITIAL_LOG_COUNTER_VALUE = -1;

        private final AtomicLong skippedLogCalls = new AtomicLong(INITIAL_LOG_COUNTER_VALUE);
        private final AtomicReference<Instant> timeOfLastLogCall;
        private final Clock clock;
        private final Logger throttledLogger;
        private final Level level;
        private final String originalMessage;
        private final Throwable throwable;

        LogExecutor(Clock clock, Logger logger, Level level, String originalMessage, @Nullable Throwable throwable) {
            this.clock = Objects.requireNonNull(clock);
            timeOfLastLogCall = new AtomicReference<>(Instant.now(this.clock));
            this.throttledLogger = Objects.requireNonNull(logger);
            this.level = Objects.requireNonNull(level);
            this.originalMessage = Objects.requireNonNull(originalMessage);
            this.throwable = throwable;
        }

        void logRepeatedMessages() {
            var numSkippedLogCalls = skippedLogCalls.get();
            if (hasRepeatedLogsToEmit(numSkippedLogCalls) == false) {
                return;
            }

            String enrichedMessage;
            if (numSkippedLogCalls == 1) {
                enrichedMessage = Strings.format("%s, repeated 1 time, last message at [%s]", originalMessage, timeOfLastLogCall.get());
            } else {
                enrichedMessage = Strings.format(
                    "%s, repeated %s times, last message at [%s]",
                    originalMessage,
                    skippedLogCalls,
                    timeOfLastLogCall.get()
                );
            }

            log(enrichedMessage);
        }

        private void log(String enrichedMessage) {
            LogBuilder builder = throttledLogger.atLevel(level);
            if (throwable != null) {
                builder = builder.withThrowable(throwable);
            }

            builder.log(enrichedMessage);
        }

        private static boolean hasRepeatedLogsToEmit(long numSkippedLogCalls) {
            return numSkippedLogCalls > 0;
        }

        void logFirstMessage() {
            timeOfLastLogCall.set(Instant.now(this.clock));

            if (hasLoggedOriginalMessage(skippedLogCalls.getAndIncrement()) == false) {
                log(originalMessage);
            }
        }

        private static boolean hasLoggedOriginalMessage(long numSkippedLogCalls) {
            // a negative value indicates that we haven't yet logged the original message
            return numSkippedLogCalls >= 0;
        }
    }
}
