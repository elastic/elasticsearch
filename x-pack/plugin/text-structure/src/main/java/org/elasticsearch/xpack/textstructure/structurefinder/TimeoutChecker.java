/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.grok.MatcherWatchdog;
import org.joni.Matcher;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class can be used to keep track of when a long running operation started and
 * to check whether it has run for longer than permitted.
 *
 * An object should be constructed at the beginning of the operation and then the
 * {@link #check} method called periodically during the processing of the operation.
 *
 * This class does not use the {@link Thread#interrupt} mechanism because some other
 * methods already convert interruptions to other types of exceptions (for example
 * {@link Grok#captures}) and this would lead to non-uniform exception types and
 * misleading error messages in the event that the interrupt was handled by one of
 * these methods.  The code in the long running operation would still have to
 * periodically call {@link Thread#interrupt}, so it is not much more of an
 * inconvenience to have to periodically call this class's {@link #check} method.
 */
public class TimeoutChecker implements Closeable {

    private static final TimeoutCheckerWatchdog timeoutCheckerWatchdog = new TimeoutCheckerWatchdog();
    public static final MatcherWatchdog watchdog = timeoutCheckerWatchdog;

    private final String operation;
    private final TimeValue timeout;
    private final Thread checkedThread;
    private final ScheduledFuture<?> future;
    private boolean isClosed; // only accessed within synchronized methods
    private volatile boolean timeoutExceeded;

    /**
     * The constructor should be called at the start of the operation whose duration
     * is to be checked, as the timeout is measured relative to time of construction.
     * @param operation A description of the operation whose duration is to be checked.
     * @param timeout The timeout period.  If <code>null</code> then there is no timeout.
     * @param scheduler Used to schedule the timer.  This may be <code>null</code>
     *                  in the case where {@code timeout} is also <code>null</code>.
     */
    public TimeoutChecker(String operation, TimeValue timeout, ScheduledExecutorService scheduler) {
        this.operation = operation;
        this.timeout = timeout;
        this.checkedThread = Thread.currentThread();
        timeoutCheckerWatchdog.add(checkedThread, timeout);
        this.future = (timeout != null) ? scheduler.schedule(this::setTimeoutExceeded, timeout.nanos(), TimeUnit.NANOSECONDS) : null;
    }

    /**
     * Stops the timer if running.
     */
    @Override
    public synchronized void close() {
        if (isClosed) {
            return;
        }
        FutureUtils.cancel(future);
        timeoutCheckerWatchdog.remove(checkedThread);
        isClosed = true;
    }

    /**
     * Check whether the operation has been running longer than the permitted time.
     * @param where Which stage of the operation is currently in progress?
     * @throws ElasticsearchTimeoutException If the operation is found to have taken longer than the permitted time.
     */
    public void check(String where) {
        if (timeoutExceeded) {
            throw new ElasticsearchTimeoutException(
                "Aborting " + operation + " during [" + where + "] as it has taken longer than the timeout of [" + timeout + "]"
            );
        }
    }

    /**
     * Wrapper around {@link Grok#captures} that translates any timeout exception
     * to the style thrown by this class's {@link #check} method.
     * @param grok The grok pattern from which captures are to be extracted.
     * @param text The text to match and extract values from.
     * @param where Which stage of the operation is currently in progress?
     * @return A map containing field names and their respective coerced values that matched.
     * @throws ElasticsearchTimeoutException If the operation is found to have taken longer than the permitted time.
     */
    public Map<String, Object> grokCaptures(Grok grok, String text, String where) {
        try {
            return grok.captures(text);
        } finally {
            // If a timeout has occurred then this check will overwrite any timeout exception thrown by Grok.captures() and this
            // is intentional - the exception from this class makes more sense in the context of the find structure API
            check(where);
        }
    }

    private synchronized void setTimeoutExceeded() {
        // Even though close() cancels the timer, it's possible that it can already be running when close()
        // is called, so this check prevents the effects of this method occurring after close() returns
        if (isClosed) {
            return;
        }
        timeoutExceeded = true;
        timeoutCheckerWatchdog.interruptLongRunningThreadIfRegistered(checkedThread);
    }

    /**
     * An implementation of the type of watchdog used by the {@link Grok} class to interrupt
     * matching operations that take too long.  Rather than have a timeout per match operation
     * like the {@link MatcherWatchdog.Default} implementation, the interruption is governed by
     * a {@link TimeoutChecker} associated with the thread doing the matching.
     */
    static class TimeoutCheckerWatchdog implements MatcherWatchdog {

        final ConcurrentHashMap<Thread, WatchDogEntry> registry = new ConcurrentHashMap<>();

        void add(Thread thread, TimeValue timeout) {
            WatchDogEntry previousValue = registry.put(thread, new WatchDogEntry(timeout));
            assert previousValue == null;
        }

        @Override
        public synchronized void register(Matcher matcher) {
            WatchDogEntry value = registry.get(Thread.currentThread());
            if (value != null) {
                boolean wasFalse = value.registered.compareAndSet(false, true);
                assert wasFalse;
                value.matchers.add(matcher);
                if (value.isTimedOut()) {
                    matcher.interrupt();
                }
            }
        }

        @Override
        public long maxExecutionTimeInMillis() {
            WatchDogEntry value = registry.get(Thread.currentThread());
            return value != null ? value.timeout.getMillis() : Long.MAX_VALUE;
        }

        @Override
        public void unregister(Matcher matcher) {
            WatchDogEntry value = registry.get(Thread.currentThread());
            if (value != null) {
                boolean wasTrue = value.registered.compareAndSet(true, false);
                assert wasTrue;
                value.matchers.remove(matcher);
            }
        }

        void remove(Thread thread) {
            WatchDogEntry previousValue = registry.remove(thread);
            assert previousValue != null;
        }

        synchronized void interruptLongRunningThreadIfRegistered(Thread thread) {
            WatchDogEntry value = registry.get(thread);
            value.timedOut();
            if (value.registered.get()) {
                for (Matcher matcher : value.matchers) {
                    matcher.interrupt();
                }
            }
        }

        static class WatchDogEntry {

            final TimeValue timeout;
            final AtomicBoolean registered;
            final Collection<Matcher> matchers;
            boolean timedOut;

            WatchDogEntry(TimeValue timeout) {
                this.timeout = timeout;
                this.registered = new AtomicBoolean(false);
                this.matchers = new CopyOnWriteArrayList<>();
            }

            private void timedOut() {
                timedOut = true;
            }

            private boolean isTimedOut() {
                return timedOut;
            }
        }
    }
}
