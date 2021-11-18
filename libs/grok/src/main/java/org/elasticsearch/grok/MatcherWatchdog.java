/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.grok;

import org.joni.Matcher;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * Protects against long running operations that happen between the register and unregister invocations.
 * Threads that invoke {@link #register(Matcher)}, but take too long to invoke the {@link #unregister(Matcher)} method
 * will be interrupted.
 *
 * This is needed for Joni's {@link org.joni.Matcher#search(int, int, int)} method, because
 * it can end up spinning endlessly if the regular expression is too complex. Joni has checks
 * that for every 30k iterations it checks if the current thread is interrupted and if so
 * returns {@link org.joni.Matcher#INTERRUPTED}.
 */
public interface MatcherWatchdog {

    /**
     * Registers the current matcher and interrupts the this matcher
     * if the takes too long for this thread to invoke {@link #unregister(Matcher)}.
     *
     * @param matcher The matcher to register
     */
    void register(Matcher matcher);

    /**
     * @return The maximum allowed time in milliseconds for a thread to invoke {@link #unregister(Matcher)}
     *         after {@link #register(Matcher)} has been invoked before this ThreadWatchDog starts to interrupting that thread.
     */
    long maxExecutionTimeInMillis();

    /**
     * Unregisters the current matcher and prevents it from being interrupted.
     *
     * @param matcher The matcher to unregister
     */
    void unregister(Matcher matcher);

    /**
     * Returns an implementation that checks for each fixed interval if there are threads that have invoked {@link #register(Matcher)}
     * and not {@link #unregister(Matcher)} and have been in this state for longer than the specified max execution interval and
     * then interrupts these threads.
     *
     * @param interval              The fixed interval to check if there are threads to interrupt
     * @param maxExecutionTime      The time a thread has the execute an operation.
     * @param relativeTimeSupplier  A supplier that returns relative time
     * @param scheduler             A scheduler that is able to execute a command for each fixed interval
     */
    static MatcherWatchdog newInstance(
        long interval,
        long maxExecutionTime,
        LongSupplier relativeTimeSupplier,
        BiConsumer<Long, Runnable> scheduler
    ) {
        return new Default(interval, maxExecutionTime, relativeTimeSupplier, scheduler);
    }

    /**
     * @return A noop implementation that does not interrupt threads and is useful for testing and pre-defined grok expressions.
     */
    static MatcherWatchdog noop() {
        return Noop.INSTANCE;
    }

    class Noop implements MatcherWatchdog {

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

    class Default implements MatcherWatchdog {

        private final long interval;
        private final long maxExecutionTime;
        private final LongSupplier relativeTimeSupplier;
        private final BiConsumer<Long, Runnable> scheduler;
        private final AtomicInteger registered = new AtomicInteger(0);
        private final AtomicBoolean running = new AtomicBoolean(false);
        final ConcurrentHashMap<Matcher, Long> registry = new ConcurrentHashMap<>();

        private Default(long interval, long maxExecutionTime, LongSupplier relativeTimeSupplier, BiConsumer<Long, Runnable> scheduler) {
            this.interval = interval;
            this.maxExecutionTime = maxExecutionTime;
            this.relativeTimeSupplier = relativeTimeSupplier;
            this.scheduler = scheduler;
        }

        public void register(Matcher matcher) {
            registered.getAndIncrement();
            Long previousValue = registry.put(matcher, relativeTimeSupplier.getAsLong());
            if (running.compareAndSet(false, true)) {
                scheduler.accept(interval, this::interruptLongRunningExecutions);
            }
            assert previousValue == null;
        }

        @Override
        public long maxExecutionTimeInMillis() {
            return maxExecutionTime;
        }

        public void unregister(Matcher matcher) {
            Long previousValue = registry.remove(matcher);
            registered.decrementAndGet();
            assert previousValue != null;
        }

        private void interruptLongRunningExecutions() {
            final long currentRelativeTime = relativeTimeSupplier.getAsLong();
            for (Map.Entry<Matcher, Long> entry : registry.entrySet()) {
                if ((currentRelativeTime - entry.getValue()) > maxExecutionTime) {
                    entry.getKey().interrupt();
                    // not removing the entry here, this happens in the unregister() method.
                }
            }
            if (registered.get() > 0) {
                scheduler.accept(interval, this::interruptLongRunningExecutions);
            } else {
                running.set(false);
            }
        }

    }

}
