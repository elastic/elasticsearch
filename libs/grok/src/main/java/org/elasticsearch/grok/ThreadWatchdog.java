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
package org.elasticsearch.grok;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * Protects against long running operations that happen between the register and unregister invocations.
 * Threads that invoke {@link #register()}, but take too long to invoke the {@link #unregister()} method
 * will be interrupted.
 *
 * This is needed for Joni's {@link org.joni.Matcher#search(int, int, int)} method, because
 * it can end up spinning endlessly if the regular expression is too complex. Joni has checks
 * that for every 30k iterations it checks if the current thread is interrupted and if so
 * returns {@link org.joni.Matcher#INTERRUPTED}.
 */
public interface ThreadWatchdog {
    
    /**
     * Registers the current thread and interrupts the current thread
     * if the takes too long for this thread to invoke {@link #unregister()}.
     */
    void register();
    
    /**
     * @return The maximum allowed time in milliseconds for a thread to invoke {@link #unregister()}
     *         after {@link #register()} has been invoked before this ThreadWatchDog starts to interrupting that thread.
     */
    long maxExecutionTimeInMillis();
    
    /**
     * Unregisters the current thread and prevents it from being interrupted.
     */
    void unregister();
    
    /**
     * Returns an implementation that checks for each fixed interval if there are threads that have invoked {@link #register()}
     * and not {@link #unregister()} and have been in this state for longer than the specified max execution interval and
     * then interrupts these threads.
     *
     * @param interval              The fixed interval to check if there are threads to interrupt
     * @param maxExecutionTime      The time a thread has the execute an operation.
     * @param relativeTimeSupplier  A supplier that returns relative time
     * @param scheduler             A scheduler that is able to execute a command for each fixed interval
     */
    static ThreadWatchdog newInstance(long interval,
                                      long maxExecutionTime,
                                      LongSupplier relativeTimeSupplier,
                                      BiConsumer<Long, Runnable> scheduler) {
        return new Default(interval, maxExecutionTime, relativeTimeSupplier, scheduler);
    }
    
    /**
     * @return A noop implementation that does not interrupt threads and is useful for testing and pre-defined grok expressions.
     */
    static ThreadWatchdog noop() {
        return Noop.INSTANCE;
    }
    
    class Noop implements ThreadWatchdog {
    
        private static final Noop INSTANCE = new Noop();
        
        private Noop() {
        }
    
        @Override
        public void register() {
        }
    
        @Override
        public long maxExecutionTimeInMillis() {
            return Long.MAX_VALUE;
        }
        
        @Override
        public void unregister() {
        }
    }
    
    class Default implements ThreadWatchdog {
        
        private final long interval;
        private final long maxExecutionTime;
        private final LongSupplier relativeTimeSupplier;
        private final BiConsumer<Long, Runnable> scheduler;
        private final AtomicInteger registered = new AtomicInteger(0);
        private final AtomicBoolean running = new AtomicBoolean(false);
        final ConcurrentHashMap<Thread, Long> registry = new ConcurrentHashMap<>();
        
        private Default(long interval,
                        long maxExecutionTime,
                        LongSupplier relativeTimeSupplier,
                        BiConsumer<Long, Runnable> scheduler) {
            this.interval = interval;
            this.maxExecutionTime = maxExecutionTime;
            this.relativeTimeSupplier = relativeTimeSupplier;
            this.scheduler = scheduler;
        }
        
        public void register() {
            registered.getAndIncrement();
            Long previousValue = registry.put(Thread.currentThread(), relativeTimeSupplier.getAsLong());
            if (running.compareAndSet(false, true) == true) {
                scheduler.accept(interval, this::interruptLongRunningExecutions);
            }
            assert previousValue == null;
        }
    
        @Override
        public long maxExecutionTimeInMillis() {
            return maxExecutionTime;
        }
    
        public void unregister() {
            Long previousValue = registry.remove(Thread.currentThread());
            registered.decrementAndGet();
            assert previousValue != null;
        }
        
        private void interruptLongRunningExecutions() {
            final long currentRelativeTime = relativeTimeSupplier.getAsLong();
            for (Map.Entry<Thread, Long> entry : registry.entrySet()) {
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
