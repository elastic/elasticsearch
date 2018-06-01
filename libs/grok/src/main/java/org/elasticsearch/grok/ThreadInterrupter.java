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
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

/**
 * Protects against long running operations that happen between the register and de-register invocations.
 * Threads that invoke {@link #register()}, but take too long to invoke the {@link #deregister()} method
 * will be interrupted.
 *
 * This is needed for Joni's {@link org.joni.Matcher#search(int, int, int)} method, because
 * it can end up spinning endlessly if the regular expression is too complex. Joni has checks
 * that that for every 30k iterations it checks if the current thread is interrupted and if so
 * returns {@link org.joni.Matcher#INTERRUPTED}.
 */
public interface ThreadInterrupter {
    
    /**
     * Registers the current thread and interrupts the current thread
     * if the takes too long for this thread to invoke {@link #deregister()}.
     */
    void register();
    
    /**
     * De-registers the current thread and prevents it from being interrupted.
     */
    void deregister();
    
    /**
     * Returns an implementation that checks for each fixed interval if there are threads that have invoked {@link #register()}
     * and not {@link #deregister()} and have been in this state for longer than the specified max execution interval and
     * then interrupts these threads.
     *
     * @param interval              The fixed interval to check if there are threads to interrupt
     * @param maxExecutionTime      The time a thread has the execute an operation.
     * @param relativeTimeSupplier  A supplier that returns relative time
     * @param scheduler             A scheduler that is able to execute a command for each fixed interval
     */
    static ThreadInterrupter newInstance(long interval,
                                         long maxExecutionTime,
                                         LongSupplier relativeTimeSupplier,
                                         BiFunction<Long, Runnable, ScheduledFuture<?>> scheduler) {
        return new Default(interval, maxExecutionTime, relativeTimeSupplier, scheduler);
    }
    
    /**
     * @return A noop implementation that does not interrupt threads and is useful for testing and pre-defined grok expressions.
     */
    static ThreadInterrupter noop() {
        return new Noop();
    }
    
    class Noop implements ThreadInterrupter {
        
        private Noop() {
        }
        
        @Override
        public void register() {
        }
        
        @Override
        public void deregister() {
        }
    }
    
    class Default implements ThreadInterrupter {
        
        private final long interval;
        private final long maxExecutionTime;
        private final LongSupplier relativeTimeSupplier;
        private final BiFunction<Long, Runnable, ScheduledFuture<?>> scheduler;
        final ConcurrentHashMap<Thread, Long> registry = new ConcurrentHashMap<>();
        
        private Default(long interval,
                        long maxExecutionTime,
                        LongSupplier relativeTimeSupplier,
                        BiFunction<Long, Runnable, ScheduledFuture<?>> scheduler) {
            this.interval = interval;
            this.maxExecutionTime = maxExecutionTime;
            this.relativeTimeSupplier = relativeTimeSupplier;
            this.scheduler = scheduler;
            scheduler.apply(interval, this::interruptLongRunningExecutions);
        }
        
        public void register() {
            Long previousValue = registry.put(Thread.currentThread(), relativeTimeSupplier.getAsLong());
            assert previousValue == null;
        }
        
        public void deregister() {
            Long previousValue = registry.remove(Thread.currentThread());
            assert previousValue != null;
        }
        
        private void interruptLongRunningExecutions() {
            try {
                final long currentRelativeTime = relativeTimeSupplier.getAsLong();
                for (Map.Entry<Thread, Long> entry : registry.entrySet()) {
                    long threadTime = entry.getValue();
                    if ((currentRelativeTime - threadTime) > maxExecutionTime) {
                        entry.getKey().interrupt();
                        // not removing the entry here, this happens in the deregister() method.
                    }
                }
            } finally {
                scheduler.apply(interval, this::interruptLongRunningExecutions);
            }
        }
        
    }
    
}
