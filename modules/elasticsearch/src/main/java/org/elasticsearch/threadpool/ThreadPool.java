/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.threadpool;

import org.elasticsearch.common.unit.TimeValue;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author kimchy (shay.banon)
 */
public interface ThreadPool extends Executor {

    ThreadPoolInfo info();

    ThreadPoolStats stats();

    /**
     * The minimum number of threads in the thread pool.
     */
    int getMinThreads();

    /**
     * The maximum number of threads in the thread pool.
     */
    int getMaxThreads();

    /**
     * The size of scheduler threads.
     */
    int getSchedulerThreads();

    /**
     * Returns the current number of threads in the pool.
     *
     * @return the number of threads
     */
    int getPoolSize();

    /**
     * Returns the approximate number of threads that are actively
     * executing tasks.
     *
     * @return the number of threads
     */
    int getActiveCount();

    /**
     * The size of the scheduler thread pool.
     */
    int getSchedulerPoolSize();

    /**
     * The approximate number of threads that are actively executing scheduled
     * tasks.
     */
    int getSchedulerActiveCount();

    /**
     * Returns <tt>true</tt> if the thread pool has started.
     */
    boolean isStarted();

    /**
     * Returns a cached executor that will always allocate threads.
     */
    Executor cached();

    void shutdownNow();

    /**
     * Initiates an orderly shutdown in which previously submitted
     * tasks are executed, but no new tasks will be accepted.
     * Invocation has no additional effect if already shut down.
     */
    void shutdown();

    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;

    void execute(Runnable command);

    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit);

    public ScheduledFuture<?> schedule(Runnable command, TimeValue delay);

    ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, TimeValue interval);
}
