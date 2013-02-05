/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.Priority;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A prioritizing executor which uses a priority queue as a work queue. The jobs that will be submitted will be treated
 * as {@link PrioritizedRunnable} and/or {@link PrioritizedCallable}, those tasks that are not instances of these two will
 * be wrapped and assign a default {@link Priority#NORMAL} priority.
 *
 * Note, if two tasks have the same priority, the first to arrive will be executed first (FIFO style).
 */
public class PrioritizedEsThreadPoolExecutor extends EsThreadPoolExecutor {

    private AtomicLong tieBreaker = new AtomicLong(Long.MIN_VALUE);

    public PrioritizedEsThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, new PriorityBlockingQueue<Runnable>(), threadFactory);
    }

    public PrioritizedEsThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, new PriorityBlockingQueue<Runnable>(), threadFactory, handler);
    }

    public PrioritizedEsThreadPoolExecutor(int corePoolSize, int initialWorkQueuSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, new PriorityBlockingQueue<Runnable>(initialWorkQueuSize), threadFactory, handler);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        if (!(runnable instanceof PrioritizedRunnable)) {
            runnable = PrioritizedRunnable.wrap(runnable, Priority.NORMAL);
        }
        return new PrioritizedFutureTask<T>((PrioritizedRunnable) runnable, value, tieBreaker.incrementAndGet());
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        if (!(callable instanceof PrioritizedCallable)) {
            callable = PrioritizedCallable.wrap(callable, Priority.NORMAL);
        }
        return new PrioritizedFutureTask<T>((PrioritizedCallable<T>) callable, tieBreaker.incrementAndGet());
    }

    /**
     *
     */
    static class PrioritizedFutureTask<T> extends FutureTask<T> implements Comparable<PrioritizedFutureTask> {

        private final Priority priority;
        private final long tieBreaker;

        public PrioritizedFutureTask(PrioritizedRunnable runnable, T value, long tieBreaker) {
            super(runnable, value);
            this.priority = runnable.priority();
            this.tieBreaker = tieBreaker;
        }

        public PrioritizedFutureTask(PrioritizedCallable<T> callable, long tieBreaker) {
            super(callable);
            this.priority = callable.priority();
            this.tieBreaker = tieBreaker;
        }

        @Override
        public int compareTo(PrioritizedFutureTask pft) {
            int res = priority.compareTo(pft.priority);
            if (res != 0) {
                return res;
            }
            return tieBreaker < pft.tieBreaker ? -1 : 1;
        }
    }
}
