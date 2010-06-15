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

package org.elasticsearch.common.util.concurrent;


import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An {@link ExecutorService} that executes each submitted task using one of
 * possibly several pooled threads, normally configured using
 * {@link DynamicExecutors} factory methods.
 *
 * @author kimchy (shay.banon)
 */
public class DynamicThreadPoolExecutor extends ThreadPoolExecutor {
    /**
     * number of threads that are actively executing tasks
     */
    private final AtomicInteger activeCount = new AtomicInteger();

    public DynamicThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
                                     long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue,
                                     ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    @Override public int getActiveCount() {
        return activeCount.get();
    }

    @Override protected void beforeExecute(Thread t, Runnable r) {
        activeCount.incrementAndGet();
    }

    @Override protected void afterExecute(Runnable r, Throwable t) {
        activeCount.decrementAndGet();
    }

    /**
     * Much like a {@link SynchronousQueue} which acts as a rendezvous channel. It
     * is well suited for handoff designs, in which a tasks is only queued if there
     * is an available thread to pick it up.
     * <p/>
     * This queue is correlated with a thread-pool, and allows insertions to the
     * queue only if there is a free thread that can poll this task. Otherwise, the
     * task is rejected and the decision is left up to one of the
     * {@link RejectedExecutionHandler} policies:
     * <ol>
     * <li> {@link ForceQueuePolicy} - forces the queue to accept the rejected task. </li>
     * <li> {@link TimedBlockingPolicy} - waits for a given time for the task to be
     * executed.</li>
     * </ol>
     *
     * @author kimchy (Shay Banon)
     */
    public static class DynamicQueue<E> extends LinkedBlockingQueue<E> {
        private static final long serialVersionUID = 1L;

        /**
         * The executor this Queue belongs to
         */
        private transient ThreadPoolExecutor executor;

        /**
         * Creates a <tt>DynamicQueue</tt> with a capacity of
         * {@link Integer#MAX_VALUE}.
         */
        public DynamicQueue() {
            super();
        }

        /**
         * Creates a <tt>DynamicQueue</tt> with the given (fixed) capacity.
         *
         * @param capacity the capacity of this queue.
         */
        public DynamicQueue(int capacity) {
            super(capacity);
        }

        /**
         * Sets the executor this queue belongs to.
         */
        public void setThreadPoolExecutor(ThreadPoolExecutor executor) {
            this.executor = executor;
        }

        /**
         * Inserts the specified element at the tail of this queue if there is at
         * least one available thread to run the current task. If all pool threads
         * are actively busy, it rejects the offer.
         *
         * @param o the element to add.
         * @return <tt>true</tt> if it was possible to add the element to this
         *         queue, else <tt>false</tt>
         * @see ThreadPoolExecutor#execute(Runnable)
         */
        @Override
        public boolean offer(E o) {
            int allWorkingThreads = executor.getActiveCount() + super.size();
            return allWorkingThreads < executor.getPoolSize() && super.offer(o);
        }
    }

    /**
     * A handler for rejected tasks that adds the specified element to this queue,
     * waiting if necessary for space to become available.
     */
    public static class ForceQueuePolicy implements RejectedExecutionHandler {
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            try {
                executor.getQueue().put(r);
            } catch (InterruptedException e) {
                //should never happen since we never wait
                throw new RejectedExecutionException(e);
            }
        }
    }

    /**
     * A handler for rejected tasks that inserts the specified element into this
     * queue, waiting if necessary up to the specified wait time for space to become
     * available.
     */
    public static class TimedBlockingPolicy implements RejectedExecutionHandler {
        private final long waitTime;

        /**
         * @param waitTime wait time in milliseconds for space to become available.
         */
        public TimedBlockingPolicy(long waitTime) {
            this.waitTime = waitTime;
        }

        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            try {
                boolean successful = executor.getQueue().offer(r, waitTime, TimeUnit.MILLISECONDS);
                if (!successful)
                    throw new RejectedExecutionException("Rejected execution after waiting "
                            + waitTime + " ms for task [" + r.getClass() + "] to be executed.");
            } catch (InterruptedException e) {
                throw new RejectedExecutionException(e);
            }
        }
    }
}
