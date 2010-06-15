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

import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.*;

/**
 * Factory and utility methods for handling {@link DynamicThreadPoolExecutor}.
 *
 * @author kimchy (shay.banon)
 */
public class DynamicExecutors {
    /**
     * Creates a thread pool that creates new threads as needed, but will reuse
     * previously constructed threads when they are available. Calls to
     * <tt>execute</tt> will reuse previously constructed threads if
     * available. If no existing thread is available, a new thread will be
     * created and added to the pool. No more than <tt>max</tt> threads will
     * be created. Threads that have not been used for a <tt>keepAlive</tt>
     * timeout are terminated and removed from the cache. Thus, a pool that
     * remains idle for long enough will not consume any resources other than
     * the <tt>min</tt> specified.
     *
     * @param min           the number of threads to keep in the pool, even if they are
     *                      idle.
     * @param max           the maximum number of threads to allow in the pool.
     * @param keepAliveTime when the number of threads is greater than the min,
     *                      this is the maximum time that excess idle threads will wait
     *                      for new tasks before terminating (in milliseconds).
     * @return the newly created thread pool
     */
    public static ExecutorService newScalingThreadPool(int min, int max, long keepAliveTime) {
        return newScalingThreadPool(min, max, keepAliveTime, Executors.defaultThreadFactory());
    }

    /**
     * Creates a thread pool, same as in
     * {@link #newScalingThreadPool(int, int, long)}, using the provided
     * ThreadFactory to create new threads when needed.
     *
     * @param min           the number of threads to keep in the pool, even if they are
     *                      idle.
     * @param max           the maximum number of threads to allow in the pool.
     * @param keepAliveTime when the number of threads is greater than the min,
     *                      this is the maximum time that excess idle threads will wait
     *                      for new tasks before terminating (in milliseconds).
     * @param threadFactory the factory to use when creating new threads.
     * @return the newly created thread pool
     */
    public static ExecutorService newScalingThreadPool(int min, int max, long keepAliveTime, ThreadFactory threadFactory) {
        DynamicThreadPoolExecutor.DynamicQueue<Runnable> queue = new DynamicThreadPoolExecutor.DynamicQueue<Runnable>();
        ThreadPoolExecutor executor = new DynamicThreadPoolExecutor(min, max, keepAliveTime, TimeUnit.MILLISECONDS, queue, threadFactory);
        executor.setRejectedExecutionHandler(new DynamicThreadPoolExecutor.ForceQueuePolicy());
        queue.setThreadPoolExecutor(executor);
        return executor;
    }

    /**
     * Creates a thread pool similar to that constructed by
     * {@link #newScalingThreadPool(int, int, long)}, but blocks the call to
     * <tt>execute</tt> if the queue has reached it's capacity, and all
     * <tt>max</tt> threads are busy handling requests.
     * <p/>
     * If the wait time of this queue has elapsed, a
     * {@link RejectedExecutionException} will be thrown.
     *
     * @param min           the number of threads to keep in the pool, even if they are
     *                      idle.
     * @param max           the maximum number of threads to allow in the pool.
     * @param keepAliveTime when the number of threads is greater than the min,
     *                      this is the maximum time that excess idle threads will wait
     *                      for new tasks before terminating (in milliseconds).
     * @param capacity      the fixed capacity of the underlying queue (resembles
     *                      backlog).
     * @param waitTime      the wait time (in milliseconds) for space to become
     *                      available in the queue.
     * @return the newly created thread pool
     */
    public static ExecutorService newBlockingThreadPool(int min, int max, long keepAliveTime, int capacity, long waitTime) {
        return newBlockingThreadPool(min, max, keepAliveTime, capacity, waitTime, Executors.defaultThreadFactory());
    }

    /**
     * Creates a thread pool, same as in
     * {@link #newBlockingThreadPool(int, int, long, int, long)}, using the
     * provided ThreadFactory to create new threads when needed.
     *
     * @param min           the number of threads to keep in the pool, even if they are
     *                      idle.
     * @param max           the maximum number of threads to allow in the pool.
     * @param keepAliveTime when the number of threads is greater than the min,
     *                      this is the maximum time that excess idle threads will wait
     *                      for new tasks before terminating (in milliseconds).
     * @param capacity      the fixed capacity of the underlying queue (resembles
     *                      backlog).
     * @param waitTime      the wait time (in milliseconds) for space to become
     *                      available in the queue.
     * @param threadFactory the factory to use when creating new threads.
     * @return the newly created thread pool
     */
    public static ExecutorService newBlockingThreadPool(int min, int max,
                                                        long keepAliveTime, int capacity, long waitTime,
                                                        ThreadFactory threadFactory) {
        DynamicThreadPoolExecutor.DynamicQueue<Runnable> queue = new DynamicThreadPoolExecutor.DynamicQueue<Runnable>(capacity);
        ThreadPoolExecutor executor = new DynamicThreadPoolExecutor(min, max, keepAliveTime, TimeUnit.MILLISECONDS, queue, threadFactory);
        executor.setRejectedExecutionHandler(new DynamicThreadPoolExecutor.TimedBlockingPolicy(waitTime));
        queue.setThreadPoolExecutor(executor);
        return executor;
    }

    public static ThreadFactory daemonThreadFactory(Settings settings, String namePrefix) {
        String name = settings.get("name");
        if (name == null) {
            name = "elasticsearch";
        } else {
            name = "elasticsearch[" + name + "]";
        }
        return daemonThreadFactory(name + namePrefix);
    }

    /**
     * A priority based thread factory, for all Thread priority constants:
     * <tt>Thread.MIN_PRIORITY, Thread.NORM_PRIORITY, Thread.MAX_PRIORITY</tt>;
     * <p/>
     * This factory is used instead of Executers.DefaultThreadFactory to allow
     * manipulation of priority and thread owner name.
     *
     * @param namePrefix a name prefix for this thread
     * @return a thread factory based on given priority.
     */
    public static ThreadFactory daemonThreadFactory(String namePrefix) {
        final ThreadFactory f = Executors.defaultThreadFactory();
        final String o = namePrefix + "-";

        return new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = f.newThread(r);

                /*
                 * Thread name: owner-pool-N-thread-M, where N is the sequence
                 * number of this factory, and M is the sequence number of the
                 * thread created by this factory.
                 */
                t.setName(o + t.getName());

                /* override default definition t.setDaemon(false); */
                t.setDaemon(true);

                return t;
            }
        };
    }

    /**
     * A priority based thread factory, for all Thread priority constants:
     * <tt>Thread.MIN_PRIORITY, Thread.NORM_PRIORITY, Thread.MAX_PRIORITY</tt>;
     * <p/>
     * This factory is used instead of Executers.DefaultThreadFactory to allow
     * manipulation of priority and thread owner name.
     *
     * @param priority   The priority to be assigned to each thread;
     *                   can be either <tt>Thread.MIN_PRIORITY, Thread.NORM_PRIORITY</tt>
     *                   or Thread.MAX_PRIORITY.
     * @param namePrefix a name prefix for this thread
     * @return a thread factory based on given priority.
     */
    public static ThreadFactory priorityThreadFactory(int priority, String namePrefix) {
        final ThreadFactory f = DynamicExecutors.daemonThreadFactory(namePrefix);
        final int p = priority;

        return new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = f.newThread(r);

                /* override default thread priority of Thread.NORM_PRIORITY */
                if (p != Thread.NORM_PRIORITY)
                    t.setPriority(p);

                return t;
            }
        };
    }

    /**
     * Cannot instantiate.
     */
    private DynamicExecutors() {
    }
}
