/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

/**
 *
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
}
