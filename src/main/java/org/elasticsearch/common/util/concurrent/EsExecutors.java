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

import jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class EsExecutors {

    public static EsThreadPoolExecutor newScalingExecutorService(int min, int max, long keepAliveTime, TimeUnit unit,
                                                                 ThreadFactory threadFactory) {
        ExecutorScalingQueue<Runnable> queue = new ExecutorScalingQueue<Runnable>();
        // we force the execution, since we might run into concurrency issues in offer for ScalingBlockingQueue
        EsThreadPoolExecutor executor = new EsThreadPoolExecutor(min, max, keepAliveTime, unit, queue, threadFactory,
                new ForceQueuePolicy());
        queue.executor = executor;
        return executor;
    }

    public static EsThreadPoolExecutor newBlockingExecutorService(int min, int max, long keepAliveTime, TimeUnit unit,
                                                                  ThreadFactory threadFactory, int capacity,
                                                                  long waitTime, TimeUnit waitTimeUnit) {
        ExecutorBlockingQueue<Runnable> queue = new ExecutorBlockingQueue<Runnable>(capacity);
        EsThreadPoolExecutor executor = new EsThreadPoolExecutor(min, max, keepAliveTime, unit, queue, threadFactory,
                new TimedBlockingPolicy(waitTimeUnit.toMillis(waitTime)));
        queue.executor = executor;
        return executor;
    }

    public static String threadName(Settings settings, String namePrefix) {
        String name = settings.get("name");
        if (name == null) {
            name = "elasticsearch";
        } else {
            name = "elasticsearch[" + name + "]";
        }
        return name + "[" + namePrefix + "]";
    }

    public static ThreadFactory daemonThreadFactory(Settings settings, String namePrefix) {
        return daemonThreadFactory(threadName(settings, namePrefix));
    }

    public static ThreadFactory daemonThreadFactory(String namePrefix) {
        return new EsThreadFactory(namePrefix);
    }

    static class EsThreadFactory implements ThreadFactory {
        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;

        public EsThreadFactory(String namePrefix) {
            this.namePrefix = namePrefix;
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + "[T#" + threadNumber.getAndIncrement() + "]",
                    0);
            t.setDaemon(true);
            return t;
        }
    }

    /**
     * Cannot instantiate.
     */
    private EsExecutors() {
    }

    static class ExecutorScalingQueue<E> extends LinkedTransferQueue<E> {

        ThreadPoolExecutor executor;

        public ExecutorScalingQueue() {
        }

        @Override
        public boolean offer(E e) {
            int left = executor.getMaximumPoolSize() - executor.getCorePoolSize();
            if (!tryTransfer(e)) {
                if (left > 0) {
                    return false;
                } else {
                    return super.offer(e);
                }
            } else {
                return true;
            }
        }
    }

    static class ExecutorBlockingQueue<E> extends ArrayBlockingQueue<E> {

        ThreadPoolExecutor executor;

        ExecutorBlockingQueue(int capacity) {
            super(capacity);
        }

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
    static class ForceQueuePolicy implements RejectedExecutionHandler {
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            try {
                executor.getQueue().put(r);
            } catch (InterruptedException e) {
                //should never happen since we never wait
                throw new EsRejectedExecutionException(e);
            }
        }
    }

    /**
     * A handler for rejected tasks that inserts the specified element into this
     * queue, waiting if necessary up to the specified wait time for space to become
     * available.
     */
    static class TimedBlockingPolicy implements XRejectedExecutionHandler {

        private final CounterMetric rejected = new CounterMetric();
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
                if (!successful) {
                    rejected.inc();
                    throw new EsRejectedExecutionException();
                }
            } catch (InterruptedException e) {
                throw new EsRejectedExecutionException(e);
            }
        }

        @Override
        public long rejected() {
            return rejected.count();
        }
    }
}
