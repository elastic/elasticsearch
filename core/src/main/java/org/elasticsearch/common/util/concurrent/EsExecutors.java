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

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class EsExecutors {

    /**
     * Settings key to manually set the number of available processors.
     * This is used to adjust thread pools sizes etc. per node.
     */
    public static final Setting<Integer> PROCESSORS_SETTING =
        Setting.intSetting("processors", Runtime.getRuntime().availableProcessors(), 1, Property.NodeScope);

    /**
     * Returns the number of available processors. Defaults to
     * {@link Runtime#availableProcessors()} but can be overridden by passing a {@link Settings}
     * instance with the key "processors" set to the desired value.
     *
     * @param settings a {@link Settings} instance from which to derive the available processors
     * @return the number of available processors
     */
    public static int numberOfProcessors(final Settings settings) {
        return PROCESSORS_SETTING.get(settings);
    }

    public static PrioritizedEsThreadPoolExecutor newSinglePrioritizing(String name, ThreadFactory threadFactory, ThreadContext contextHolder, ScheduledExecutorService timer) {
        return new PrioritizedEsThreadPoolExecutor(name, 1, 1, 0L, TimeUnit.MILLISECONDS, threadFactory, contextHolder, timer);
    }

    public static EsThreadPoolExecutor newScaling(String name, int min, int max, long keepAliveTime, TimeUnit unit, ThreadFactory threadFactory, ThreadContext contextHolder) {
        ExecutorScalingQueue<Runnable> queue = new ExecutorScalingQueue<>();
        EsThreadPoolExecutor executor = new EsThreadPoolExecutor(name, min, max, keepAliveTime, unit, queue, threadFactory, new ForceQueuePolicy(), contextHolder);
        queue.executor = executor;
        return executor;
    }

    public static EsThreadPoolExecutor newFixed(String name, int size, int queueCapacity, ThreadFactory threadFactory, ThreadContext contextHolder) {
        BlockingQueue<Runnable> queue;
        if (queueCapacity < 0) {
            queue = ConcurrentCollections.newBlockingQueue();
        } else {
            queue = new SizeBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(), queueCapacity);
        }
        return new EsThreadPoolExecutor(name, size, size, 0, TimeUnit.MILLISECONDS, queue, threadFactory, new EsAbortPolicy(), contextHolder);
    }

    /**
     * Return a new executor that will automatically adjust the queue size based on queue throughput.
     *
     * @param size number of fixed threads to use for executing tasks
     * @param initialQueueCapacity initial size of the executor queue
     * @param minQueueSize minimum queue size that the queue can be adjusted to
     * @param maxQueueSize maximum queue size that the queue can be adjusted to
     * @param frameSize number of tasks during which stats are collected before adjusting queue size
     */
    public static EsThreadPoolExecutor newAutoQueueFixed(String name, int size, int initialQueueCapacity, int minQueueSize,
                                                         int maxQueueSize, int frameSize, TimeValue targetedResponseTime,
                                                         ThreadFactory threadFactory, ThreadContext contextHolder) {
        if (initialQueueCapacity <= 0) {
            throw new IllegalArgumentException("initial queue capacity for [" + name + "] executor must be positive, got: " +
                            initialQueueCapacity);
        }
        ResizableBlockingQueue<Runnable> queue =
                new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(), initialQueueCapacity);
        return new QueueResizingEsThreadPoolExecutor(name, size, size, 0, TimeUnit.MILLISECONDS,
                queue, minQueueSize, maxQueueSize, TimedRunnable::new, frameSize, targetedResponseTime, threadFactory,
                new EsAbortPolicy(), contextHolder);
    }

    private static final ExecutorService DIRECT_EXECUTOR_SERVICE = new AbstractExecutorService() {

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }

    };

    /**
     * Returns an {@link ExecutorService} that executes submitted tasks on the current thread. This executor service does not support being
     * shutdown.
     *
     * @return an {@link ExecutorService} that executes submitted tasks on the current thread
     */
    public static ExecutorService newDirectExecutorService() {
        return DIRECT_EXECUTOR_SERVICE;
    }

    public static String threadName(Settings settings, String ... names) {
        String namePrefix =
                Arrays
                        .stream(names)
                        .filter(name -> name != null)
                        .collect(Collectors.joining(".", "[", "]"));
        return threadName(settings, namePrefix);
    }

    public static String threadName(Settings settings, String namePrefix) {
        if (Node.NODE_NAME_SETTING.exists(settings)) {
            return threadName(Node.NODE_NAME_SETTING.get(settings), namePrefix);
        } else {
            return threadName("", namePrefix);
        }
    }

    public static String threadName(final String nodeName, final String namePrefix) {
        return "elasticsearch" + (nodeName.isEmpty() ? "" : "[") + nodeName + (nodeName.isEmpty() ? "" : "]") + "[" + namePrefix + "]";
    }

    public static ThreadFactory daemonThreadFactory(Settings settings, String namePrefix) {
        return daemonThreadFactory(threadName(settings, namePrefix));
    }

    public static ThreadFactory daemonThreadFactory(Settings settings, String ... names) {
        return daemonThreadFactory(threadName(settings, names));
    }

    public static ThreadFactory daemonThreadFactory(String namePrefix) {
        return new EsThreadFactory(namePrefix);
    }

    static class EsThreadFactory implements ThreadFactory {

        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;

        EsThreadFactory(String namePrefix) {
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

        ExecutorScalingQueue() {
        }

        @Override
        public boolean offer(E e) {
            // first try to transfer to a waiting worker thread
            if (!tryTransfer(e)) {
                // check if there might be spare capacity in the thread
                // pool executor
                int left = executor.getMaximumPoolSize() - executor.getCorePoolSize();
                if (left > 0) {
                    // reject queuing the task to force the thread pool
                    // executor to add a worker if it can; combined
                    // with ForceQueuePolicy, this causes the thread
                    // pool to always scale up to max pool size and we
                    // only queue when there is no spare capacity
                    return false;
                } else {
                    return super.offer(e);
                }
            } else {
                return true;
            }
        }

    }

    /**
     * A handler for rejected tasks that adds the specified element to this queue,
     * waiting if necessary for space to become available.
     */
    static class ForceQueuePolicy implements XRejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            try {
                executor.getQueue().put(r);
            } catch (InterruptedException e) {
                //should never happen since we never wait
                throw new EsRejectedExecutionException(e);
            }
        }

        @Override
        public long rejected() {
            return 0;
        }
    }

}
