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

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
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
        Setting.intSetting("processors", Math.min(32, Runtime.getRuntime().availableProcessors()), 1, Property.NodeScope);

    /**
     * Returns the number of processors available but at most <tt>32</tt>.
     */
    public static int boundedNumberOfProcessors(Settings settings) {
        /* This relates to issues where machines with large number of cores
         * ie. >= 48 create too many threads and run into OOM see #3478
         * We just use an 32 core upper-bound here to not stress the system
         * too much with too many created threads */
        return PROCESSORS_SETTING.get(settings);
    }

    public static PrioritizedEsThreadPoolExecutor newSinglePrioritizing(String name, ThreadFactory threadFactory, ThreadContext contextHolder) {
        return new PrioritizedEsThreadPoolExecutor(name, 1, 1, 0L, TimeUnit.MILLISECONDS, threadFactory, contextHolder);
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

    public static String threadName(Settings settings, String ... names) {
        String namePrefix =
                Arrays
                        .stream(names)
                        .filter(name -> name != null)
                        .collect(Collectors.joining(".", "[", "]"));
        return threadName(settings, namePrefix);
    }

    public static String threadName(Settings settings, String namePrefix) {
        String nodeName = settings.get("node.name");
        if (nodeName == null) {
            return threadName("", namePrefix);
        } else {
            return threadName(nodeName, namePrefix);
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
