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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class EsExecutors {

    /**
     * Setting to manually control the number of allocated processors. This setting is used to adjust thread pool sizes per node. The
     * default value is {@link Runtime#availableProcessors()} but should be manually controlled if not all processors on the machine are
     * available to Elasticsearch (e.g., because of CPU limits).
     */
    public static final Setting<Integer> NODE_PROCESSORS_SETTING = Setting.intSetting(
        "node.processors",
        Runtime.getRuntime().availableProcessors(),
        1,
        Runtime.getRuntime().availableProcessors(),
        Property.NodeScope);

    /**
     * Returns the number of allocated processors. Defaults to {@link Runtime#availableProcessors()} but can be overridden by passing a
     * {@link Settings} instance with the key {@code node.processors} set to the desired value.
     *
     * @param settings a {@link Settings} instance from which to derive the allocated processors
     * @return the number of allocated processors
     */
    public static int allocatedProcessors(final Settings settings) {
        return NODE_PROCESSORS_SETTING.get(settings);
    }

    public static PrioritizedEsThreadPoolExecutor newSinglePrioritizing(String name, ThreadFactory threadFactory,
                                                                        ThreadContext contextHolder, ScheduledExecutorService timer) {
        return new PrioritizedEsThreadPoolExecutor(name, 1, 1, 0L, TimeUnit.MILLISECONDS, threadFactory, contextHolder, timer);
    }

    public static EsThreadPoolExecutor newScaling(String name, int min, int max, long keepAliveTime, TimeUnit unit,
                                                  ThreadFactory threadFactory, ThreadContext contextHolder) {
        ExecutorScalingQueue<Runnable> queue = new ExecutorScalingQueue<>();
        EsThreadPoolExecutor executor =
            new EsThreadPoolExecutor(name, min, max, keepAliveTime, unit, queue, threadFactory, new ForceQueuePolicy(), contextHolder);
        queue.executor = executor;
        return executor;
    }

    public static EsThreadPoolExecutor newFixed(String name, int size, int queueCapacity,
                                                ThreadFactory threadFactory, ThreadContext contextHolder, boolean trackEWMA) {
        BlockingQueue<Runnable> queue;
        if (queueCapacity < 0) {
            queue = ConcurrentCollections.newBlockingQueue();
        } else {
            queue = new SizeBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(), queueCapacity);
        }
        if (trackEWMA) {
            return new EWMATrackingEsThreadPoolExecutor(name, size, size, 0, TimeUnit.MILLISECONDS,
                queue, TimedRunnable::new, threadFactory, new EsAbortPolicy(), contextHolder);
        } else {
            return new EsThreadPoolExecutor(name, size, size, 0, TimeUnit.MILLISECONDS,
                queue, threadFactory, new EsAbortPolicy(), contextHolder);
        }
    }

    /**
     * Checks if the runnable arose from asynchronous submission of a task to an executor. If an uncaught exception was thrown
     * during the execution of this task, we need to inspect this runnable and see if it is an error that should be propagated
     * to the uncaught exception handler.
     *
     * @param runnable the runnable to inspect, should be a RunnableFuture
     * @return non fatal exception or null if no exception.
     */
    public static Throwable rethrowErrors(Runnable runnable) {
        if (runnable instanceof RunnableFuture) {
            assert ((RunnableFuture) runnable).isDone();
            try {
                ((RunnableFuture) runnable).get();
            } catch (final Exception e) {
                /*
                 * In theory, Future#get can only throw a cancellation exception, an interrupted exception, or an execution
                 * exception. We want to ignore cancellation exceptions, restore the interrupt status on interrupted exceptions, and
                 * inspect the cause of an execution. We are going to be extra paranoid here though and completely unwrap the
                 * exception to ensure that there is not a buried error anywhere. We assume that a general exception has been
                 * handled by the executed task or the task submitter.
                 */
                assert e instanceof CancellationException
                    || e instanceof InterruptedException
                    || e instanceof ExecutionException : e;
                final Optional<Error> maybeError = ExceptionsHelper.maybeError(e);
                if (maybeError.isPresent()) {
                    // throw this error where it will propagate to the uncaught exception handler
                    throw maybeError.get();
                }
                if (e instanceof InterruptedException) {
                    // restore the interrupt status
                    Thread.currentThread().interrupt();
                }
                if (e instanceof ExecutionException) {
                    return e.getCause();
                }
            }
        }

        return null;
    }

    private static final class DirectExecutorService extends AbstractExecutorService {

        @SuppressForbidden(reason = "properly rethrowing errors, see EsExecutors.rethrowErrors")
        DirectExecutorService() {
            super();
        }

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
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(Runnable command) {
            command.run();
            rethrowErrors(command);
        }
    }

    private static final ExecutorService DIRECT_EXECUTOR_SERVICE = new DirectExecutorService();

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
            // TODO this should only be allowed in tests
            return threadName("", namePrefix);
        }
    }

    public static String threadName(final String nodeName, final String namePrefix) {
        // TODO missing node names should only be allowed in tests
        return "elasticsearch" + (nodeName.isEmpty() ? "" : "[") + nodeName + (nodeName.isEmpty() ? "" : "]") + "[" + namePrefix + "]";
    }

    public static ThreadFactory daemonThreadFactory(Settings settings, String namePrefix) {
        return daemonThreadFactory(threadName(settings, namePrefix));
    }

    public static ThreadFactory daemonThreadFactory(String nodeName, String namePrefix) {
        assert nodeName != null && false == nodeName.isEmpty();
        return daemonThreadFactory(threadName(nodeName, namePrefix));
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
                // force queue policy should only be used with a scaling queue
                assert executor.getQueue() instanceof ExecutorScalingQueue;
                executor.getQueue().put(r);
            } catch (final InterruptedException e) {
                // a scaling queue never blocks so a put to it can never be interrupted
                throw new AssertionError(e);
            }
        }

        @Override
        public long rejected() {
            return 0;
        }

    }

}
