/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.sniff;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Class responsible for sniffing nodes from some source (default is elasticsearch itself) and setting them to a provided instance of
 * {@link RestClient}. Must be created via {@link SnifferBuilder}, which allows to set all of the different options or rely on defaults.
 * A background task fetches the nodes through the {@link NodesSniffer} and sets them to the {@link RestClient} instance.
 * It is possible to perform sniffing on failure by creating a {@link SniffOnFailureListener} and providing it as an argument to
 * {@link RestClientBuilder#setFailureListener(RestClient.FailureListener)}. The Sniffer implementation needs to be lazily set to the
 * previously created SniffOnFailureListener through {@link SniffOnFailureListener#setSniffer(Sniffer)}.
 */
public class Sniffer implements Closeable {

    private static final Log logger = LogFactory.getLog(Sniffer.class);
    private static final String SNIFFER_THREAD_NAME = "es_rest_client_sniffer";

    private final NodesSniffer nodesSniffer;
    private final RestClient restClient;
    private final long sniffIntervalMillis;
    private final long sniffAfterFailureDelayMillis;
    private final Scheduler scheduler;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private volatile ScheduledTask nextScheduledTask;

    Sniffer(RestClient restClient, NodesSniffer nodesSniffer, long sniffInterval, long sniffAfterFailureDelay) {
        this(restClient, nodesSniffer, new DefaultScheduler(), sniffInterval, sniffAfterFailureDelay);
    }

    Sniffer(RestClient restClient, NodesSniffer nodesSniffer, Scheduler scheduler, long sniffInterval, long sniffAfterFailureDelay) {
        this.nodesSniffer = nodesSniffer;
        this.restClient = restClient;
        this.sniffIntervalMillis = sniffInterval;
        this.sniffAfterFailureDelayMillis = sniffAfterFailureDelay;
        this.scheduler = scheduler;
        /*
         * The first sniffing round is async, so this constructor returns before nextScheduledTask is assigned to a task.
         * The initialized flag is a protection against NPE due to that.
         */
        Task task = new Task(sniffIntervalMillis) {
            @Override
            public void run() {
                super.run();
                initialized.compareAndSet(false, true);
            }
        };
        /*
         * We do not keep track of the returned future as we never intend to cancel the initial sniffing round, we rather
         * prevent any other operation from being executed till the sniffer is properly initialized
         */
        scheduler.schedule(task, 0L);
    }

    /**
     * Schedule sniffing to run as soon as possible if it isn't already running. Once such sniffing round runs
     * it will also schedule a new round after sniffAfterFailureDelay ms.
     */
    public void sniffOnFailure() {
        // sniffOnFailure does nothing until the initial sniffing round has been completed
        if (initialized.get()) {
            /*
             * If sniffing is already running, there is no point in scheduling another round right after the current one.
             * Concurrent calls may be checking the same task state, but only the first skip call on the same task returns true.
             * The task may also get replaced while we check its state, in which case calling skip on it returns false.
             */
            if (this.nextScheduledTask.skip()) {
                /*
                 * We do not keep track of this future as the task will immediately run and we don't intend to cancel it
                 * due to concurrent sniffOnFailure runs. Effectively the previous (now cancelled or skipped) task will stay
                 * assigned to nextTask till this onFailure round gets run and schedules its corresponding afterFailure round.
                 */
                scheduler.schedule(new Task(sniffAfterFailureDelayMillis), 0L);
            }
        }
    }

    enum TaskState {
        WAITING,
        SKIPPED,
        STARTED
    }

    class Task implements Runnable {
        final long nextTaskDelay;
        final AtomicReference<TaskState> taskState = new AtomicReference<>(TaskState.WAITING);

        Task(long nextTaskDelay) {
            this.nextTaskDelay = nextTaskDelay;
        }

        @Override
        public void run() {
            /*
             * Skipped or already started tasks do nothing. In most cases tasks will be cancelled and not run, but we want to protect for
             * cases where future#cancel returns true yet the task runs. We want to make sure that such tasks do nothing otherwise they will
             * schedule another round at the end and so on, leaving us with multiple parallel sniffing "tracks" whish is undesirable.
             */
            if (taskState.compareAndSet(TaskState.WAITING, TaskState.STARTED) == false) {
                return;
            }
            try {
                sniff();
            } catch (Exception e) {
                logger.error("error while sniffing nodes", e);
            } finally {
                Task task = new Task(sniffIntervalMillis);
                Future<?> future = scheduler.schedule(task, nextTaskDelay);
                // tasks are run by a single threaded executor, so swapping is safe with a simple volatile variable
                ScheduledTask previousTask = nextScheduledTask;
                nextScheduledTask = new ScheduledTask(task, future);
                assert initialized.get() == false || previousTask.task.isSkipped() || previousTask.task.hasStarted()
                    : "task that we are replacing is neither " + "cancelled nor has it ever started";
            }
        }

        /**
         * Returns true if the task has started, false in case it didn't start (yet?) or it was skipped
         */
        boolean hasStarted() {
            return taskState.get() == TaskState.STARTED;
        }

        /**
         * Sets this task to be skipped. Returns true if the task will be skipped, false if the task has already started.
         */
        boolean skip() {
            /*
             * Threads may still get run although future#cancel returns true. We make sure that a task is either cancelled (or skipped),
             * or entirely run. In the odd case that future#cancel returns true and the thread still runs, the task won't do anything.
             * In case future#cancel returns true but the task has already started, this state change will not succeed hence this method
             * returns false and the task will normally run.
             */
            return taskState.compareAndSet(TaskState.WAITING, TaskState.SKIPPED);
        }

        /**
         * Returns true if the task was set to be skipped before it was started
         */
        boolean isSkipped() {
            return taskState.get() == TaskState.SKIPPED;
        }
    }

    static final class ScheduledTask {
        final Task task;
        final Future<?> future;

        ScheduledTask(Task task, Future<?> future) {
            this.task = task;
            this.future = future;
        }

        /**
         * Cancels this task. Returns true if the task has been successfully cancelled, meaning it won't be executed
         * or if it is its execution won't have any effect. Returns false if the task cannot be cancelled (possibly it was
         * already cancelled or already completed).
         */
        boolean skip() {
            /*
             * Future#cancel should return false whenever a task cannot be cancelled, most likely as it has already started. We don't
             * trust it much though so we try to cancel hoping that it will work.  At the same time we always call skip too, which means
             * that if the task has already started the state change will fail. We could potentially not call skip when cancel returns
             * false but we prefer to stay on the safe side.
             */
            future.cancel(false);
            return task.skip();
        }
    }

    final void sniff() throws IOException {
        List<Node> sniffedNodes = nodesSniffer.sniff();
        if (logger.isDebugEnabled()) {
            logger.debug("sniffed nodes: " + sniffedNodes);
        }
        if (sniffedNodes.isEmpty()) {
            logger.warn("no nodes to set, nodes will be updated at the next sniffing round");
        } else {
            restClient.setNodes(sniffedNodes);
        }
    }

    @Override
    public void close() {
        if (initialized.get()) {
            nextScheduledTask.skip();
        }
        this.scheduler.shutdown();
    }

    /**
     * Returns a new {@link SnifferBuilder} to help with {@link Sniffer} creation.
     *
     * @param restClient the client that gets its hosts set (via
     *      {@link RestClient#setNodes(Collection)}) once they are fetched
     * @return a new instance of {@link SnifferBuilder}
     */
    public static SnifferBuilder builder(RestClient restClient) {
        return new SnifferBuilder(restClient);
    }

    /**
     * The Scheduler interface allows to isolate the sniffing scheduling aspects so that we can test
     * the sniffer by injecting when needed a custom scheduler that is more suited for testing.
     */
    interface Scheduler {
        /**
         * Schedules the provided {@link Runnable} to be executed in <code>delayMillis</code> milliseconds
         */
        Future<?> schedule(Task task, long delayMillis);

        /**
         * Shuts this scheduler down
         */
        void shutdown();
    }

    /**
     * Default implementation of {@link Scheduler}, based on {@link ScheduledExecutorService}
     */
    static final class DefaultScheduler implements Scheduler {
        final ScheduledExecutorService executor;

        DefaultScheduler() {
            this(initScheduledExecutorService());
        }

        DefaultScheduler(ScheduledExecutorService executor) {
            this.executor = executor;
        }

        private static ScheduledExecutorService initScheduledExecutorService() {
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, new SnifferThreadFactory(SNIFFER_THREAD_NAME));
            executor.setRemoveOnCancelPolicy(true);
            return executor;
        }

        @Override
        public Future<?> schedule(Task task, long delayMillis) {
            return executor.schedule(task, delayMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public void shutdown() {
            executor.shutdown();
            try {
                if (executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                    return;
                }
                executor.shutdownNow();
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static class SnifferThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;
        private final ThreadFactory originalThreadFactory;

        private SnifferThreadFactory(String namePrefix) {
            this.namePrefix = namePrefix;
            this.originalThreadFactory = AccessController.doPrivileged(new PrivilegedAction<ThreadFactory>() {
                @Override
                public ThreadFactory run() {
                    return Executors.defaultThreadFactory();
                }
            });
        }

        @Override
        public Thread newThread(final Runnable r) {
            return AccessController.doPrivileged(new PrivilegedAction<Thread>() {
                @Override
                public Thread run() {
                    Thread t = originalThreadFactory.newThread(r);
                    t.setName(namePrefix + "[T#" + threadNumber.getAndIncrement() + "]");
                    t.setDaemon(true);
                    return t;
                }
            });
        }
    }
}
