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

package org.elasticsearch.client.sniff;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Class responsible for sniffing nodes from some source (default is elasticsearch itself) and setting them to a provided instance of
 * {@link RestClient}. Must be created via {@link SnifferBuilder}, which allows to set all of the different options or rely on defaults.
 * A background task fetches the nodes through the {@link HostsSniffer} and sets them to the {@link RestClient} instance.
 * It is possible to perform sniffing on failure by creating a {@link SniffOnFailureListener} and providing it as an argument to
 * {@link RestClientBuilder#setFailureListener(RestClient.FailureListener)}. The Sniffer implementation needs to be lazily set to the
 * previously created SniffOnFailureListener through {@link SniffOnFailureListener#setSniffer(Sniffer)}.
 */
public class Sniffer implements Closeable {

    private static final Log logger = LogFactory.getLog(Sniffer.class);
    private static final String SNIFFER_THREAD_NAME = "es_rest_client_sniffer";

    private final HostsSniffer hostsSniffer;
    private final RestClient restClient;

    private final long sniffIntervalMillis;
    private final long sniffAfterFailureDelayMillis;
    private final Scheduler scheduler;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<ScheduledTask> nextTask = new AtomicReference<>();

    Sniffer(RestClient restClient, HostsSniffer hostsSniffer, long sniffInterval, long sniffAfterFailureDelay) {
        this(restClient, hostsSniffer, new DefaultScheduler(), sniffInterval, sniffAfterFailureDelay);
    }

    Sniffer(RestClient restClient, HostsSniffer hostsSniffer, Scheduler scheduler,  long sniffInterval, long sniffAfterFailureDelay) {
        this.hostsSniffer = hostsSniffer;
        this.restClient = restClient;
        this.sniffIntervalMillis = sniffInterval;
        this.sniffAfterFailureDelayMillis = sniffAfterFailureDelay;
        this.scheduler = scheduler;
        //first sniffing round is immediately executed, next one will be executed depending on the configured sniff interval
        scheduleNextRound(0L, sniffIntervalMillis, false);
    }

    /**
     * Triggers a new immediate sniffing round, which will schedule a new round in sniffAfterFailureDelayMillis ms
     */
    public final void sniffOnFailure() {
        scheduleNextRound(0L, sniffAfterFailureDelayMillis, true);
    }

    //TODO test concurrency on this method
    private void scheduleNextRound(long delay, long nextDelay, boolean mustCancelNextRound) {
        Task task = new Task(nextDelay);
        ScheduledTask scheduledTask = task.schedule(delay);
        assert scheduledTask.task == task;
        ScheduledTask previousTask = nextTask.getAndSet(scheduledTask);
        if (mustCancelNextRound) {
            previousTask.cancelIfNotYetStarted();
        }
    }

    final class Task implements Runnable {
        final long nextTaskDelay;

        Task(long nextTaskDelay) {
            this.nextTaskDelay = nextTaskDelay;
        }

        ScheduledTask schedule(long delay) {
            return scheduler.schedule(this, delay);
        }

        @Override
        public void run() {
            try {
                sniff();
            } catch (Exception e) {
                logger.error("error while sniffing nodes", e);
            } finally {
                scheduleNextRound(nextTaskDelay, sniffIntervalMillis, false);
            }
        }
    }

    static final class ScheduledTask {
        final Task task;
        final Future<?> future;

        ScheduledTask(Task task, Future<?> future) {
            this.task = task;
            this.future = future;
        }

        void cancelIfNotYetStarted() {
            this.future.cancel(false);
        }
    }

    final void sniff() throws IOException {
        List<HttpHost> sniffedHosts = hostsSniffer.sniffHosts();
        logger.debug("sniffed hosts: " + sniffedHosts);
        if (sniffedHosts.isEmpty()) {
            logger.warn("no hosts to set, hosts will be updated at the next sniffing round");
        } else {
            restClient.setHosts(sniffedHosts.toArray(new HttpHost[sniffedHosts.size()]));
        }
    }

    @Override
    public void close() {
        nextTask.get().cancelIfNotYetStarted();
        this.scheduler.shutdown();
    }

    /**
     * Returns a new {@link SnifferBuilder} to help with {@link Sniffer} creation.
     *
     * @param restClient the client that gets its hosts set (via {@link RestClient#setHosts(HttpHost...)}) once they are fetched
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
        ScheduledTask schedule(Task task, long delayMillis);

        /**
         * Shuts this scheduler down
         */
        void shutdown();
    }

    /**
     * Default implementation of {@link Scheduler}, based on {@link ScheduledExecutorService}
     */
    static final class DefaultScheduler implements Scheduler {
        final ScheduledThreadPoolExecutor executor;

        DefaultScheduler() {
            this(initScheduledExecutorService());
        }

        DefaultScheduler(ScheduledThreadPoolExecutor executor) {
            this.executor = executor;
        }

        //TODO test this
        static ScheduledThreadPoolExecutor initScheduledExecutorService() {
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, new SnifferThreadFactory(SNIFFER_THREAD_NAME));
            executor.setRemoveOnCancelPolicy(true);
            //TODO does this have any effect?
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            return executor;
        }

        //TODO can it happen that we get so many failures that we keep on cancelling without ever getting to execute the next round?
        // no, because we add each failed node to the blacklist, and it stays there till setHosts is called, at the end of the sniff round
        //may happen if we end up reviving nodes from the blacklist as all of the sniffed nodes are marked dead.
        //in that case the sniff round won't work either though.

        @Override
        public ScheduledTask schedule(Task task, long delayMillis) {
            ScheduledFuture<?> future = executor.schedule(task, delayMillis, TimeUnit.MILLISECONDS);
            return new ScheduledTask(task, future);
        }

        @Override
        public void shutdown() {
            executor.shutdown();
            try {
                if (executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                    return;
                }
                executor.shutdownNow();
            } catch (InterruptedException e) {
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
