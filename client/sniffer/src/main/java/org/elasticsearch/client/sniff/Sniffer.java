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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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

    Sniffer(RestClient restClient, HostsSniffer hostsSniffer, long sniffInterval, long sniffAfterFailureDelay) {
        this(restClient, hostsSniffer, new DefaultScheduler(), sniffInterval, sniffAfterFailureDelay);
    }

    Sniffer(RestClient restClient, HostsSniffer hostsSniffer, Scheduler scheduler,  long sniffInterval, long sniffAfterFailureDelay) {
        this.hostsSniffer = hostsSniffer;
        this.restClient = restClient;
        this.sniffIntervalMillis = sniffInterval;
        this.sniffAfterFailureDelayMillis = sniffAfterFailureDelay;
        this.scheduler = scheduler;
        scheduleNextRun(0);
    }

    private void scheduleNextRun(long delayMillis) {
        scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                sniff(null, sniffIntervalMillis);
            }
        }, delayMillis);
    }

    /**
     * Triggers a new sniffing round and explicitly takes out the failed host provided as argument
     */
    public void sniffOnFailure(HttpHost failedHost) {
        sniff(failedHost, sniffAfterFailureDelayMillis);
    }

    private void sniff(HttpHost excludeHost, long nextSniffDelayMillis) {
        //If a sniffing round is already running nothing happens, it makes no sense to start or wait to start another round
        if (running.compareAndSet(false, true)) {
            try {
                List<HttpHost> sniffedHosts = hostsSniffer.sniffHosts();
                logger.debug("sniffed hosts: " + sniffedHosts);
                if (excludeHost != null) {
                    sniffedHosts.remove(excludeHost);
                }
                if (sniffedHosts.isEmpty()) {
                    logger.warn("no hosts to set, hosts will be updated at the next sniffing round");
                } else {
                    this.restClient.setHosts(sniffedHosts.toArray(new HttpHost[sniffedHosts.size()]));
                }
            } catch (Exception e) {
                logger.error("error while sniffing nodes", e);
            } finally {
                //TODO potential problem here if this doesn't happen last? though tests become complicated
                running.set(false);
                //TODO do we want to also test that this never gets called concurrently?
                scheduleNextRun(nextSniffDelayMillis);
            }
        }
    }

    @Override
    public void close() {
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
     * the sniffer by injecting a custom scheduler that is more suited for testing.
     */
    interface Scheduler {
        /**
         * Schedules the provided {@link Runnable} to run in <code>delayMillis</code> milliseconds
         */
        void schedule(Runnable runnable, long delayMillis);

        /**
         * Shuts this scheduler down
         */
        void shutdown();
    }

    /**
     * Default implementation of {@link Scheduler}, based on {@link ScheduledExecutorService}
     */
    static final class DefaultScheduler implements Scheduler {
        final ScheduledExecutorService scheduledExecutorService;
        private ScheduledFuture<?> scheduledFuture;

        DefaultScheduler() {
            this(Executors.newScheduledThreadPool(1, new SnifferThreadFactory(SNIFFER_THREAD_NAME)));
        }

        DefaultScheduler(ScheduledExecutorService scheduledExecutorService) {
            this.scheduledExecutorService = scheduledExecutorService;
        }

        //TODO test concurrent calls to schedule?
        @Override
        public synchronized void schedule(Runnable runnable, long delayMillis) {
            //TODO maybe this is not even needed, just let it throw rejected execution exception instead?
            if (scheduledExecutorService.isShutdown() == false) {
                try {
                    if (this.scheduledFuture != null) {
                        //regardless of when the next sniff is scheduled, cancel it and schedule a new one with the latest delay.
                        //instead of piling up sniff rounds to be run, the last run decides when the following execution will be.
                        this.scheduledFuture.cancel(false);
                    }
                    logger.debug("scheduling next sniff in " + delayMillis + " ms");
                    this.scheduledFuture = scheduledExecutorService.schedule(runnable, delayMillis, TimeUnit.MILLISECONDS);
                } catch(Exception e) {
                    logger.error("error while scheduling next sniffer task", e);
                }
            }
        }

        //TODO test concurrent calls to shutdown?
        @Override
        public synchronized void shutdown() {
            scheduledExecutorService.shutdown();
            try {
                if (scheduledExecutorService.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                    return;
                }
                scheduledExecutorService.shutdownNow();
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
