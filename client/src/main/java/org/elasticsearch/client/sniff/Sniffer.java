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
import org.apache.http.impl.client.CloseableHttpClient;
import org.elasticsearch.client.Connection;
import org.elasticsearch.client.RestClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Calls nodes info api and returns a list of http hosts extracted from it.
 */
//TODO This could potentially be using _cat/nodes which wouldn't require jackson as a dependency, but we'd have bw comp problems with 2.x
public final class Sniffer extends RestClient.FailureListener implements Closeable {

    private static final Log logger = LogFactory.getLog(Sniffer.class);

    private final boolean sniffOnFailure;
    private final Task task;

    public Sniffer(RestClient restClient, int sniffRequestTimeout, String scheme, int sniffInterval,
                   boolean sniffOnFailure, int sniffAfterFailureDelay) {
        HostsSniffer hostsSniffer = new HostsSniffer(restClient, sniffRequestTimeout, scheme);
        this.task = new Task(hostsSniffer, restClient, sniffInterval, sniffAfterFailureDelay);
        this.sniffOnFailure = sniffOnFailure;
        restClient.setFailureListener(this);
    }

    @Override
    public void onFailure(Connection connection) throws IOException {
        if (sniffOnFailure) {
            //re-sniff immediately but take out the node that failed
            task.sniffOnFailure(connection.getHost());
        }
    }

    @Override
    public void close() throws IOException {
        task.shutdown();
    }

    private static class Task implements Runnable {
        private final HostsSniffer hostsSniffer;
        private final RestClient restClient;

        private final int sniffInterval;
        private final int sniffAfterFailureDelay;
        private final ScheduledExecutorService scheduledExecutorService;
        private final AtomicBoolean running = new AtomicBoolean(false);
        private volatile int nextSniffDelay;
        private volatile ScheduledFuture<?> scheduledFuture;

        private Task(HostsSniffer hostsSniffer, RestClient restClient, int sniffInterval, int sniffAfterFailureDelay) {
            this.hostsSniffer = hostsSniffer;
            this.restClient = restClient;
            this.sniffInterval = sniffInterval;
            this.sniffAfterFailureDelay = sniffAfterFailureDelay;
            this.scheduledExecutorService = Executors.newScheduledThreadPool(1);
            this.scheduledFuture = this.scheduledExecutorService.schedule(this, 0, TimeUnit.MILLISECONDS);
            this.nextSniffDelay = sniffInterval;
        }

        @Override
        public void run() {
            sniff(null);
        }

        void sniffOnFailure(HttpHost failedHost) {
            this.nextSniffDelay = sniffAfterFailureDelay;
            sniff(failedHost);
        }

        void sniff(HttpHost excludeHost) {
            if (running.compareAndSet(false, true)) {
                try {
                    List<HttpHost> sniffedNodes = hostsSniffer.sniffHosts();
                    if (excludeHost != null) {
                        sniffedNodes.remove(excludeHost);
                    }
                    logger.debug("sniffed nodes: " + sniffedNodes);
                    this.restClient.setNodes(sniffedNodes.toArray(new HttpHost[sniffedNodes.size()]));
                } catch (Throwable t) {
                    logger.error("error while sniffing nodes", t);
                } finally {
                    try {
                        //regardless of whether and when the next sniff is scheduled, cancel it and schedule a new one with updated delay
                        this.scheduledFuture.cancel(false);
                        logger.debug("scheduling next sniff in " + nextSniffDelay + " ms");
                        this.scheduledFuture = this.scheduledExecutorService.schedule(this, nextSniffDelay, TimeUnit.MILLISECONDS);
                    } catch (Throwable t) {
                        logger.error("error while scheduling next sniffer task", t);
                    } finally {
                        this.nextSniffDelay = sniffInterval;
                        running.set(false);
                    }
                }
            }
        }

        void shutdown() {
            scheduledExecutorService.shutdown();
            try {
                if (scheduledExecutorService.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            scheduledExecutorService.shutdownNow();
        }
    }

    /**
     * Returns a new {@link Builder} to help with {@link Sniffer} creation.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Sniffer builder. Helps creating a new {@link Sniffer}.
     */
    public static final class Builder {
        public static final int DEFAULT_SNIFF_INTERVAL = 60000 * 5; //5 minutes
        public static final int DEFAULT_SNIFF_AFTER_FAILURE_DELAY = 60000; //1 minute
        public static final int DEFAULT_SNIFF_REQUEST_TIMEOUT = 1000; //1 second

        private int sniffRequestTimeout = DEFAULT_SNIFF_REQUEST_TIMEOUT;
        private int sniffInterval = DEFAULT_SNIFF_INTERVAL;
        private boolean sniffOnFailure = true;
        private int sniffAfterFailureDelay = DEFAULT_SNIFF_AFTER_FAILURE_DELAY;
        private String scheme = "http";
        private RestClient restClient;

        private Builder() {

        }

        /**
         * Sets the interval between consecutive ordinary sniff executions. Will be honoured when sniffOnFailure is disabled or
         * when there are no failures between consecutive sniff executions.
         * @throws IllegalArgumentException if sniffInterval is not greater than 0
         */
        public Builder setSniffInterval(int sniffInterval) {
            if (sniffInterval <= 0) {
                throw new IllegalArgumentException("sniffInterval must be greater than 0");
            }
            this.sniffInterval = sniffInterval;
            return this;
        }

        /**
         * Enables/disables sniffing on failure. If enabled, at each failure nodes will be reloaded, and a new sniff execution will
         * be scheduled after a shorter time than usual (sniffAfterFailureDelay).
         */
        public Builder setSniffOnFailure(boolean sniffOnFailure) {
            this.sniffOnFailure = sniffOnFailure;
            return this;
        }

        /**
         * Sets the delay of a sniff execution scheduled after a failure.
         */
        public Builder setSniffAfterFailureDelay(int sniffAfterFailureDelay) {
            if (sniffAfterFailureDelay <= 0) {
                throw new IllegalArgumentException("sniffAfterFailureDelay must be greater than 0");
            }
            this.sniffAfterFailureDelay = sniffAfterFailureDelay;
            return this;
        }

        /**
         * Sets the http client. Mandatory argument. Best practice is to use the same client used
         * within {@link org.elasticsearch.client.RestClient} which can be created manually or
         * through {@link RestClient.Builder#createDefaultHttpClient(Collection)}.
         * @see CloseableHttpClient
         */
        public Builder setRestClient(RestClient restClient) {
            this.restClient = restClient;
            return this;
        }

        /**
         * Sets the sniff request timeout to be passed in as a query string parameter to elasticsearch.
         * Allows to halt the request without any failure, as only the nodes that have responded
         * within this timeout will be returned.
         */
        public Builder setSniffRequestTimeout(int sniffRequestTimeout) {
            if (sniffRequestTimeout <=0) {
                throw new IllegalArgumentException("sniffRequestTimeout must be greater than 0");
            }
            this.sniffRequestTimeout = sniffRequestTimeout;
            return this;
        }

        /**
         * Sets the scheme to be used for sniffed nodes. This information is not returned by elasticsearch,
         * default is http but should be customized if https is needed/enabled.
         */
        public Builder setScheme(String scheme) {
            Objects.requireNonNull(scheme, "scheme cannot be null");
            if (scheme.equals("http") == false && scheme.equals("https") == false) {
                throw new IllegalArgumentException("scheme must be either http or https");
            }
            this.scheme = scheme;
            return this;
        }

        /**
         * Creates the {@link Sniffer} based on the provided configuration.
         */
        public Sniffer build() {
            Objects.requireNonNull(restClient, "restClient cannot be null");
            return new Sniffer(restClient, sniffRequestTimeout, scheme, sniffInterval, sniffOnFailure, sniffAfterFailureDelay);
        }
    }
}
