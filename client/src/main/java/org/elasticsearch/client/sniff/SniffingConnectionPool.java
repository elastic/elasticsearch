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
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.elasticsearch.client.Connection;
import org.elasticsearch.client.ConnectionPool;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Connection pool implementation that sniffs nodes from elasticsearch at regular intervals.
 * Can optionally sniff nodes on each failure as well.
 */
public class SniffingConnectionPool extends ConnectionPool {

    private static final Log logger = LogFactory.getLog(SniffingConnectionPool.class);

    private final boolean sniffOnFailure;
    private final Sniffer sniffer;
    private volatile List<Connection> connections;
    private final SnifferTask snifferTask;

    private SniffingConnectionPool(int sniffInterval, boolean sniffOnFailure, int sniffAfterFailureDelay, CloseableHttpClient client,
                                   RequestConfig sniffRequestConfig, int sniffRequestTimeout, String scheme, HttpHost... hosts) {
        this.sniffOnFailure = sniffOnFailure;
        this.sniffer = new Sniffer(client, sniffRequestConfig, sniffRequestTimeout, scheme);
        this.connections = createConnections(hosts);
        this.snifferTask = new SnifferTask(sniffInterval, sniffAfterFailureDelay);
    }

    @Override
    protected List<Connection> getConnections() {
        return this.connections;
    }

    @Override
    public void onFailure(Connection connection) throws IOException {
        super.onFailure(connection);
        if (sniffOnFailure) {
            //re-sniff immediately but take out the node that failed
            snifferTask.sniffOnFailure(connection.getHost());
        }
    }

    @Override
    public void close() throws IOException {
        snifferTask.shutdown();
    }

    private class SnifferTask implements Runnable {
        private final int sniffInterval;
        private final int sniffAfterFailureDelay;
        private final ScheduledExecutorService scheduledExecutorService;
        private final AtomicBoolean running = new AtomicBoolean(false);
        private volatile boolean failure = false;
        private volatile ScheduledFuture<?> scheduledFuture;

        private SnifferTask(int sniffInterval, int sniffAfterFailureDelay) {
            this.sniffInterval = sniffInterval;
            this.sniffAfterFailureDelay = sniffAfterFailureDelay;
            this.scheduledExecutorService = Executors.newScheduledThreadPool(1);
            this.scheduledFuture = this.scheduledExecutorService.schedule(this, 0, TimeUnit.MILLISECONDS);
        }

        @Override
        public void run() {
            sniff(null);
        }

        void sniffOnFailure(HttpHost failedHost) {
            //sync sniff straightaway on failure
            failure = true;
            sniff(failedHost);
        }

        void sniff(HttpHost excludeHost) {
            if (running.compareAndSet(false, true)) {
                try {
                    sniff(nextConnection(), excludeHost);
                } catch (Throwable t) {
                    logger.error("error while sniffing nodes", t);
                } finally {
                    try {
                        //regardless of whether and when the next sniff is scheduled, cancel it and schedule a new one with updated delay
                        this.scheduledFuture.cancel(false);
                        if (this.failure) {
                            this.scheduledFuture = this.scheduledExecutorService.schedule(this,
                                    sniffAfterFailureDelay, TimeUnit.MILLISECONDS);
                            this.failure = false;
                        } else {
                            this.scheduledFuture = this.scheduledExecutorService.schedule(this, sniffInterval, TimeUnit.MILLISECONDS);
                        }
                    } catch (Throwable t) {
                        logger.error("error while scheduling next sniffer task", t);
                    } finally {
                        running.set(false);
                    }
                }
            }
        }

        void sniff(Iterator<Connection> connectionIterator, HttpHost excludeHost) throws IOException {
            IOException lastSeenException = null;
            while (connectionIterator.hasNext()) {
                Connection connection = connectionIterator.next();
                try {
                    List<HttpHost> sniffedNodes = sniffer.sniffNodes(connection.getHost());
                    if (excludeHost != null) {
                        sniffedNodes.remove(excludeHost);
                    }
                    connections = createConnections(sniffedNodes.toArray(new HttpHost[sniffedNodes.size()]));
                    onSuccess(connection);
                    return;
                } catch (IOException e) {
                    //here we have control over the request, if it fails something is really wrong, always call onFailure
                    onFailure(connection);
                    if (lastSeenException != null) {
                        e.addSuppressed(lastSeenException);
                    }
                    lastSeenException = e;
                }
            }
            logger.warn("failed to sniff nodes", lastSeenException);
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
     * Returns a new {@link Builder} to help with {@link SniffingConnectionPool} creation.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Sniffing connection pool builder. Helps creating a new {@link SniffingConnectionPool}.
     */
    public static final class Builder {
        private int sniffInterval = 5 * 1000 * 60;
        private boolean sniffOnFailure = true;
        private int sniffAfterFailureDelay = 60000;
        private CloseableHttpClient httpClient;
        private RequestConfig sniffRequestConfig;
        private int sniffRequestTimeout = 1000;
        private String scheme = "http";
        private HttpHost[] hosts;

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
         * through {@link RestClient.Builder#createDefaultHttpClient()}.
         * @see CloseableHttpClient
         */
        public Builder setHttpClient(CloseableHttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        /**
         * Sets the configuration to be used for each sniff request. Useful as sniff can have
         * different timeouts compared to ordinary requests.
         * @see RequestConfig
         */
        public Builder setSniffRequestConfig(RequestConfig sniffRequestConfig) {
            this.sniffRequestConfig = sniffRequestConfig;
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
         * Sets the hosts that the client will send requests to.
         */
        public Builder setHosts(HttpHost... hosts) {
            this.hosts = hosts;
            return this;
        }

        /**
         * Creates the {@link SniffingConnectionPool} based on the provided configuration.
         */
        public SniffingConnectionPool build() {
            Objects.requireNonNull(httpClient, "httpClient cannot be null");
            if (hosts == null || hosts.length == 0) {
                throw new IllegalArgumentException("no hosts provided");
            }

            if (sniffRequestConfig == null) {
                sniffRequestConfig = RequestConfig.custom().setConnectTimeout(500).setSocketTimeout(1000)
                        .setConnectionRequestTimeout(500).build();
            }
            return new SniffingConnectionPool(sniffInterval, sniffOnFailure, sniffAfterFailureDelay, httpClient, sniffRequestConfig,
                    sniffRequestTimeout, scheme, hosts);
        }
    }
}
