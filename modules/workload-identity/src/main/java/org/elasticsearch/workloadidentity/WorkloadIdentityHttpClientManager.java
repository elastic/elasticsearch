/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns the Apache HC {@link CloseableHttpAsyncClient} used to talk to the
 * workload-identity-issuer, together with the {@link PoolingNHttpClientConnectionManager} and
 * {@link HttpConnectionEvictor} backing it.
 *
 * <p>The {@link SSLIOSessionStrategy} from {@link WorkloadIdentitySslConfig} is captured at
 * construction and is not refreshed thereafter; cert rotation requires a node restart.
 *
 * <p>Lifecycle: construct &rarr; {@link #start()} &rarr; {@link #getHttpClient()} &rarr; {@link #close()}.
 */
public final class WorkloadIdentityHttpClientManager implements Closeable {

    private static final Logger logger = LogManager.getLogger(WorkloadIdentityHttpClientManager.class);

    private final CloseableHttpAsyncClient httpClient;
    private final HttpConnectionEvictor connectionEvictor;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public WorkloadIdentityHttpClientManager(Settings settings, WorkloadIdentitySslConfig sslConfig, ThreadPool threadPool) {
        final int maxTotalConnections = WorkloadIdentityHttpSettings.MAX_TOTAL_CONNECTIONS.get(settings);
        final int maxRouteConnections = WorkloadIdentityHttpSettings.MAX_ROUTE_CONNECTIONS.get(settings);
        final TimeValue evictionInterval = WorkloadIdentityHttpSettings.CONNECTION_EVICTION_INTERVAL.get(settings);
        final TimeValue connectionMaxIdle = WorkloadIdentityHttpSettings.CONNECTION_MAX_IDLE_TIME.get(settings);

        // SSL strategy is captured here exactly once; see WorkloadIdentitySslConfig.
        PoolingNHttpClientConnectionManager connectionManager = createConnectionManager(sslConfig.getStrategy());
        connectionManager.setMaxTotal(maxTotalConnections);
        connectionManager.setDefaultMaxPerRoute(maxRouteConnections);

        // Disable cookies and connection state to maximize pooling across requests that share the
        // same mTLS identity. Per-request connect/socket timeouts are applied by the issuer client.
        this.httpClient = HttpAsyncClientBuilder.create()
            .setConnectionManager(connectionManager)
            .disableCookieManagement()
            .disableConnectionState()
            .build();

        this.connectionEvictor = new HttpConnectionEvictor(threadPool, connectionManager, evictionInterval, connectionMaxIdle);
    }

    /**
     * Start the underlying async client and connection evictor. Must be called once, after
     * construction and before the first {@link #getHttpClient()} call.
     */
    public synchronized void start() {
        if (started.compareAndSet(false, true) == false) {
            return;
        }
        httpClient.start();
        connectionEvictor.start();
    }

    /**
     * @return the long-lived Apache HC async client. The reference is stable for the lifetime
     *         of the manager; we deliberately do not rebuild it on cert rotation (see
     *         the class Javadoc).
     */
    public CloseableHttpAsyncClient getHttpClient() {
        if (closed.get()) {
            throw new IllegalStateException("workload-identity HTTP client manager is closed");
        }
        if (started.get() == false) {
            throw new IllegalStateException("workload-identity HTTP client manager has not been started");
        }
        return httpClient;
    }

    private static PoolingNHttpClientConnectionManager createConnectionManager(SSLIOSessionStrategy sslStrategy) {
        final ConnectingIOReactor ioReactor;
        try {
            ioReactor = new DefaultConnectingIOReactor(IOReactorConfig.custom().setSoKeepAlive(true).build());
        } catch (IOReactorException e) {
            throw new ElasticsearchException("failed to initialize workload-identity HTTP client manager", e);
        }

        final Registry<SchemeIOSessionStrategy> registry = RegistryBuilder.<SchemeIOSessionStrategy>create()
            .register("http", NoopIOSessionStrategy.INSTANCE)
            .register("https", sslStrategy)
            .build();
        return new PoolingNHttpClientConnectionManager(ioReactor, registry);
    }

    @Override
    public synchronized void close() {
        if (closed.compareAndSet(false, true) == false) {
            return;
        }
        try {
            connectionEvictor.close();
        } catch (Exception e) {
            logger.warn("failed to close workload-identity connection evictor", e);
        }
        try {
            httpClient.close();
        } catch (IOException e) {
            logger.warn("failed to close workload-identity HTTP client", e);
        }
    }
}
