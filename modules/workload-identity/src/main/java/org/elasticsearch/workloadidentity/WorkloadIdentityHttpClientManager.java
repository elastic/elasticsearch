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
 * <p>The HC client, connection manager, IO reactor and evictor are built once at construction
 * and live for the lifetime of the manager. Cert/key rotation reaches the connection
 * establishment path via a {@link ReloadableSchemeIoSessionStrategy} indirection installed in
 * the scheme registry: {@link #reload()} swaps that delegate to the freshly-built
 * {@link SSLIOSessionStrategy} from {@link WorkloadIdentitySslConfig}, so the very next TLS
 * handshake picks up the new material. Already-pooled connections are not aborted — in-flight
 * requests complete on the previous (still-valid) certificate.
 *
 * <p>Lifecycle: construct &rarr; {@link #start()} &rarr; {@link #getHttpClient()} &rarr;
 * (optional repeated {@link #reload()}) &rarr; {@link #close()}.
 */
public final class WorkloadIdentityHttpClientManager implements Closeable {

    private static final Logger logger = LogManager.getLogger(WorkloadIdentityHttpClientManager.class);

    private final WorkloadIdentitySslConfig sslConfig;
    private final ReloadableSchemeIoSessionStrategy sslStrategy;
    private final CloseableHttpAsyncClient httpClient;
    private final HttpConnectionEvictor connectionEvictor;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public WorkloadIdentityHttpClientManager(Settings settings, WorkloadIdentitySslConfig sslConfig, ThreadPool threadPool) {
        this.sslConfig = sslConfig;
        final int maxTotalConnections = WorkloadIdentityHttpSettings.MAX_TOTAL_CONNECTIONS.get(settings);
        final int maxRouteConnections = WorkloadIdentityHttpSettings.MAX_ROUTE_CONNECTIONS.get(settings);
        final TimeValue evictionInterval = WorkloadIdentityHttpSettings.CONNECTION_EVICTION_INTERVAL.get(settings);
        final TimeValue connectionMaxIdle = WorkloadIdentityHttpSettings.CONNECTION_MAX_IDLE_TIME.get(settings);

        // The wrapper delegates to whatever SSLIOSessionStrategy is currently produced by the
        // SSL config. Subsequent reload() calls swap the wrapped delegate; the wrapper itself
        // (the instance registered against "https" below) is captured by the connection manager
        // at construction and never replaced.
        this.sslStrategy = new ReloadableSchemeIoSessionStrategy(sslConfig.getStrategy());
        final PoolingNHttpClientConnectionManager connectionManager = createConnectionManager(sslStrategy);
        connectionManager.setMaxTotal(maxTotalConnections);
        connectionManager.setDefaultMaxPerRoute(maxRouteConnections);

        // Disable cookies and connection state to maximize pooling across requests that share the
        // same mTLS identity. Per-request connect/socket timeouts are applied by the issuer client.
        // The rotation-aware reuse strategy closes connections whose TLS handshake predates the
        // most recent reload(), so the pool drains stale material within one request RTT per
        // connection without disturbing in-flight requests (see RotationAwareReuseStrategy).
        this.httpClient = HttpAsyncClientBuilder.create()
            .setConnectionManager(connectionManager)
            .setConnectionReuseStrategy(new RotationAwareReuseStrategy(sslStrategy))
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
     * @return the Apache HC async client. The same instance is returned for the lifetime of the
     *         manager: cert/key rotation is applied in place via the registered scheme strategy
     *         (see {@link #reload()}) rather than by republishing a new client.
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

    /**
     * React to an SSL reload by swapping the registered {@link SchemeIOSessionStrategy}'s
     * underlying {@link SSLIOSessionStrategy} to one built over the
     * {@link WorkloadIdentitySslConfig}'s freshly-loaded {@code SSLContext} and advancing the
     * wrapper's rotation epoch.
     */
    public void reload() {
        if (closed.get() || !started.get()) {
            return;
        }
        final SSLIOSessionStrategy next;
        try {
            next = sslConfig.getStrategy();
        } catch (Exception e) {
            logger.error("failed to fetch new workload-identity SSL strategy during reload; keeping previous delegate", e);
            return;
        }
        sslStrategy.setDelegate(next);
        logger.info(
            "rotated workload-identity SSL material; existing pooled connections will be retired by the reuse strategy "
                + "as each completes its next response"
        );
    }

    // Visible for testing
    ReloadableSchemeIoSessionStrategy getSslStrategy() {
        return sslStrategy;
    }

    private static PoolingNHttpClientConnectionManager createConnectionManager(SchemeIOSessionStrategy sslStrategy) {
        final ConnectingIOReactor ioReactor;
        try {
            // Override the IOReactorConfig default of availableProcessors(): this client is low-QPS and
            // single-host, and concurrent callers share in-flight fetches (HttpsWorkloadIdentityIssuerClient#tokens),
            // so one dispatcher suffices and keeps the thread footprint independent of host CPU count.
            ioReactor = new DefaultConnectingIOReactor(IOReactorConfig.custom().setSoKeepAlive(true).setIoThreadCount(1).build());
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
        // Close the HC client first so its blocking shutdown of the IO reactor and connection
        // manager runs while the evictor is still scheduled; then cancel further evictor passes.
        try {
            httpClient.close();
        } catch (IOException e) {
            logger.warn("failed to close workload-identity HTTP client", e);
        }
        try {
            connectionEvictor.close();
        } catch (Exception e) {
            logger.warn("failed to close workload-identity connection evictor", e);
        }
    }
}
