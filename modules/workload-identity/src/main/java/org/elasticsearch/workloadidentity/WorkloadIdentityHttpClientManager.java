/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Owns the Apache HC {@link CloseableHttpAsyncClient} used to talk to the
 * workload-identity-issuer, together with the {@link PoolingAsyncClientConnectionManager} backing it.
 *
 * <p>The HC client, connection manager and IO reactor are built once at construction
 * and live for the lifetime of the manager. Cert/key rotation reaches the connection
 * establishment path via a {@link ReloadableTlsStrategy} indirection installed in the connection
 * manager: {@link #reload()} swaps that delegate to the freshly-built
 * {@link org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy} from {@link WorkloadIdentitySslConfig}, then drains idle
 * connections immediately via {@code closeIdle(ZERO)}. Connections that were in-flight at rotation
 * time are retired by {@link RotationAwareReuseStrategy} after their next response: the strategy
 * compares the rotation epoch stamped on the TLS session against the current epoch, and returns
 * {@code false} from {@code keepAlive} when they differ.
 *
 * <p>Lifecycle: {@code INIT} (constructed) &rarr; {@code INIT_RELOADED} (first {@link #reload()})
 * &rarr; {@code STARTED} ({@link #start()}) &rarr;
 * {@code CLOSED} ({@link #close()}).
 */
public final class WorkloadIdentityHttpClientManager implements Closeable {

    private static final Logger logger = LogManager.getLogger(WorkloadIdentityHttpClientManager.class);

    private final WorkloadIdentitySslConfig sslConfig;
    private final ReloadableTlsStrategy tlsStrategy;
    private final PoolingAsyncClientConnectionManager connectionManager;
    private final CloseableHttpAsyncClient httpClient;

    /** Lifecycle states; see the class Javadoc for transitions. */
    private enum State {
        /** Constructed; no TLS strategy delegate published yet. */
        INIT,
        /** Initial TLS delegate published via {@link #reload()}; {@link #start()} not yet called. */
        INIT_RELOADED,
        /** Apache HC async client started; {@link #getHttpClient()} returns the client. */
        STARTED,
        /** {@link #close()} has been called; all resources released. Terminal. */
        CLOSED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);

    public WorkloadIdentityHttpClientManager(Settings settings, WorkloadIdentitySslConfig sslConfig) {
        this.sslConfig = sslConfig;
        final int maxTotalConnections = WorkloadIdentityHttpSettings.MAX_TOTAL_CONNECTIONS.get(settings);
        final int maxRouteConnections = WorkloadIdentityHttpSettings.MAX_ROUTE_CONNECTIONS.get(settings);
        final long connectTimeoutMillis = WorkloadIdentityHttpSettings.CONNECT_TIMEOUT.get(settings).millis();
        final long connectionMaxIdleMillis = WorkloadIdentityHttpSettings.CONNECTION_MAX_IDLE_TIME.get(settings).millis();

        // Constructed empty; the first reload() call publishes the initial delegate.
        this.tlsStrategy = new ReloadableTlsStrategy();
        this.connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
            .setTlsStrategy(tlsStrategy)
            .setMaxConnTotal(maxTotalConnections)
            .setMaxConnPerRoute(maxRouteConnections)
            .setDefaultConnectionConfig(ConnectionConfig.custom().setConnectTimeout(Timeout.ofMilliseconds(connectTimeoutMillis)).build())
            .build();

        // Override the IOReactorConfig default of availableProcessors(): this client is low-QPS
        // and single-host, and concurrent callers share in-flight fetches via the token cache,
        // so one dispatcher suffices and keeps the thread footprint independent of host CPU count.
        this.httpClient = HttpAsyncClients.custom()
            .setConnectionManager(connectionManager)
            .setIOReactorConfig(IOReactorConfig.custom().setSoKeepAlive(true).setIoThreadCount(1).build())
            .setConnectionReuseStrategy(new RotationAwareReuseStrategy(tlsStrategy))
            .disableCookieManagement()
            .disableConnectionState()
            .evictExpiredConnections()
            .evictIdleConnections(TimeValue.ofMilliseconds(connectionMaxIdleMillis))
            .build();
    }

    /**
     * Start the underlying async client. Valid only from {@code INIT_RELOADED} (i.e.
     * {@link #reload()} has published the initial TLS delegate); any other source state throws.
     */
    public void start() {
        if (state.compareAndSet(State.INIT_RELOADED, State.STARTED) == false) {
            throw new IllegalStateException("cannot start workload-identity HTTP client manager in state [" + state.get() + "]");
        }
        httpClient.start();
    }

    /**
     * @return the Apache HC async client. The same instance is returned for the lifetime of the
     *         manager: cert/key rotation is applied in place via the registered TLS strategy
     *         (see {@link #reload()}) rather than by republishing a new client.
     */
    public CloseableHttpAsyncClient getHttpClient() {
        final State current = state.get();
        if (current != State.STARTED) {
            throw new IllegalStateException("workload-identity HTTP client manager in state [" + current + "]");
        }
        return httpClient;
    }

    /**
     * Swap the registered {@link ReloadableTlsStrategy}'s underlying
     * {@link org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy} to one built over the
     * freshly-loaded {@code SSLContext}, drain idle connections, and advance the rotation epoch.
     * In-flight connections are retired by {@link RotationAwareReuseStrategy} after their next response.
     */
    public void reload() {
        if (state.get() == State.CLOSED) {
            return;
        }
        final DefaultClientTlsStrategy next;
        try {
            next = sslConfig.getStrategy();
        } catch (Exception e) {
            logger.warn("failed to fetch new workload-identity TLS strategy during reload; keeping previous delegate", e);
            return;
        }
        tlsStrategy.setDelegate(next);
        connectionManager.closeIdle(TimeValue.ZERO_MILLISECONDS);
        state.compareAndSet(State.INIT, State.INIT_RELOADED);
        logger.debug("published workload-identity TLS strategy; idle connections drained");
    }

    // Visible for testing
    ReloadableTlsStrategy getTlsStrategy() {
        return tlsStrategy;
    }

    /**
     * Shut down the started HC client. Only the {@code STARTED → CLOSED} transition does work;
     * calls from any other state (including a repeat call from {@code CLOSED}) silently return.
     */
    @Override
    public void close() {
        if (state.compareAndSet(State.STARTED, State.CLOSED) == false) {
            return;
        }
        try {
            httpClient.close();
        } catch (IOException e) {
            logger.warn("failed to close workload-identity HTTP client", e);
        }
    }
}
