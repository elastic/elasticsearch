/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.pool.PoolStats;
import org.apache.hc.core5.util.Timeout;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX;

public class HttpClientManager implements Closeable {
    private static final Logger logger = LogManager.getLogger(HttpClientManager.class);
    /**
     * The maximum number of total connections the connection pool can lease to all routes.
     * The configuration applies to each instance of HTTPClientManager (max_total_connections=10 and instances=5 leads to 50 connections).
     * From googling around the connection pools maxTotal value should be close to the number of available threads.
     *
     * https://stackoverflow.com/questions/30989637/how-to-decide-optimal-settings-for-setmaxtotal-and-setdefaultmaxperroute
     */
    public static final Setting<Integer> MAX_TOTAL_CONNECTIONS = Setting.intSetting(
        "xpack.inference.http.max_total_connections",
        50, // default
        1, // min
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The max number of connections a single route can lease.
     * This configuration applies to each instance of HttpClientManager.
     */
    public static final Setting<Integer> MAX_ROUTE_CONNECTIONS = Setting.intSetting(
        "xpack.inference.http.max_route_connections",
        20, // default
        1, // min
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final TimeValue DEFAULT_CONNECTION_EVICTION_THREAD_INTERVAL_TIME = TimeValue.timeValueMinutes(1);
    public static final Setting<TimeValue> CONNECTION_EVICTION_THREAD_INTERVAL_SETTING = Setting.timeSetting(
        "xpack.inference.http.connection_eviction_interval",
        DEFAULT_CONNECTION_EVICTION_THREAD_INTERVAL_TIME,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final TimeValue DEFAULT_CONNECTION_MAX_IDLE_TIME_SETTING = DEFAULT_CONNECTION_EVICTION_THREAD_INTERVAL_TIME;
    /**
     * The max duration of time for a connection to be marked as idle and ready to be closed. This defines the amount of time
     * a connection can be unused in the connection pool before being closed the next time the eviction thread runs.
     * It also defines the keep-alive value for the connection if one is not specified by the 3rd party service's server.
     *
     * For more info see the answer here:
     * https://stackoverflow.com/questions/64676200/understanding-the-lifecycle-of-a-connection-managed-by-poolinghttpclientconnecti
     */
    public static final Setting<TimeValue> CONNECTION_MAX_IDLE_TIME_SETTING = Setting.timeSetting(
        "xpack.inference.http.connection_eviction_max_idle_time",
        DEFAULT_CONNECTION_MAX_IDLE_TIME_SETTING,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final ThreadPool threadPool;
    private final PoolingAsyncClientConnectionManager connectionManager;
    private IdleConnectionEvictor connectionEvictor;
    private final HttpClient httpClient;

    private volatile TimeValue evictionInterval;
    private volatile TimeValue connectionMaxIdle;

    public static HttpClientManager create(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        ThrottlerManager throttlerManager,
        CircuitBreaker circuitBreaker
    ) {
        return create(settings, threadPool, clusterService, throttlerManager, circuitBreaker, null);
    }

    public static HttpClientManager create(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        ThrottlerManager throttlerManager,
        CircuitBreaker circuitBreaker,
        @Nullable TimeValue connectionTtl
    ) {
        var connectionManager = createConnectionManager(null, connectionTtl, HttpSettings.CONNECTION_TIMEOUT.get(settings));
        return new HttpClientManager(settings, connectionManager, threadPool, clusterService, throttlerManager, circuitBreaker);
    }

    public static HttpClientManager create(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        ThrottlerManager throttlerManager,
        SSLService sslService,
        TimeValue connectionTtl,
        CircuitBreaker circuitBreaker
    ) {
        // Set the TLS strategy to ensure an encrypted connection, as Elastic Inference Service requires it.
        var tlsStrategy = sslService.profile(ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX).clientTlsStrategy();
        var connectionManager = createConnectionManager(tlsStrategy, connectionTtl, HttpSettings.CONNECTION_TIMEOUT.get(settings));
        return new HttpClientManager(settings, connectionManager, threadPool, clusterService, throttlerManager, circuitBreaker);
    }

    // Default for testing
    HttpClientManager(
        Settings settings,
        PoolingAsyncClientConnectionManager connectionManager,
        ThreadPool threadPool,
        ClusterService clusterService,
        ThrottlerManager throttlerManager,
        CircuitBreaker circuitBreaker
    ) {
        this.threadPool = threadPool;

        this.connectionManager = connectionManager;
        setMaxConnections(MAX_TOTAL_CONNECTIONS.get(settings));
        setMaxRouteConnections(MAX_ROUTE_CONNECTIONS.get(settings));

        this.httpClient = HttpClient.create(
            new HttpSettings(settings, clusterService),
            threadPool,
            connectionManager,
            throttlerManager,
            circuitBreaker
        );

        this.evictionInterval = CONNECTION_EVICTION_THREAD_INTERVAL_SETTING.get(settings);
        this.connectionMaxIdle = CONNECTION_MAX_IDLE_TIME_SETTING.get(settings);
        connectionEvictor = createConnectionEvictor();

        this.addSettingsUpdateConsumers(clusterService);
    }

    // TODO (httpclient5 migration): the connect timeout moved from the 4.x RequestConfig to the connection manager's
    // ConnectionConfig (the 5.x RequestConfig variant is deprecated), and the EIS TLS strategy now comes from
    // SslProfile.clientTlsStrategy() instead of ioSessionStrategy(). Verify the EIS mTLS path (including reloadable certs and
    // verification_mode) against a real EIS endpoint before merging.
    private static PoolingAsyncClientConnectionManager createConnectionManager(
        @Nullable TlsStrategy tlsStrategy,
        @Nullable TimeValue connectionTtl,
        TimeValue connectTimeout
    ) {
        var connectionConfig = ConnectionConfig.custom().setConnectTimeout(Timeout.ofMilliseconds(connectTimeout.millis()));

        /*
          If the connection TTL is not set, the TTL will be controlled using the IdleConnectionEvictor and keep-alive strategy.
          The max idle time cluster setting will dictate how much time an open connection can be unused for before it can be closed.
         */
        if (connectionTtl != null) {
            connectionConfig.setTimeToLive(org.apache.hc.core5.util.TimeValue.ofMilliseconds(connectionTtl.millis()));
        }

        var builder = PoolingAsyncClientConnectionManagerBuilder.create().setDefaultConnectionConfig(connectionConfig.build());

        // When no TLS strategy is provided the builder installs the default one, which uses the JVM's default SSL context,
        // matching the 4.x client's default SSL session strategy.
        if (tlsStrategy != null) {
            builder.setTlsStrategy(tlsStrategy);
        }

        return builder.build();
    }

    private void addSettingsUpdateConsumers(ClusterService clusterService) {
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_TOTAL_CONNECTIONS, this::setMaxConnections);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_ROUTE_CONNECTIONS, this::setMaxRouteConnections);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(CONNECTION_EVICTION_THREAD_INTERVAL_SETTING, this::setEvictionInterval);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CONNECTION_MAX_IDLE_TIME_SETTING, this::setConnectionMaxIdle);
    }

    private IdleConnectionEvictor createConnectionEvictor() {
        return new IdleConnectionEvictor(threadPool, connectionManager, evictionInterval, connectionMaxIdle);
    }

    public static List<Setting<?>> getSettingsDefinitions() {
        return List.of(
            MAX_TOTAL_CONNECTIONS,
            MAX_ROUTE_CONNECTIONS,
            CONNECTION_EVICTION_THREAD_INTERVAL_SETTING,
            CONNECTION_MAX_IDLE_TIME_SETTING
        );
    }

    public void start() {
        httpClient.start();
        connectionEvictor.start();
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }

    public PoolStats getPoolStats() {
        return connectionManager.getTotalStats();
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
        connectionEvictor.close();
    }

    private void setMaxConnections(int maxConnections) {
        connectionManager.setMaxTotal(maxConnections);
    }

    private void setMaxRouteConnections(int maxConnections) {
        connectionManager.setDefaultMaxPerRoute(maxConnections);
    }

    // This is only used for testing
    boolean isEvictionThreadRunning() {
        return connectionEvictor.isRunning();
    }

    // default for testing
    void setEvictionInterval(TimeValue evictionInterval) {
        logger.debug(() -> format("Eviction thread's interval time updated to [%s]", evictionInterval));
        this.evictionInterval = evictionInterval;

        connectionEvictor.close();
        connectionEvictor = createConnectionEvictor();
        connectionEvictor.start();
    }

    void setConnectionMaxIdle(TimeValue connectionMaxIdle) {
        logger.debug(() -> format("Eviction thread's max idle time updated to [%s]", connectionMaxIdle));
        this.connectionMaxIdle = connectionMaxIdle;
        connectionEvictor.setMaxIdleTime(connectionMaxIdle);
    }
}
