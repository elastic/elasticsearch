/*
 * Copyright 2010 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.util.http.client;

import javax.net.ssl.SSLEngine;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * Configuration class to use with a {@link AsyncHttpClient}. System property can be also used to configure this
 * object default behavior by doing:
 * <p/>
 * -Dcom.ning.http.client.AsyncHttpClientConfig.nameOfTheProperty
 * ex:
 * <p/>
 * -Dcom.ning.http.client.AsyncHttpClientConfig.defaultMaxTotalConnections
 * -Dcom.ning.http.client.AsyncHttpClientConfig.defaultMaxTotalConnections
 * -Dcom.ning.http.client.AsyncHttpClientConfig.defaultMaxConnectionsPerHost
 * -Dcom.ning.http.client.AsyncHttpClientConfig.defaultConnectionTimeoutInMS
 * -Dcom.ning.http.client.AsyncHttpClientConfig.defaultIdleConnectionTimeoutInMS
 * -Dcom.ning.http.client.AsyncHttpClientConfig.defaultRequestTimeoutInMS
 * -Dcom.ning.http.client.AsyncHttpClientConfig.defaultRedirectsEnabled
 * -Dcom.ning.http.client.AsyncHttpClientConfig.defaultMaxRedirects
 */
public class AsyncHttpClientConfig {

    private final static String ASYNC_CLIENT = AsyncHttpClient.class.getName() + ".";

    private final int maxTotalConnections;
    private final int maxConnectionPerHost;
    private final int connectionTimeOutInMs;
    private final int idleConnectionTimeoutInMs;
    private final int requestTimeoutInMs;
    private final boolean redirectEnabled;
    private final int maxDefaultRedirects;
    private final boolean compressionEnabled;
    private final String userAgent;
    private final boolean keepAlive;
    private final ScheduledExecutorService reaper;
    private final ExecutorService applicationThreadPool;
    private final ProxyServer proxyServer;
    private final SSLEngine sslEngine;

    private AsyncHttpClientConfig(int maxTotalConnections,
                                  int maxConnectionPerHost,
                                  int connectionTimeOutInMs,
                                  int idleConnectionTimeoutInMs,
                                  int requestTimeoutInMs,
                                  boolean redirectEnabled,
                                  int maxDefaultRedirects,
                                  boolean compressionEnabled,
                                  String userAgent,
                                  boolean keepAlive,
                                  ScheduledExecutorService reaper,
                                  ExecutorService applicationThreadPool,
                                  ProxyServer proxyServer,
                                  SSLEngine sslEngine) {

        this.maxTotalConnections = maxTotalConnections;
        this.maxConnectionPerHost = maxConnectionPerHost;
        this.connectionTimeOutInMs = connectionTimeOutInMs;
        this.idleConnectionTimeoutInMs = idleConnectionTimeoutInMs;
        this.requestTimeoutInMs = requestTimeoutInMs;
        this.redirectEnabled = redirectEnabled;
        this.maxDefaultRedirects = maxDefaultRedirects;
        this.compressionEnabled = compressionEnabled;
        this.userAgent = userAgent;
        this.keepAlive = keepAlive;
        this.sslEngine = sslEngine;

        if (reaper == null) {
            this.reaper = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                public Thread newThread(Runnable r) {
                    return new Thread(r, "AsyncHttpClient-Reaper");
                }
            });
        } else {
            this.reaper = reaper;
        }

        if (applicationThreadPool == null) {
            this.applicationThreadPool = Executors.newCachedThreadPool();
        } else {
            this.applicationThreadPool = applicationThreadPool;
        }
        this.proxyServer = proxyServer;
    }

    /**
     * A {@link ScheduledExecutorService} used to expire idle connections.
     *
     * @return {@link ScheduledExecutorService}
     */
    public ScheduledExecutorService reaper() {
        return reaper;
    }

    /**
     * Return the maximum number of connections an {@link org.elasticsearch.util.http.client.AsyncHttpClient} can handle.
     *
     * @return the maximum number of connections an {@link org.elasticsearch.util.http.client.AsyncHttpClient} can handle.
     */
    public int getMaxTotalConnections() {
        return maxTotalConnections;
    }

    /**
     * Return the maximum number of connections per hosts an {@link org.elasticsearch.util.http.client.AsyncHttpClient} can handle.
     *
     * @return the maximum number of connections per host an {@link org.elasticsearch.util.http.client.AsyncHttpClient} can handle.
     */
    public int getMaxConnectionPerHost() {
        return maxConnectionPerHost;
    }

    /**
     * Return the maximum time in millisecond an {@link org.elasticsearch.util.http.client.AsyncHttpClient} can wait when connecting to a remote host
     *
     * @return the maximum time in millisecond an {@link org.elasticsearch.util.http.client.AsyncHttpClient} can wait when connecting to a remote host
     */
    public int getConnectionTimeoutInMs() {
        return connectionTimeOutInMs;
    }

    /**
     * Return the maximum time in millisecond an {@link org.elasticsearch.util.http.client.AsyncHttpClient} can stay idle.
     *
     * @return the maximum time in millisecond an {@link org.elasticsearch.util.http.client.AsyncHttpClient} can stay idle.
     */
    public int getIdleConnectionTimeoutInMs() {
        return idleConnectionTimeoutInMs;
    }

    /**
     * Return the maximum time in millisecond an {@link org.elasticsearch.util.http.client.AsyncHttpClient} wait for a response
     *
     * @return the maximum time in millisecond an {@link org.elasticsearch.util.http.client.AsyncHttpClient} wait for a response
     */
    public int getRequestTimeoutInMs() {
        return requestTimeoutInMs;
    }

    /**
     * Is HTTP redirect enabled
     *
     * @return true if enabled.
     */
    public boolean isRedirectEnabled() {
        return redirectEnabled;
    }

    /**
     * Get the maximum number of HTTP redirect
     *
     * @return the maximum number of HTTP redirect
     */
    public int getMaxRedirects() {
        return maxDefaultRedirects;
    }

    /**
     * Is HTTP keep-alive enabled.
     *
     * @return true if keep-alive is enabled
     */
    public boolean getKeepAlive() {
        return keepAlive;
    }

    /**
     * Return the USER_AGENT header value
     *
     * @return the USER_AGENT header value
     */
    public String getUserAgent() {
        return userAgent;
    }

    /**
     * Is HTTP compression enabled.
     *
     * @return true if compression is enabled
     */
    public boolean isCompressionEnabled() {
        return compressionEnabled;
    }

    /**
     * Return the {@link java.util.concurrent.ExecutorService} an {@link AsyncHttpClient} use for handling
     * asynchronous response.
     *
     * @return the {@link java.util.concurrent.ExecutorService} an {@link AsyncHttpClient} use for handling
     *         asynchronous response.
     */
    public ExecutorService executorService() {
        return applicationThreadPool;
    }

    /**
     * An instance of {@link org.elasticsearch.util.http.client.ProxyServer} used by an {@link AsyncHttpClient}
     *
     * @return instance of {@link org.elasticsearch.util.http.client.ProxyServer}
     */
    public ProxyServer getProxyServer() {
        return proxyServer;
    }

    /**
     * Return an instance of {@link SSLEngine} used for SSL connection.
     *
     * @return an instance of {@link SSLEngine} used for SSL connection.
     */
    public SSLEngine getSSLEngine() {
        return sslEngine;
    }

    /**
     * Builder for an {@link AsyncHttpClient}
     */
    public static class Builder {
        private int defaultMaxTotalConnections = Integer.getInteger(ASYNC_CLIENT + "defaultMaxTotalConnections", 2000);
        private int defaultMaxConnectionPerHost = Integer.getInteger(ASYNC_CLIENT + "defaultMaxConnectionsPerHost", 2000);
        private int defaultConnectionTimeOutInMs = Integer.getInteger(ASYNC_CLIENT + "defaultConnectionTimeoutInMS", 60 * 1000);
        private int defaultIdleConnectionTimeoutInMs = Integer.getInteger(ASYNC_CLIENT + "defaultIdleConnectionTimeoutInMS", 60 * 1000);
        private int defaultRequestTimeoutInMs = Integer.getInteger(ASYNC_CLIENT + "defaultRequestTimeoutInMS", 60 * 1000);
        private boolean redirectEnabled = Boolean.getBoolean(ASYNC_CLIENT + "defaultRedirectsEnabled");
        private int maxDefaultRedirects = Integer.getInteger(ASYNC_CLIENT + "defaultMaxRedirects", 5);
        private boolean compressionEnabled = Boolean.getBoolean(ASYNC_CLIENT + "compressionEnabled");
        private String userAgent = System.getProperty(ASYNC_CLIENT + "userAgent", "ES/1.0");
        private boolean keepAlive = true;
        private ScheduledExecutorService reaper = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
        private ExecutorService applicationThreadPool = Executors.newCachedThreadPool();
        private ProxyServer proxyServer = null;
        private SSLEngine sslEngine;

        public Builder() {
        }

        /**
         * Set the maximum number of connections an {@link org.elasticsearch.util.http.client.AsyncHttpClient} can handle.
         *
         * @param defaultMaxTotalConnections the maximum number of connections an {@link org.elasticsearch.util.http.client.AsyncHttpClient} can handle.
         * @return a {@link Builder}
         */
        public Builder setMaximumConnectionsTotal(int defaultMaxTotalConnections) {
            this.defaultMaxTotalConnections = defaultMaxTotalConnections;
            return this;
        }

        /**
         * Set the maximum number of connections per hosts an {@link org.elasticsearch.util.http.client.AsyncHttpClient} can handle.
         *
         * @param defaultMaxConnectionPerHost the maximum number of connections per host an {@link org.elasticsearch.util.http.client.AsyncHttpClient} can handle.
         * @return a {@link Builder}
         */
        public Builder setMaximumConnectionsPerHost(int defaultMaxConnectionPerHost) {
            this.defaultMaxConnectionPerHost = defaultMaxConnectionPerHost;
            return this;
        }

        /**
         * Set the maximum time in millisecond an {@link org.elasticsearch.util.http.client.AsyncHttpClient} can wait when connecting to a remote host
         *
         * @param defaultConnectionTimeOutInMs the maximum time in millisecond an {@link org.elasticsearch.util.http.client.AsyncHttpClient} can wait when connecting to a remote host
         * @return a {@link Builder}
         */
        public Builder setConnectionTimeoutInMs(int defaultConnectionTimeOutInMs) {
            this.defaultConnectionTimeOutInMs = defaultConnectionTimeOutInMs;
            return this;
        }

        /**
         * Set the maximum time in millisecond an {@link org.elasticsearch.util.http.client.AsyncHttpClient} can stay idle.
         *
         * @param defaultIdleConnectionTimeoutInMs
         *         the maximum time in millisecond an {@link org.elasticsearch.util.http.client.AsyncHttpClient} can stay idle.
         * @return a {@link Builder}
         */
        public Builder setIdleConnectionTimeoutInMs(int defaultIdleConnectionTimeoutInMs) {
            this.defaultIdleConnectionTimeoutInMs = defaultIdleConnectionTimeoutInMs;
            return this;
        }

        /**
         * Set the maximum time in millisecond an {@link org.elasticsearch.util.http.client.AsyncHttpClient} wait for a response
         *
         * @param defaultRequestTimeoutInMs the maximum time in millisecond an {@link org.elasticsearch.util.http.client.AsyncHttpClient} wait for a response
         * @return a {@link Builder}
         */
        public Builder setRequestTimeoutInMs(int defaultRequestTimeoutInMs) {
            this.defaultRequestTimeoutInMs = defaultRequestTimeoutInMs;
            return this;
        }

        /**
         * Set to true to enable HTTP redirect
         *
         * @param redirectEnabled true if enabled.
         * @return a {@link Builder}
         */
        public Builder setFollowRedirects(boolean redirectEnabled) {
            this.redirectEnabled = redirectEnabled;
            return this;
        }

        /**
         * Set the maximum number of HTTP redirect
         *
         * @param maxDefaultRedirects the maximum number of HTTP redirect
         * @return a {@link Builder}
         */
        public Builder setMaximumNumberOfRedirects(int maxDefaultRedirects) {
            this.maxDefaultRedirects = maxDefaultRedirects;
            return this;
        }

        /**
         * Enable HTTP compression.
         *
         * @param compressionEnabled true if compression is enabled
         * @return a {@link Builder}
         */
        public Builder setCompressionEnabled(boolean compressionEnabled) {
            this.compressionEnabled = compressionEnabled;
            return this;
        }

        /**
         * Set the USER_AGENT header value
         *
         * @param userAgent the USER_AGENT header value
         * @return a {@link Builder}
         */
        public Builder setUserAgent(String userAgent) {
            this.userAgent = userAgent;
            return this;
        }

        /**
         * Set HTTP keep-alive value.
         *
         * @param keepAlive true if keep-alive is enabled
         * @return a {@link Builder}
         */
        public Builder setKeepAlive(boolean keepAlive) {
            this.keepAlive = keepAlive;
            return this;
        }

        /**
         * Set the{@link ScheduledExecutorService} used to expire idle connections.
         *
         * @param reaper the{@link ScheduledExecutorService} used to expire idle connections.
         * @return a {@link Builder}
         */
        public Builder setScheduledExecutorService(ScheduledExecutorService reaper) {
            this.reaper = reaper;
            return this;
        }

        /**
         * Set the {@link java.util.concurrent.ExecutorService} an {@link AsyncHttpClient} use for handling
         * asynchronous response.
         *
         * @param applicationThreadPool the {@link java.util.concurrent.ExecutorService} an {@link AsyncHttpClient} use for handling
         *                              asynchronous response.
         * @return a {@link Builder}
         */
        public Builder setExecutorService(ExecutorService applicationThreadPool) {
            this.applicationThreadPool = applicationThreadPool;
            return this;
        }

        /**
         * Set an instance of {@link org.elasticsearch.util.http.client.ProxyServer} used by an {@link AsyncHttpClient}
         *
         * @param proxyServer instance of {@link org.elasticsearch.util.http.client.ProxyServer}
         * @return a {@link Builder}
         */
        public Builder setProxyServer(ProxyServer proxyServer) {
            this.proxyServer = proxyServer;
            return this;
        }

        /**
         * Set the {@link SSLEngine} for secure connection.
         *
         * @param sslEngine the {@link SSLEngine} for secure connection
         * @return a {@link Builder}
         */
        public Builder setSSLEngine(SSLEngine sslEngine) {
            this.sslEngine = sslEngine;
            return this;
        }

        /**
         * Build an {@link AsyncHttpClientConfig}
         *
         * @return an {@link AsyncHttpClientConfig}
         */
        public AsyncHttpClientConfig build() {
            return new AsyncHttpClientConfig(defaultMaxTotalConnections,
                    defaultMaxConnectionPerHost,
                    defaultConnectionTimeOutInMs,
                    defaultIdleConnectionTimeoutInMs,
                    defaultRequestTimeoutInMs,
                    redirectEnabled,
                    maxDefaultRedirects,
                    compressionEnabled,
                    userAgent,
                    keepAlive,
                    reaper,
                    applicationThreadPool,
                    proxyServer,
                    sslEngine);
        }

    }
}

