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

package org.elasticsearch.client;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;

import javax.net.ssl.SSLContext;
import java.security.AccessController;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.util.Objects;

/**
 * Helps creating a new {@link RestClient}. Allows to set the most common http client configuration options when internally
 * creating the underlying {@link org.apache.http.nio.client.HttpAsyncClient}. Also allows to provide an externally created
 * {@link org.apache.http.nio.client.HttpAsyncClient} in case additional customization is needed.
 */
public final class RestClientBuilder {
    public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 1000;
    public static final int DEFAULT_SOCKET_TIMEOUT_MILLIS = 30000;
    public static final int DEFAULT_MAX_RETRY_TIMEOUT_MILLIS = DEFAULT_SOCKET_TIMEOUT_MILLIS;
    public static final int DEFAULT_CONNECTION_REQUEST_TIMEOUT_MILLIS = 500;
    public static final int DEFAULT_MAX_CONN_PER_ROUTE = 10;
    public static final int DEFAULT_MAX_CONN_TOTAL = 30;

    private static final Header[] EMPTY_HEADERS = new Header[0];

    private final HttpHost[] hosts;
    private int maxRetryTimeout = DEFAULT_MAX_RETRY_TIMEOUT_MILLIS;
    private Header[] defaultHeaders = EMPTY_HEADERS;
    private RestClient.FailureListener failureListener;
    private HttpClientConfigCallback httpClientConfigCallback;
    private RequestConfigCallback requestConfigCallback;
    private String pathPrefix;

    /**
     * Creates a new builder instance and sets the hosts that the client will send requests to.
     *
     * @throws NullPointerException if {@code hosts} or any host is {@code null}.
     * @throws IllegalArgumentException if {@code hosts} is empty.
     */
    RestClientBuilder(HttpHost... hosts) {
        Objects.requireNonNull(hosts, "hosts must not be null");
        if (hosts.length == 0) {
            throw new IllegalArgumentException("no hosts provided");
        }
        for (HttpHost host : hosts) {
            Objects.requireNonNull(host, "host cannot be null");
        }
        this.hosts = hosts;
    }

    /**
     * Sets the default request headers, which will be sent along with each request.
     * <p>
     * Request-time headers will always overwrite any default headers.
     *
     * @throws NullPointerException if {@code defaultHeaders} or any header is {@code null}.
     */
    public RestClientBuilder setDefaultHeaders(Header[] defaultHeaders) {
        Objects.requireNonNull(defaultHeaders, "defaultHeaders must not be null");
        for (Header defaultHeader : defaultHeaders) {
            Objects.requireNonNull(defaultHeader, "default header must not be null");
        }
        this.defaultHeaders = defaultHeaders;
        return this;
    }

    /**
     * Sets the {@link RestClient.FailureListener} to be notified for each request failure
     *
     * @throws NullPointerException if {@code failureListener} is {@code null}.
     */
    public RestClientBuilder setFailureListener(RestClient.FailureListener failureListener) {
        Objects.requireNonNull(failureListener, "failureListener must not be null");
        this.failureListener = failureListener;
        return this;
    }

    /**
     * Sets the maximum timeout (in milliseconds) to honour in case of multiple retries of the same request.
     * {@link #DEFAULT_MAX_RETRY_TIMEOUT_MILLIS} if not specified.
     *
     * @throws IllegalArgumentException if {@code maxRetryTimeoutMillis} is not greater than 0
     */
    public RestClientBuilder setMaxRetryTimeoutMillis(int maxRetryTimeoutMillis) {
        if (maxRetryTimeoutMillis <= 0) {
            throw new IllegalArgumentException("maxRetryTimeoutMillis must be greater than 0");
        }
        this.maxRetryTimeout = maxRetryTimeoutMillis;
        return this;
    }

    /**
     * Sets the {@link HttpClientConfigCallback} to be used to customize http client configuration
     *
     * @throws NullPointerException if {@code httpClientConfigCallback} is {@code null}.
     */
    public RestClientBuilder setHttpClientConfigCallback(HttpClientConfigCallback httpClientConfigCallback) {
        Objects.requireNonNull(httpClientConfigCallback, "httpClientConfigCallback must not be null");
        this.httpClientConfigCallback = httpClientConfigCallback;
        return this;
    }

    /**
     * Sets the {@link RequestConfigCallback} to be used to customize http client configuration
     *
     * @throws NullPointerException if {@code requestConfigCallback} is {@code null}.
     */
    public RestClientBuilder setRequestConfigCallback(RequestConfigCallback requestConfigCallback) {
        Objects.requireNonNull(requestConfigCallback, "requestConfigCallback must not be null");
        this.requestConfigCallback = requestConfigCallback;
        return this;
    }

    /**
     * Sets the path's prefix for every request used by the http client.
     * <p>
     * For example, if this is set to "/my/path", then any client request will become <code>"/my/path/" + endpoint</code>.
     * <p>
     * In essence, every request's {@code endpoint} is prefixed by this {@code pathPrefix}. The path prefix is useful for when
     * Elasticsearch is behind a proxy that provides a base path; it is not intended for other purposes and it should not be supplied in
     * other scenarios.
     *
     * @throws NullPointerException if {@code pathPrefix} is {@code null}.
     * @throws IllegalArgumentException if {@code pathPrefix} is empty, only '/', or ends with more than one '/'.
     */
    public RestClientBuilder setPathPrefix(String pathPrefix) {
        Objects.requireNonNull(pathPrefix, "pathPrefix must not be null");
        String cleanPathPrefix = pathPrefix;

        if (cleanPathPrefix.startsWith("/") == false) {
            cleanPathPrefix = "/" + cleanPathPrefix;
        }

        // best effort to ensure that it looks like "/base/path" rather than "/base/path/"
        if (cleanPathPrefix.endsWith("/")) {
            cleanPathPrefix = cleanPathPrefix.substring(0, cleanPathPrefix.length() - 1);

            if (cleanPathPrefix.endsWith("/")) {
                throw new IllegalArgumentException("pathPrefix is malformed. too many trailing slashes: [" + pathPrefix + "]");
            }
        }

        if (cleanPathPrefix.isEmpty() || "/".equals(cleanPathPrefix)) {
            throw new IllegalArgumentException("pathPrefix must not be empty or '/': [" + pathPrefix + "]");
        }

        this.pathPrefix = cleanPathPrefix;
        return this;
    }

    /**
     * Creates a new {@link RestClient} based on the provided configuration.
     */
    public RestClient build() {
        if (failureListener == null) {
            failureListener = new RestClient.FailureListener();
        }
        CloseableHttpAsyncClient httpClient = AccessController.doPrivileged(new PrivilegedAction<CloseableHttpAsyncClient>() {
            @Override
            public CloseableHttpAsyncClient run() {
                return createHttpClient();
            }
        });
        RestClient restClient = new RestClient(httpClient, maxRetryTimeout, defaultHeaders, hosts, pathPrefix, failureListener);
        httpClient.start();
        return restClient;
    }

    private CloseableHttpAsyncClient createHttpClient() {
        //default timeouts are all infinite
        RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
                .setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_MILLIS)
                .setSocketTimeout(DEFAULT_SOCKET_TIMEOUT_MILLIS)
                .setConnectionRequestTimeout(DEFAULT_CONNECTION_REQUEST_TIMEOUT_MILLIS);
        if (requestConfigCallback != null) {
            requestConfigBuilder = requestConfigCallback.customizeRequestConfig(requestConfigBuilder);
        }

        try {
            HttpAsyncClientBuilder httpClientBuilder = HttpAsyncClientBuilder.create().setDefaultRequestConfig(requestConfigBuilder.build())
                //default settings for connection pooling may be too constraining
                .setMaxConnPerRoute(DEFAULT_MAX_CONN_PER_ROUTE).setMaxConnTotal(DEFAULT_MAX_CONN_TOTAL)
                .setSSLContext(SSLContext.getDefault());
            if (httpClientConfigCallback != null) {
                httpClientBuilder = httpClientConfigCallback.customizeHttpClient(httpClientBuilder);
            }

            final HttpAsyncClientBuilder finalBuilder = httpClientBuilder;
            return AccessController.doPrivileged(new PrivilegedAction<CloseableHttpAsyncClient>() {
                @Override
                public CloseableHttpAsyncClient run() {
                    return finalBuilder.build();
                }
            });
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("could not create the default ssl context", e);
        }
    }

    /**
     * Callback used the default {@link RequestConfig} being set to the {@link CloseableHttpClient}
     * @see HttpClientBuilder#setDefaultRequestConfig
     */
    public interface RequestConfigCallback {
        /**
         * Allows to customize the {@link RequestConfig} that will be used with each request.
         * It is common to customize the different timeout values through this method without losing any other useful default
         * value that the {@link RestClientBuilder} internally sets.
         */
        RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder);
    }

    /**
     * Callback used to customize the {@link CloseableHttpClient} instance used by a {@link RestClient} instance.
     * Allows to customize default {@link RequestConfig} being set to the client and any parameter that
     * can be set through {@link HttpClientBuilder}
     */
    public interface HttpClientConfigCallback {
        /**
         * Allows to customize the {@link CloseableHttpAsyncClient} being created and used by the {@link RestClient}.
         * Commonly used to customize the default {@link org.apache.http.client.CredentialsProvider} for authentication
         * or the {@link SchemeIOSessionStrategy} for communication through ssl without losing any other useful default
         * value that the {@link RestClientBuilder} internally sets, like connection pooling.
         */
        HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder);
    }
}
