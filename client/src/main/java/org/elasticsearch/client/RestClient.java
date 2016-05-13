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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class RestClient implements Closeable {

    private static final Log logger = LogFactory.getLog(RestClient.class);

    private final CloseableHttpClient client;
    private final long maxRetryTimeout;
    private final AtomicInteger lastConnectionIndex = new AtomicInteger(0);
    private volatile List<Connection> connections;
    private volatile FailureListener failureListener = new FailureListener();

    private RestClient(CloseableHttpClient client, long maxRetryTimeout, HttpHost... hosts) {
        this.client = client;
        this.maxRetryTimeout = maxRetryTimeout;
        setNodes(hosts);
    }

    public synchronized void setNodes(HttpHost... hosts) {
        List<Connection> connections = new ArrayList<>(hosts.length);
        for (HttpHost host : hosts) {
            Objects.requireNonNull(host, "host cannot be null");
            connections.add(new Connection(host));
        }
        this.connections = Collections.unmodifiableList(connections);
    }

    public ElasticsearchResponse performRequest(String method, String endpoint, Map<String, Object> params, HttpEntity entity)
            throws IOException {
        URI uri = buildUri(endpoint, params);
        HttpRequestBase request = createHttpRequest(method, uri, entity);
        //we apply a soft margin so that e.g. if a request took 59 seconds and timeout is set to 60 we don't do another attempt
        long retryTimeout = Math.round(this.maxRetryTimeout / (float)100 * 98);
        IOException lastSeenException = null;
        long startTime = System.nanoTime();
        Iterator<Connection> connectionIterator = nextConnection();
        while (connectionIterator.hasNext()) {
            Connection connection = connectionIterator.next();

            if (lastSeenException != null) {
                long timeElapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                long timeout = retryTimeout - timeElapsed;
                if (timeout <= 0) {
                    IOException retryTimeoutException = new IOException(
                            "request retries exceeded max retry timeout [" + retryTimeout + "]");
                    retryTimeoutException.addSuppressed(lastSeenException);
                    throw retryTimeoutException;
                }
                request.reset();
            }

            CloseableHttpResponse response;
            try {
                response = client.execute(connection.getHost(), request);
            } catch(IOException e) {
                RequestLogger.log(logger, "request failed", request, connection.getHost(), e);
                onFailure(connection);
                lastSeenException = addSuppressedException(lastSeenException, e);
                continue;
            }
            int statusCode = response.getStatusLine().getStatusCode();
            //TODO make ignore status code configurable. rest-spec and tests support that parameter (ignore_missing)
            if (statusCode < 300 || (request.getMethod().equals(HttpHead.METHOD_NAME) && statusCode == 404) ) {
                RequestLogger.log(logger, "request succeeded", request, connection.getHost(), response);
                onSuccess(connection);
                return new ElasticsearchResponse(request.getRequestLine(), connection.getHost(), response);
            } else {
                RequestLogger.log(logger, "request failed", request, connection.getHost(), response);
                String responseBody = null;
                if (response.getEntity() != null) {
                    responseBody = EntityUtils.toString(response.getEntity());
                }
                ElasticsearchResponseException elasticsearchResponseException = new ElasticsearchResponseException(
                        request.getRequestLine(), connection.getHost(), response.getStatusLine(), responseBody);
                lastSeenException = addSuppressedException(lastSeenException, elasticsearchResponseException);
                //clients don't retry on 500 because elasticsearch still misuses it instead of 400 in some places
                if (statusCode == 502 || statusCode == 503 || statusCode == 504) {
                    onFailure(connection);
                } else {
                    //don't retry and call onSuccess as the error should be a request problem, the node is alive
                    onSuccess(connection);
                    break;
                }
            }
        }
        assert lastSeenException != null;
        throw lastSeenException;
    }

    /**
     * Returns an iterator of connections that should be used for a request call.
     * Ideally, the first connection is retrieved from the iterator and used successfully for the request.
     * Otherwise, after each failure the next connection should be retrieved from the iterator so that the request can be retried.
     * The maximum total of attempts is equal to the number of connections that are available in the iterator.
     * The iterator returned will never be empty, rather an {@link IllegalStateException} will be thrown in that case.
     * In case there are no alive connections available, or dead ones that should be retried, one dead connection
     * gets resurrected and returned.
     */
    private Iterator<Connection> nextConnection() {
        if (this.connections.isEmpty()) {
            throw new IllegalStateException("no connections available");
        }

        List<Connection> rotatedConnections = new ArrayList<>(connections);
        //TODO is it possible to make this O(1)? (rotate is O(n))
        Collections.rotate(rotatedConnections, rotatedConnections.size() - lastConnectionIndex.getAndIncrement());
        Iterator<Connection> connectionIterator = rotatedConnections.iterator();
        while (connectionIterator.hasNext()) {
            Connection connection = connectionIterator.next();
            if (connection.isBlacklisted()) {
                connectionIterator.remove();
            }
        }
        if (rotatedConnections.isEmpty()) {
            List<Connection> sortedConnections = new ArrayList<>(connections);
            Collections.sort(sortedConnections, new Comparator<Connection>() {
                @Override
                public int compare(Connection o1, Connection o2) {
                    return Long.compare(o1.getDeadUntil(), o2.getDeadUntil());
                }
            });
            Connection connection = sortedConnections.get(0);
            logger.trace("trying to resurrect connection for " + connection.getHost());
            return Collections.singleton(connection).iterator();
        }
        return rotatedConnections.iterator();
    }

    /**
     * Called after each successful request call.
     * Receives as an argument the connection that was used for the successful request.
     */
    public void onSuccess(Connection connection) {
        connection.markAlive();
        logger.trace("marked connection alive for " + connection.getHost());
    }

    /**
     * Called after each failed attempt.
     * Receives as an argument the connection that was used for the failed attempt.
     */
    private void onFailure(Connection connection) throws IOException {
        connection.markDead();
        logger.debug("marked connection dead for " + connection.getHost());
        failureListener.onFailure(connection);
    }

    public synchronized void setFailureListener(FailureListener failureListener) {
        this.failureListener = failureListener;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private static IOException addSuppressedException(IOException suppressedException, IOException currentException) {
        if (suppressedException != null) {
            currentException.addSuppressed(suppressedException);
        }
        return currentException;
    }

    private static HttpRequestBase createHttpRequest(String method, URI uri, HttpEntity entity) {
        switch(method.toUpperCase(Locale.ROOT)) {
            case HttpDeleteWithEntity.METHOD_NAME:
                HttpDeleteWithEntity httpDeleteWithEntity = new HttpDeleteWithEntity(uri);
                addRequestBody(httpDeleteWithEntity, entity);
                return httpDeleteWithEntity;
            case HttpGetWithEntity.METHOD_NAME:
                HttpGetWithEntity httpGetWithEntity = new HttpGetWithEntity(uri);
                addRequestBody(httpGetWithEntity, entity);
                return httpGetWithEntity;
            case HttpHead.METHOD_NAME:
                if (entity != null) {
                    throw new UnsupportedOperationException("HEAD with body is not supported");
                }
                return new HttpHead(uri);
            case HttpPost.METHOD_NAME:
                HttpPost httpPost = new HttpPost(uri);
                addRequestBody(httpPost, entity);
                return httpPost;
            case HttpPut.METHOD_NAME:
                HttpPut httpPut = new HttpPut(uri);
                addRequestBody(httpPut, entity);
                return httpPut;
            default:
                throw new UnsupportedOperationException("http method not supported: " + method);
        }
    }

    private static void addRequestBody(HttpEntityEnclosingRequestBase httpRequest, HttpEntity entity) {
        if (entity != null) {
            httpRequest.setEntity(entity);
        }
    }

    private static URI buildUri(String path, Map<String, Object> params) {
        try {
            URIBuilder uriBuilder = new URIBuilder(path);
            for (Map.Entry<String, Object> param : params.entrySet()) {
                uriBuilder.addParameter(param.getKey(), param.getValue().toString());
            }
            return uriBuilder.build();
        } catch(URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    /**
     * Returns a new {@link Builder} to help with {@link RestClient} creation.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Rest client builder. Helps creating a new {@link RestClient}.
     */
    public static final class Builder {
        public static final int DEFAULT_CONNECT_TIMEOUT = 500;
        public static final int DEFAULT_SOCKET_TIMEOUT = 5000;
        public static final int DEFAULT_MAX_RETRY_TIMEOUT = DEFAULT_SOCKET_TIMEOUT;
        public static final int DEFAULT_CONNECTION_REQUEST_TIMEOUT = 500;

        private CloseableHttpClient httpClient;
        private int maxRetryTimeout = DEFAULT_MAX_RETRY_TIMEOUT;
        private HttpHost[] hosts;

        private Builder() {

        }

        /**
         * Sets the http client. A new default one will be created if not specified, by calling {@link #createDefaultHttpClient()}.
         *
         * @see CloseableHttpClient
         */
        public Builder setHttpClient(CloseableHttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        /**
         * Sets the maximum timeout to honour in case of multiple retries of the same request.
         * {@link #DEFAULT_MAX_RETRY_TIMEOUT} if not specified.
         *
         * @throws IllegalArgumentException if maxRetryTimeout is not greater than 0
         */
        public Builder setMaxRetryTimeout(int maxRetryTimeout) {
            if (maxRetryTimeout <= 0) {
                throw new IllegalArgumentException("maxRetryTimeout must be greater than 0");
            }
            this.maxRetryTimeout = maxRetryTimeout;
            return this;
        }

        /**
         * Sets the hosts that the client will send requests to.
         */
        public Builder setHosts(HttpHost... hosts) {
            if (hosts == null || hosts.length == 0) {
                throw new IllegalArgumentException("no hosts provided");
            }
            this.hosts = hosts;
            return this;
        }

        /**
         * Creates a new {@link RestClient} based on the provided configuration.
         */
        public RestClient build() {
            if (httpClient == null) {
                httpClient = createDefaultHttpClient();
            }
            return new RestClient(httpClient, maxRetryTimeout, hosts);
        }

        /**
         * Creates an http client with default settings
         *
         * @see CloseableHttpClient
         */
        public static CloseableHttpClient createDefaultHttpClient() {
            PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
            //default settings may be too constraining
            connectionManager.setDefaultMaxPerRoute(10);
            connectionManager.setMaxTotal(30);

            //default timeouts are all infinite
            RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(DEFAULT_CONNECT_TIMEOUT)
                    .setSocketTimeout(DEFAULT_SOCKET_TIMEOUT)
                    .setConnectionRequestTimeout(DEFAULT_CONNECTION_REQUEST_TIMEOUT).build();

            return HttpClientBuilder.create().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig).build();
        }
    }

    /**
     * Listener that allows to be notified whenever a failure happens. Useful when sniffing is enabled, so that we can sniff on failure.
     * The default implementation is a no-op.
     */
    public static class FailureListener {
        public void onFailure(Connection connection) throws IOException {

        }
    }
}
