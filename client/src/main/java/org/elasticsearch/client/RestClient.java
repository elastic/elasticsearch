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
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class RestClient implements Closeable {

    private static final Log logger = LogFactory.getLog(RestClient.class);

    private final CloseableHttpClient client;
    private final ConnectionPool connectionPool;
    private final long maxRetryTimeout;

    private RestClient(CloseableHttpClient client, ConnectionPool connectionPool, long maxRetryTimeout) {
        this.client = client;
        this.connectionPool = connectionPool;
        this.maxRetryTimeout = maxRetryTimeout;
    }

    public ElasticsearchResponse performRequest(String method, String endpoint, Map<String, Object> params, HttpEntity entity)
            throws IOException {
        URI uri = buildUri(endpoint, params);
        HttpRequestBase request = createHttpRequest(method, uri, entity);
        return performRequest(request, connectionPool.nextConnection());
    }

    private ElasticsearchResponse performRequest(HttpRequestBase request, Iterator<Connection> connectionIterator) throws IOException {
        //we apply a soft margin so that e.g. if a request took 59 seconds and timeout is set to 60 we don't do another attempt
        long retryTimeout = Math.round(this.maxRetryTimeout / (float)100 * 98);
        IOException lastSeenException = null;
        long startTime = System.nanoTime();

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
            }

            CloseableHttpResponse response;
            try {
                response = client.execute(connection.getHost(), request);
            } catch(IOException e) {
                RequestLogger.log(logger, "request failed", request, connection.getHost(), e);
                connectionPool.onFailure(connection);
                lastSeenException = addSuppressedException(lastSeenException, e);
                continue;
            } finally {
                request.reset();
            }
            int statusCode = response.getStatusLine().getStatusCode();
            //TODO make ignore status code configurable. rest-spec and tests support that parameter (ignore_missing)
            if (statusCode < 300 || (request.getMethod().equals(HttpHead.METHOD_NAME) && statusCode == 404) ) {
                RequestLogger.log(logger, "request succeeded", request, connection.getHost(), response);
                connectionPool.onSuccess(connection);
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
                    connectionPool.onFailure(connection);
                } else {
                    //don't retry and call onSuccess as the error should be a request problem, the node is alive
                    connectionPool.onSuccess(connection);
                    break;
                }
            }
        }
        assert lastSeenException != null;
        throw lastSeenException;
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

    @Override
    public void close() throws IOException {
        connectionPool.close();
        client.close();
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
        private static final int DEFAULT_MAX_RETRY_TIMEOUT = 10000;

        private ConnectionPool connectionPool;
        private CloseableHttpClient httpClient;
        private int maxRetryTimeout = DEFAULT_MAX_RETRY_TIMEOUT;
        private HttpHost[] hosts;

        private Builder() {

        }

        /**
         * Sets the connection pool. {@link StaticConnectionPool} will be used if not specified.
         * @see ConnectionPool
         */
        public Builder setConnectionPool(ConnectionPool connectionPool) {
            this.connectionPool = connectionPool;
            return this;
        }

        /**
         * Sets the http client. A new default one will be created if not specified, by calling {@link #createDefaultHttpClient()}.
         * @see CloseableHttpClient
         */
        public Builder setHttpClient(CloseableHttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        /**
         * Sets the maximum timeout to honour in case of multiple retries of the same request.
         * {@link #DEFAULT_MAX_RETRY_TIMEOUT} if not specified.
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
         * Sets the hosts that the client will send requests to. Mandatory if no connection pool is specified,
         * as the provided hosts will be used to create the default static connection pool.
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
            if (connectionPool == null) {
                connectionPool = new StaticConnectionPool(hosts);
            }
            return new RestClient(httpClient, connectionPool, maxRetryTimeout);
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
            RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(500).setSocketTimeout(10000)
                    .setConnectionRequestTimeout(500).build();

            return HttpClientBuilder.create().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig).build();
        }
    }
}
