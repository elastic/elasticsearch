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
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.Registry;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.entity.ContentType;
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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Client that connects to an elasticsearch cluster through http.
 * Must be created using {@link Builder}, which allows to set all the different options or just rely on defaults.
 * The hosts that are part of the cluster need to be provided at creation time, but can also be replaced later
 * by calling {@link #setHosts(HttpHost...)}.
 * The method {@link #performRequest(String, String, Map, HttpEntity, Header...)} allows to send a request to the cluster. When
 * sending a request, a host gets selected out of the provided ones in a round-robin fashion. Failing hosts are marked dead and
 * retried after a certain amount of time (minimum 1 minute, maximum 30 minutes), depending on how many times they previously
 * failed (the more failures, the later they will be retried). In case of failures all of the alive nodes (or dead nodes that
 * deserve a retry) are retried till one responds or none of them does, in which case an {@link IOException} will be thrown.
 *
 * Requests can be traced by enabling trace logging for "tracer". The trace logger outputs requests and responses in curl format.
 */
public final class RestClient implements Closeable {

    private static final Log logger = LogFactory.getLog(RestClient.class);
    public static ContentType JSON_CONTENT_TYPE = ContentType.create("application/json", Consts.UTF_8);

    private final CloseableHttpClient client;
    //we don't rely on default headers supported by HttpClient as those cannot be replaced, plus it would get hairy
    //when we create the HttpClient instance on our own as there would be two different ways to set the default headers.
    private final Header[] defaultHeaders;
    private final long maxRetryTimeoutMillis;
    private final AtomicInteger lastHostIndex = new AtomicInteger(0);
    private volatile Set<HttpHost> hosts;
    private final ConcurrentMap<HttpHost, DeadHostState> blacklist = new ConcurrentHashMap<>();
    private volatile FailureListener failureListener = new FailureListener();

    private RestClient(CloseableHttpClient client, long maxRetryTimeoutMillis, Header[] defaultHeaders, HttpHost[] hosts) {
        this.client = client;
        this.maxRetryTimeoutMillis = maxRetryTimeoutMillis;
        this.defaultHeaders = defaultHeaders;
        setHosts(hosts);
    }

    /**
     * Replaces the hosts that the client communicates with.
     * @see HttpHost
     */
    public synchronized void setHosts(HttpHost... hosts) {
        if (hosts == null || hosts.length == 0) {
            throw new IllegalArgumentException("hosts must not be null nor empty");
        }
        Set<HttpHost> httpHosts = new HashSet<>();
        for (HttpHost host : hosts) {
            Objects.requireNonNull(host, "host cannot be null");
            httpHosts.add(host);
        }
        this.hosts = Collections.unmodifiableSet(httpHosts);
        this.blacklist.clear();
    }

    /**
     * Sends a request to the elasticsearch cluster that the current client points to.
     * Selects a host out of the provided ones in a round-robin fashion. Failing hosts are marked dead and retried after a certain
     * amount of time (minimum 1 minute, maximum 30 minutes), depending on how many times they previously failed (the more failures,
     * the later they will be retried). In case of failures all of the alive nodes (or dead nodes that deserve a retry) are retried
     * till one responds or none of them does, in which case an {@link IOException} will be thrown.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param entity the body of the request, null if not applicable
     * @param headers the optional request headers
     * @return the response returned by elasticsearch
     * @throws IOException in case of a problem or the connection was aborted
     * @throws ClientProtocolException in case of an http protocol error
     * @throws ResponseException in case elasticsearch responded with a status code that indicated an error
     */
    public Response performRequest(String method, String endpoint, Map<String, String> params,
                                   HttpEntity entity, Header... headers) throws IOException {
        URI uri = buildUri(endpoint, params);
        HttpRequestBase request = createHttpRequest(method, uri, entity);
        setHeaders(request, headers);
        //we apply a soft margin so that e.g. if a request took 59 seconds and timeout is set to 60 we don't do another attempt
        long retryTimeoutMillis = Math.round(this.maxRetryTimeoutMillis / (float)100 * 98);
        IOException lastSeenException = null;
        long startTime = System.nanoTime();
        for (HttpHost host : nextHost()) {
            if (lastSeenException != null) {
                //in case we are retrying, check whether maxRetryTimeout has been reached, in which case an exception will be thrown
                long timeElapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                long timeout = retryTimeoutMillis - timeElapsedMillis;
                if (timeout <= 0) {
                    IOException retryTimeoutException = new IOException(
                            "request retries exceeded max retry timeout [" + retryTimeoutMillis + "]");
                    retryTimeoutException.addSuppressed(lastSeenException);
                    throw retryTimeoutException;
                }
                //also reset the request to make it reusable for the next attempt
                request.reset();
            }

            CloseableHttpResponse httpResponse;
            try {
                httpResponse = client.execute(host, request);
            } catch(IOException e) {
                RequestLogger.logFailedRequest(logger, "request failed", request, host, e);
                onFailure(host);
                lastSeenException = addSuppressedException(lastSeenException, e);
                continue;
            }
            Response response = new Response(request.getRequestLine(), host, httpResponse);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode < 300 || (request.getMethod().equals(HttpHead.METHOD_NAME) && statusCode == 404) ) {
                RequestLogger.logResponse(logger, "request succeeded", request, host, httpResponse);
                onSuccess(host);
                return response;
            }
            RequestLogger.logResponse(logger, "request failed", request, host, httpResponse);
            String responseBody;
            try {
                if (response.getEntity() == null) {
                    responseBody = null;
                } else {
                    responseBody = EntityUtils.toString(response.getEntity());
                }
            } finally {
                response.close();
            }
            lastSeenException = addSuppressedException(lastSeenException, new ResponseException(response, responseBody));
            switch(statusCode) {
                case 502:
                case 503:
                case 504:
                    //mark host dead and retry against next one
                    onFailure(host);
                    break;
                default:
                    //mark host alive and don't retry, as the error should be a request problem
                    onSuccess(host);
                    throw lastSeenException;
            }
        }
        //we get here only when we tried all nodes and they all failed
        assert lastSeenException != null;
        throw lastSeenException;
    }

    private void setHeaders(HttpRequest httpRequest, Header[] requestHeaders) {
        Objects.requireNonNull(requestHeaders, "request headers must not be null");
        for (Header defaultHeader : defaultHeaders) {
            httpRequest.setHeader(defaultHeader);
        }
        for (Header requestHeader : requestHeaders) {
            Objects.requireNonNull(requestHeader, "request header must not be null");
            httpRequest.setHeader(requestHeader);
        }
    }

    /**
     * Returns an iterator of hosts to be used for a request call.
     * Ideally, the first host is retrieved from the iterator and used successfully for the request.
     * Otherwise, after each failure the next host should be retrieved from the iterator so that the request can be retried till
     * the iterator is exhausted. The maximum total of attempts is equal to the number of hosts that are available in the iterator.
     * The iterator returned will never be empty, rather an {@link IllegalStateException} in case there are no hosts.
     * In case there are no healthy hosts available, or dead ones to be be retried, one dead host gets returned.
     */
    private Iterable<HttpHost> nextHost() {
        Set<HttpHost> filteredHosts = new HashSet<>(hosts);
        for (Map.Entry<HttpHost, DeadHostState> entry : blacklist.entrySet()) {
            if (System.nanoTime() - entry.getValue().getDeadUntilNanos() < 0) {
                filteredHosts.remove(entry.getKey());
            }
        }

        if (filteredHosts.isEmpty()) {
            //last resort: if there are no good hosts to use, return a single dead one, the one that's closest to being retried
            List<Map.Entry<HttpHost, DeadHostState>> sortedHosts = new ArrayList<>(blacklist.entrySet());
            Collections.sort(sortedHosts, new Comparator<Map.Entry<HttpHost, DeadHostState>>() {
                @Override
                public int compare(Map.Entry<HttpHost, DeadHostState> o1, Map.Entry<HttpHost, DeadHostState> o2) {
                    return Long.compare(o1.getValue().getDeadUntilNanos(), o2.getValue().getDeadUntilNanos());
                }
            });
            HttpHost deadHost = sortedHosts.get(0).getKey();
            logger.trace("resurrecting host [" + deadHost + "]");
            return Collections.singleton(deadHost);
        }

        List<HttpHost> rotatedHosts = new ArrayList<>(filteredHosts);
        Collections.rotate(rotatedHosts, rotatedHosts.size() - lastHostIndex.getAndIncrement());
        return rotatedHosts;
    }

    /**
     * Called after each successful request call.
     * Receives as an argument the host that was used for the successful request.
     */
    private void onSuccess(HttpHost host) {
        DeadHostState removedHost = this.blacklist.remove(host);
        if (logger.isDebugEnabled() && removedHost != null) {
            logger.debug("removed host [" + host + "] from blacklist");
        }
    }

    /**
     * Called after each failed attempt.
     * Receives as an argument the host that was used for the failed attempt.
     */
    private void onFailure(HttpHost host) throws IOException {
        while(true) {
            DeadHostState previousDeadHostState = blacklist.putIfAbsent(host, DeadHostState.INITIAL_DEAD_STATE);
            if (previousDeadHostState == null) {
                logger.debug("added host [" + host + "] to blacklist");
                break;
            }
            if (blacklist.replace(host, previousDeadHostState, new DeadHostState(previousDeadHostState))) {
                logger.debug("updated host [" + host + "] already in blacklist");
                break;
            }
        }
        failureListener.onFailure(host);
    }

    /**
     * Sets a {@link FailureListener} to be notified each and every time a host fails
     */
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
                return addRequestBody(new HttpDeleteWithEntity(uri), entity);
            case HttpGetWithEntity.METHOD_NAME:
                return addRequestBody(new HttpGetWithEntity(uri), entity);
            case HttpHead.METHOD_NAME:
                return addRequestBody(new HttpHead(uri), entity);
            case HttpOptions.METHOD_NAME:
                return addRequestBody(new HttpOptions(uri), entity);
            case HttpPatch.METHOD_NAME:
                return addRequestBody(new HttpPatch(uri), entity);
            case HttpPost.METHOD_NAME:
                HttpPost httpPost = new HttpPost(uri);
                addRequestBody(httpPost, entity);
                return httpPost;
            case HttpPut.METHOD_NAME:
                return addRequestBody(new HttpPut(uri), entity);
            case HttpTrace.METHOD_NAME:
                return addRequestBody(new HttpTrace(uri), entity);
            default:
                throw new UnsupportedOperationException("http method not supported: " + method);
        }
    }

    private static HttpRequestBase addRequestBody(HttpRequestBase httpRequest, HttpEntity entity) {
        if (entity != null) {
            if (httpRequest instanceof HttpEntityEnclosingRequestBase) {
                ((HttpEntityEnclosingRequestBase)httpRequest).setEntity(entity);
            } else {
                throw new UnsupportedOperationException(httpRequest.getMethod() + " with body is not supported");
            }
        }
        return httpRequest;
    }

    private static URI buildUri(String path, Map<String, String> params) {
        Objects.requireNonNull(params, "params must not be null");
        try {
            URIBuilder uriBuilder = new URIBuilder(path);
            for (Map.Entry<String, String> param : params.entrySet()) {
                uriBuilder.addParameter(param.getKey(), param.getValue());
            }
            return uriBuilder.build();
        } catch(URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    /**
     * Returns a new {@link Builder} to help with {@link RestClient} creation.
     */
    public static Builder builder(HttpHost... hosts) {
        return new Builder(hosts);
    }

    /**
     * Rest client builder. Helps creating a new {@link RestClient}.
     */
    public static final class Builder {
        public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 1000;
        public static final int DEFAULT_SOCKET_TIMEOUT_MILLIS = 10000;
        public static final int DEFAULT_MAX_RETRY_TIMEOUT_MILLIS = DEFAULT_SOCKET_TIMEOUT_MILLIS;
        public static final int DEFAULT_CONNECTION_REQUEST_TIMEOUT_MILLIS = 500;

        private static final Header[] EMPTY_HEADERS = new Header[0];

        private final HttpHost[] hosts;
        private CloseableHttpClient httpClient;
        private int maxRetryTimeout = DEFAULT_MAX_RETRY_TIMEOUT_MILLIS;
        private Header[] defaultHeaders = EMPTY_HEADERS;

        /**
         * Creates a new builder instance and sets the hosts that the client will send requests to.
         */
        private Builder(HttpHost... hosts) {
            if (hosts == null || hosts.length == 0) {
                throw new IllegalArgumentException("no hosts provided");
            }
            this.hosts = hosts;
        }

        /**
         * Sets the http client. A new default one will be created if not
         * specified, by calling {@link #createDefaultHttpClient(Registry)})}.
         *
         * @see CloseableHttpClient
         */
        public Builder setHttpClient(CloseableHttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        /**
         * Sets the maximum timeout (in milliseconds) to honour in case of multiple retries of the same request.
         * {@link #DEFAULT_MAX_RETRY_TIMEOUT_MILLIS} if not specified.
         *
         * @throws IllegalArgumentException if maxRetryTimeoutMillis is not greater than 0
         */
        public Builder setMaxRetryTimeoutMillis(int maxRetryTimeoutMillis) {
            if (maxRetryTimeoutMillis <= 0) {
                throw new IllegalArgumentException("maxRetryTimeoutMillis must be greater than 0");
            }
            this.maxRetryTimeout = maxRetryTimeoutMillis;
            return this;
        }

        /**
         * Sets the default request headers, to be used when creating the default http client instance.
         * In case the http client is set through {@link #setHttpClient(CloseableHttpClient)}, the default headers need to be
         * set to it externally during http client construction.
         */
        public Builder setDefaultHeaders(Header[] defaultHeaders) {
            Objects.requireNonNull(defaultHeaders, "default headers must not be null");
            for (Header defaultHeader : defaultHeaders) {
                Objects.requireNonNull(defaultHeader, "default header must not be null");
            }
            this.defaultHeaders = defaultHeaders;
            return this;
        }

        /**
         * Creates a new {@link RestClient} based on the provided configuration.
         */
        public RestClient build() {
            if (httpClient == null) {
                httpClient = createDefaultHttpClient(null);
            }
            return new RestClient(httpClient, maxRetryTimeout, defaultHeaders, hosts);
        }

        /**
         * Creates a {@link CloseableHttpClient} with default settings. Used when the http client instance is not provided.
         *
         * @see CloseableHttpClient
         */
        public static CloseableHttpClient createDefaultHttpClient(Registry<ConnectionSocketFactory> socketFactoryRegistry) {
            PoolingHttpClientConnectionManager connectionManager;
            if (socketFactoryRegistry == null) {
                connectionManager = new PoolingHttpClientConnectionManager();
            } else {
                connectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
            }
            //default settings may be too constraining
            connectionManager.setDefaultMaxPerRoute(10);
            connectionManager.setMaxTotal(30);

            //default timeouts are all infinite
            RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_MILLIS)
                    .setSocketTimeout(DEFAULT_SOCKET_TIMEOUT_MILLIS)
                    .setConnectionRequestTimeout(DEFAULT_CONNECTION_REQUEST_TIMEOUT_MILLIS).build();
            return HttpClientBuilder.create().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig).build();
        }
    }

    /**
     * Listener that allows to be notified whenever a failure happens. Useful when sniffing is enabled, so that we can sniff on failure.
     * The default implementation is a no-op.
     */
    public static class FailureListener {
        /**
         * Notifies that the host provided as argument has just failed
         */
        public void onFailure(HttpHost host) throws IOException {

        }
    }
}
