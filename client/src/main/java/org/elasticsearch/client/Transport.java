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
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

final class Transport<C extends Connection> implements Closeable {

    private static final Log logger = LogFactory.getLog(Transport.class);

    private final CloseableHttpClient client;
    private final ConnectionPool<C> connectionPool;
    private final long maxRetryTimeout;

    Transport(CloseableHttpClient client, ConnectionPool<C> connectionPool, long maxRetryTimeout) {
        Objects.requireNonNull(client, "client cannot be null");
        Objects.requireNonNull(connectionPool, "connectionPool cannot be null");
        if (maxRetryTimeout <= 0) {
            throw new IllegalArgumentException("maxRetryTimeout must be greater than 0");
        }
        this.client = client;
        this.connectionPool = connectionPool;
        this.maxRetryTimeout = maxRetryTimeout;
    }

    ElasticsearchResponse performRequest(Verb verb, String endpoint, Map<String, Object> params, HttpEntity entity) throws IOException {
        URI uri = buildUri(endpoint, params);
        HttpRequestBase request = createHttpRequest(verb, uri, entity);
        Iterator<C> connectionIterator = connectionPool.nextConnection().iterator();
        if (connectionIterator.hasNext() == false) {
            C connection = connectionPool.lastResortConnection();
            logger.info("no healthy nodes available, trying " + connection.getNode());
            return performRequest(request, Stream.of(connection).iterator());
        }
        return performRequest(request, connectionIterator);
    }

    private ElasticsearchResponse performRequest(HttpRequestBase request, Iterator<C> connectionIterator) throws IOException {
        //we apply a soft margin so that e.g. if a request took 59 seconds and timeout is set to 60 we don't do another attempt
        long retryTimeout = Math.round(this.maxRetryTimeout / (float)100 * 98);
        IOException lastSeenException = null;
        long startTime = System.nanoTime();

        while (connectionIterator.hasNext()) {
            C connection = connectionIterator.next();

            if (lastSeenException != null) {
                long timeElapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                long timeout = retryTimeout - timeElapsed;
                if (timeout <= 0) {
                    RetryTimeoutException retryTimeoutException = new RetryTimeoutException(
                            "request retries exceeded max retry timeout [" + retryTimeout + "]");
                    retryTimeoutException.addSuppressed(lastSeenException);
                    throw retryTimeoutException;
                }
            }

            try {
                connectionPool.beforeAttempt(connection);
            } catch(IOException e) {
                lastSeenException = addSuppressedException(lastSeenException, e);
                continue;
            }

            try {
                ElasticsearchResponse response = performRequest(request, connection);
                connectionPool.onSuccess(connection);
                return response;
            } catch(ElasticsearchResponseException e) {
                if (e.isRecoverable()) {
                    connectionPool.onFailure(connection);
                    lastSeenException = addSuppressedException(lastSeenException, e);
                } else {
                    //don't retry and call onSuccess as the error should be a request problem
                    connectionPool.onSuccess(connection);
                    throw e;
                }
            } catch(IOException e) {
                connectionPool.onFailure(connection);
                lastSeenException = addSuppressedException(lastSeenException, e);
            }
        }
        assert lastSeenException != null;
        throw lastSeenException;
    }

    private ElasticsearchResponse performRequest(HttpRequestBase request, C connection) throws IOException {
        CloseableHttpResponse response;
        try {
            response = client.execute(connection.getNode().getHttpHost(), request);
        } catch(IOException e) {
            RequestLogger.log(logger, "request failed", request.getRequestLine(), connection.getNode(), e);
            throw e;
        } finally {
            request.reset();
        }
        StatusLine statusLine = response.getStatusLine();
        //TODO make ignore status code configurable. rest-spec and tests support that parameter.
        if (statusLine.getStatusCode() < 300 ||
                request.getMethod().equals(HttpHead.METHOD_NAME) && statusLine.getStatusCode() == 404) {
            RequestLogger.log(logger, "request succeeded", request.getRequestLine(), connection.getNode(), response.getStatusLine());
            return new ElasticsearchResponse(request.getRequestLine(), connection.getNode(), response);
        } else {
            EntityUtils.consume(response.getEntity());
            RequestLogger.log(logger, "request failed", request.getRequestLine(), connection.getNode(), response.getStatusLine());
            throw new ElasticsearchResponseException(request.getRequestLine(), connection.getNode(), statusLine);
        }
    }

    private static IOException addSuppressedException(IOException suppressedException, IOException currentException) {
        if (suppressedException != null) {
            currentException.addSuppressed(suppressedException);
        }
        return currentException;
    }

    private static HttpRequestBase createHttpRequest(Verb verb, URI uri, HttpEntity entity) {
        switch(verb) {
            case DELETE:
                HttpDeleteWithEntity httpDeleteWithEntity = new HttpDeleteWithEntity(uri);
                addRequestBody(httpDeleteWithEntity, entity);
                return httpDeleteWithEntity;
            case GET:
                HttpGetWithEntity httpGetWithEntity = new HttpGetWithEntity(uri);
                addRequestBody(httpGetWithEntity, entity);
                return httpGetWithEntity;
            case HEAD:
                if (entity != null) {
                    throw new UnsupportedOperationException("HEAD with body is not supported");
                }
                return new HttpHead(uri);
            case POST:
                HttpPost httpPost = new HttpPost(uri);
                addRequestBody(httpPost, entity);
                return httpPost;
            case PUT:
                HttpPut httpPut = new HttpPut(uri);
                addRequestBody(httpPut, entity);
                return httpPut;
            default:
                throw new UnsupportedOperationException("http method not supported: " + verb);
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
}
