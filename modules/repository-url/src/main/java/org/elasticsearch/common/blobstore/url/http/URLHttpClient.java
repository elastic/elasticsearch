/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url.http;

import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.rest.RestStatus;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class URLHttpClient implements Closeable {
    private final Logger logger = LogManager.getLogger(URLHttpClient.class);

    private final CloseableHttpClient client;
    private final URLHttpClientSettings httpClientSettings;

    public static class Factory implements Closeable {
        private final PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();

        public URLHttpClient create(URLHttpClientSettings settings) {
            final CloseableHttpClient apacheHttpClient = HttpClients.custom()
                .setSSLContext(SSLContexts.createSystemDefault())
                .setConnectionManager(connManager)
                .disableAutomaticRetries()
                .setConnectionManagerShared(true)
                .build();

            return new URLHttpClient(apacheHttpClient, settings);
        }

        @Override
        public void close() {
            connManager.close();
        }
    }

    public URLHttpClient(CloseableHttpClient client, URLHttpClientSettings httpClientSettings) {
        this.client = client;
        this.httpClientSettings = httpClientSettings;
    }

    /**
     * Executes a GET Http request against the given {@code uri}, if the response it is not
     * successful (2xx status code) it throws an {@link URLHttpClientException}. If there's
     * an IO error it throws an {@link URLHttpClientIOException}.
     */
    public HttpResponse get(URI uri, Map<String, String> headers) throws IOException {
        final HttpGet request = new HttpGet(uri);
        for (Map.Entry<String, String> headerEntry : headers.entrySet()) {
            request.setHeader(headerEntry.getKey(), headerEntry.getValue());
        }

        final RequestConfig requestConfig = RequestConfig.custom()
            .setSocketTimeout(httpClientSettings.getSocketTimeoutMs())
            .setConnectionRequestTimeout(httpClientSettings.getConnectionPoolTimeoutMs())
            .setConnectTimeout(httpClientSettings.getConnectionTimeoutMs())
            .build();
        request.setConfig(requestConfig);

        try {
            return executeRequest(request);
        } catch (IOException e) {
            throw new URLHttpClientIOException(e.getMessage(), e);
        }
    }

    private HttpResponse executeRequest(HttpGet request) throws IOException {
        final CloseableHttpResponse response = client.execute(request);

        final int statusCode = response.getStatusLine().getStatusCode();

        if (isSuccessful(statusCode) == false) {
            if (response.getEntity() != null) {
                try {
                    IOUtils.closeWhileHandlingException(response.getEntity().getContent());
                } catch (IOException e) {
                    logger.warn("Unable to release HTTP body", e);
                }
            }
            IOUtils.closeWhileHandlingException(response);

            throw new URLHttpClientException(statusCode);
        }

        return new HttpResponse() {
            @Override
            public HttpResponseInputStream getInputStream() throws IOException {
                try {
                    return new HttpResponseInputStream(request, response);
                } catch (IOException e) {
                    // Release the underlying connection in case of failure
                    IOUtils.closeWhileHandlingException(response);
                    throw e;
                }
            }

            @Override
            public int getStatusCode() {
                return statusCode;
            }

            @Override
            public String getHeader(String headerName) {
                final Header header = response.getFirstHeader(headerName);
                return header == null ? null : header.getValue();
            }

            @Override
            public void close() throws IOException {
                response.close();
            }
        };
    }

    private boolean isSuccessful(int statusCode) {
        return statusCode / 100 == RestStatus.OK.getStatus() / 100;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    interface HttpResponse extends Closeable {
        HttpResponseInputStream getInputStream() throws IOException;

        int getStatusCode();

        @Nullable
        String getHeader(String headerName);
    }
}
