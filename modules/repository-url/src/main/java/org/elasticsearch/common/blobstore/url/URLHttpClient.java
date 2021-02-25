/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url;

import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.common.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class URLHttpClient implements Closeable {
    private final CloseableHttpClient client;
    private final URLHttpClientSettings httpClientSettings;

    public static class Factory implements Closeable {
        private final PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();

        public URLHttpClient create(URLHttpClientSettings settings) {
            final CloseableHttpClient apacheHttpClient = HttpClients.custom()
                .setSSLContext(SSLContexts.createSystemDefault())
                .setConnectionManager(connManager)
                .disableAutomaticRetries()
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
            throw new URLHttpClientException("Unable to execute HTTP request: " + e.getMessage(), e);
        }
    }

    private HttpResponse executeRequest(HttpGet request) throws IOException {
        final CloseableHttpResponse response = client.execute(request);

        return new HttpResponse() {
            @Override
            public HttpResponseInputStream getInputStream() throws IOException {
                return new HttpResponseInputStream(request, response);
            }

            @Override
            public int getStatusCode() {
                return response.getStatusLine().getStatusCode();
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
