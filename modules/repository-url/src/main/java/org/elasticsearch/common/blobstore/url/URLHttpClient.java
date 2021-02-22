/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class URLHttpClient implements Closeable {
    private final CloseableHttpClient client;
    private final HttpClientConnectionManager connectionManager;

    public URLHttpClient(CloseableHttpClient client, HttpClientConnectionManager connectionManager) {
        this.client = client;
        this.connectionManager = connectionManager;
    }

    public HttpResponse get(URI uri, Map<String, String> headers, HttpClientSettings httpClientSettings) throws IOException {
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

        final CloseableHttpResponse response = client.execute(request);

        return new HttpResponse() {
            @Override
            public InputStream getInputStream() throws IOException {
                return response.getEntity().getContent();
            }

            @Override
            public int getStatusCode() {
                return response.getStatusLine().getStatusCode();
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
        connectionManager.closeExpiredConnections();
        connectionManager.closeIdleConnections(1, TimeUnit.SECONDS);
    }

    interface HttpResponse extends Closeable {
        InputStream getInputStream() throws IOException;
        int getStatusCode();
    }
}
