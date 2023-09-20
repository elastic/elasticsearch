/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.Header;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Streams;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.inference.common.SizeLimitInputStream;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;

import static org.elasticsearch.core.Strings.format;

public class HttpClient implements Closeable {

    private final ByteSizeValue maxResponseSize;
    private final int maxConnections;
    private final CloseableHttpAsyncClient client;

    public HttpClient(Settings settings) {
        this.maxResponseSize = HttpSettings.MAX_HTTP_RESPONSE_SIZE.get(settings);
        this.maxConnections = HttpSettings.MAX_CONNECTIONS.get(settings);
        this.client = createAsyncClient();
    }

    private CloseableHttpAsyncClient createAsyncClient() {
        HttpAsyncClientBuilder clientBuilder = HttpAsyncClientBuilder.create();

        // The apache client will be shared across all connections because it can be expensive to create it
        // so we don't want to support cookies to avoid accidental authentication for unauthorized users
        clientBuilder.disableCookieManagement();
        clientBuilder.setMaxConnPerRoute(maxConnections);
        clientBuilder.setMaxConnTotal(maxConnections);

        return clientBuilder.build();
    }

    public void send(HttpUriRequest request, ActionListener<HttpResponse> listener) throws IOException {
        if (client.isRunning() == false) {
            client.start();
        }

        SocketAccess.doPrivileged(() -> client.execute(request, new FutureCallback<>() {
            @Override
            public void completed(org.apache.http.HttpResponse result) {
                try {
                    listener.onResponse(new HttpResponse(headers(result), body(result)));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void failed(Exception ex) {
                listener.onFailure(ex);
            }

            @Override
            public void cancelled() {
                listener.onFailure(new CancellationException(format("Request [%s] was cancelled", request.getRequestLine())));
            }
        }));
    }

    private byte[] body(org.apache.http.HttpResponse response) throws IOException {
        if (response.getEntity() == null) {
            return new byte[0];
        }

        final byte[] body;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            try (InputStream is = new SizeLimitInputStream(maxResponseSize, response.getEntity().getContent())) {
                Streams.copy(is, outputStream);
            }
            body = outputStream.toByteArray();
        }

        return body;
    }

    private Map<String, List<String>> headers(org.apache.http.HttpResponse response) {
        Header[] headers = response.getAllHeaders();
        Map<String, List<String>> responseHeaders = Maps.newMapWithExpectedSize(headers.length);

        for (Header header : headers) {
            if (responseHeaders.containsKey(header.getName())) {
                List<String> headerValues = responseHeaders.get(header.getName());
                headerValues.add(header.getValue());
            } else {
                ArrayList<String> values = new ArrayList<>();
                values.add(header.getValue());
                responseHeaders.put(header.getName(), values);
            }
        }

        return responseHeaders;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
