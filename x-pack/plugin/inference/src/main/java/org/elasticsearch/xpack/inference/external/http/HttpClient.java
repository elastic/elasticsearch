/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Streams;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.inference.common.SizeLimitInputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.MAX_HTTP_RESPONSE_SIZE;

public class HttpClient {
    // TODO pick a reasonable value for this
    private static final int MAX_CONNECTIONS = 500;
    private static final Logger logger = LogManager.getLogger(HttpClient.class);
    private final ByteSizeValue maxResponseSize;
    private final CloseableHttpClient client;

    // TODO create a single apache http client because it could be expensive
    // TODO should proxy settings be set on a client basis? aka not per request
    // TODO this should take some sort of request

    public HttpClient(Settings settings) {
        this.maxResponseSize = MAX_HTTP_RESPONSE_SIZE.get(settings);
        this.client = createClient();
    }

    private CloseableHttpClient createClient() {
        HttpClientBuilder clientBuilder = HttpClientBuilder.create();

        // The apache client will be shared across all connections because it can be expensive to create it
        // so we don't want to support cookies to avoid accidental authentication for unauthorized users
        clientBuilder.disableCookieManagement();
        clientBuilder.evictExpiredConnections();
        clientBuilder.setMaxConnPerRoute(MAX_CONNECTIONS);
        clientBuilder.setMaxConnTotal(MAX_CONNECTIONS);

        return clientBuilder.build();
    }

    private CloseableHttpClient createAsyncClient() {
        HttpClientBuilder clientBuilder = HttpClientBuilder.create();

        // The apache client will be shared across all connections because it can be expensive to create it
        // so we don't want to support cookies to avoid accidental authentication for unauthorized users
        clientBuilder.disableCookieManagement();
        clientBuilder.evictExpiredConnections();
        clientBuilder.setMaxConnPerRoute(MAX_CONNECTIONS);
        clientBuilder.setMaxConnTotal(MAX_CONNECTIONS);

        return clientBuilder.build();
    }

    public byte[] send(HttpUriRequest request) throws IOException {
        try (CloseableHttpResponse response = SocketAccess.doPrivileged(() -> client.execute(request))) {
            return copyBody(response);
            // HttpEntity entity = response.getEntity();
            // if (entity != null) {
            // // return it as a String
            // String result = EntityUtils.toString(entity);
            // logger.info(format("Request response: %s", result));
            // }
            //
            // EntityUtils.consume(entity);
        }
    }

    private byte[] copyBody(HttpResponse response) throws IOException {
        final byte[] body;
        if (response.getEntity() == null) {
            body = new byte[0];
        } else {
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                try (InputStream is = new SizeLimitInputStream(maxResponseSize, response.getEntity().getContent())) {
                    Streams.copy(is, outputStream);
                }
                body = outputStream.toByteArray();
            }
        }

        return body;
    }

    public void sendAsync(HttpUriRequest request) throws IOException {
        try (CloseableHttpResponse response = SocketAccess.doPrivileged(() -> client.execute(request))) {
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                // return it as a String
                String result = EntityUtils.toString(entity);
                logger.info(format("Request response: %s", result));
            }
            EntityUtils.consume(entity);
        }
    }
}
