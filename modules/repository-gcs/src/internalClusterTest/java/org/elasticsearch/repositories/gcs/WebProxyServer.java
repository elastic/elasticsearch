/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.gcs;

import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Emulates a <a href="https://en.wikipedia.org/wiki/Proxy_server#Web_proxy_servers">Web Proxy Server</a>
 */
class WebProxyServer extends MockHttpProxyServer {

    private static final Set<String> BLOCKED_HEADERS = Stream.of(
        "Host",
        "Proxy-Connection",
        "Proxy-Authenticate",
        "Content-Length",
        "Transfer-Encoding"
    ).collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));

    private final CloseableHttpClient httpClient = HttpClients.createDefault();

    @Override
    public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws IOException {
        var upstreamRequest = new HttpEntityEnclosingRequestBase() {
            @Override
            public String getMethod() {
                return request.getRequestLine().getMethod();
            }
        };
        upstreamRequest.setURI(URI.create(request.getRequestLine().getUri()));
        upstreamRequest.setHeader("X-Via", "test-web-proxy-server");
        for (Header requestHeader : request.getAllHeaders()) {
            String headerName = requestHeader.getName();
            String headerValue = requestHeader.getValue();
            if (BLOCKED_HEADERS.contains(headerName) == false) {
                upstreamRequest.setHeader(headerName, headerValue);
            }
        }
        if (request instanceof HttpEntityEnclosingRequest entityRequest) {
            upstreamRequest.setEntity(
                new ByteArrayEntity(EntityUtils.toByteArray(entityRequest.getEntity()), ContentType.get(entityRequest.getEntity()))
            );
        }
        try (CloseableHttpResponse upstreamResponse = httpClient.execute(upstreamRequest)) {
            response.setStatusLine(upstreamResponse.getStatusLine());
            for (Header upstreamHeader : upstreamResponse.getAllHeaders()) {
                String name = upstreamHeader.getName();
                if (BLOCKED_HEADERS.contains(name) == false) {
                    response.addHeader(name, upstreamHeader.getValue());
                }
            }
            if (upstreamResponse.getEntity() != null) {
                response.setEntity(
                    new ByteArrayEntity(
                        EntityUtils.toByteArray(upstreamResponse.getEntity()),
                        ContentType.get(upstreamResponse.getEntity())
                    )
                );
            }
        }
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
        super.close();
    }

}
