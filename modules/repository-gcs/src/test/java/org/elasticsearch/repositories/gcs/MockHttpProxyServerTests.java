/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.gcs;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;

public class MockHttpProxyServerTests extends ESTestCase {

    public void testProxyServerWorks() throws Exception {
        String httpBody = randomAlphaOfLength(32);
        var proxyServer = new MockHttpProxyServer() {
            @Override
            public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                assertEquals("GET http://googleapis.com/ HTTP/1.1", request.getRequestLine().toString());
                response.setEntity(new StringEntity(httpBody, ContentType.TEXT_PLAIN));
            }
        };
        var httpClient = HttpClients.custom()
            .setRoutePlanner(new DefaultProxyRoutePlanner(new HttpHost(InetAddress.getLoopbackAddress(), proxyServer.getPort())))
            .build();
        try (
            proxyServer;
            httpClient;
            var httpResponse = SocketAccess.doPrivilegedIOException(() -> httpClient.execute(new HttpGet("http://googleapis.com/")))
        ) {
            assertEquals(httpBody.length(), httpResponse.getEntity().getContentLength());
            assertEquals(httpBody, EntityUtils.toString(httpResponse.getEntity()));
        }
    }
}
