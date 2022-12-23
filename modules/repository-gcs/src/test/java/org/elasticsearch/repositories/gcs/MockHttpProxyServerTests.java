/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.gcs;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;

public class MockHttpProxyServerTests extends ESTestCase {

    public void testProxyServerWorks() throws Exception {
        String httpBody = randomAlphaOfLength(32);
        var proxyServer = new MockHttpProxyServer((is, os) -> {
            try (
                var reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
                var writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)
            ) {
                assertEquals("GET http://googleapis.com/ HTTP/1.1", reader.readLine());
                writer.write(formatted("""
                    HTTP/1.1 200 OK\r
                    Content-Length: %s\r
                    \r
                    %s""", httpBody.length(), httpBody));
            }
        }).await();
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
