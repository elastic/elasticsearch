/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.equalTo;

@SuppressForbidden(reason = "uses httpserver by design")
public class HttpClientTests extends ESTestCase {

    private static HttpServer server;

    @BeforeClass
    public static void startServer() throws Throwable {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/hello/", exchange -> {
            try {
                String response = "hello world";
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            } catch (Exception e) {
                fail(e);
            }
        });
        server.createContext("/404/", exchange -> {
            try {
                exchange.sendResponseHeaders(404, 0);
            } catch (Exception e) {
                fail(e);
            }
        });
        server.start();
    }

    @AfterClass
    public static void stopServer() {
        server.stop(0);
    }

    private static String url(final String path) {
        String hostname = server.getAddress().getHostString();
        int port = server.getAddress().getPort();
        return "http://" + hostname + ":" + port + path;
    }

    private static String bytesToString(final byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public void testGetBytes() throws Exception {
        HttpClient client = new HttpClient();
        String response = bytesToString(client.getBytes(url("/hello/")));
        assertThat(response, equalTo("hello world"));
    }

    public void testGetBytes404() throws Exception {
        HttpClient client = new HttpClient();
        Exception e = expectThrows(ResourceNotFoundException.class, () -> client.getBytes(url("/404/")));
        assertThat(e.getMessage(), equalTo(url("/404/") + " not found"));
    }
}
