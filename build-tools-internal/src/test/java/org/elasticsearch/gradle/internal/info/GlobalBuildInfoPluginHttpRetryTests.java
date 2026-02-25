/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.info;

import com.sun.net.httpserver.HttpServer;

import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

public class GlobalBuildInfoPluginHttpRetryTests {

    @Test
    public void testRetriesOnceAndEventuallySucceeds() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        byte[] body = "ok".getBytes(StandardCharsets.UTF_8);

        HttpServer server = startServer(exchange -> {
            int request = requests.getAndIncrement();
            if (request == 0) {
                exchange.sendResponseHeaders(500, 0);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write("fail".getBytes(StandardCharsets.UTF_8));
                }
            } else {
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            }
            exchange.close();
        });

        try {
            String host = server.getAddress().getAddress().getHostAddress();
            String url = new URI("http", null, host, server.getAddress().getPort(), "/branches.json", null, null).toString();
            List<Long> backoffs = new ArrayList<>();
            GlobalBuildInfoPlugin.Sleeper sleeper = backoffs::add;

            byte[] bytes = GlobalBuildInfoPlugin.readHttpBytesWithRetry(url, 3, 10, sleeper);

            assertThat(bytes, equalTo(body));
            assertThat(requests.get(), equalTo(2));
            assertThat(backoffs, equalTo(List.of(10L)));
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void testExhaustsRetries() throws Exception {
        AtomicInteger requests = new AtomicInteger();

        HttpServer server = startServer(exchange -> {
            requests.incrementAndGet();
            exchange.sendResponseHeaders(500, 0);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write("fail".getBytes(StandardCharsets.UTF_8));
            }
            exchange.close();
        });

        try {
            String host = server.getAddress().getAddress().getHostAddress();
            String url = new URI("http", null, host, server.getAddress().getPort(), "/branches.json", null, null).toString();
            List<Long> backoffs = new ArrayList<>();
            GlobalBuildInfoPlugin.Sleeper sleeper = backoffs::add;

            assertThrows(IOException.class, () -> GlobalBuildInfoPlugin.readHttpBytesWithRetry(url, 3, 10, sleeper));

            assertThat(requests.get(), equalTo(3));
            assertThat(backoffs, equalTo(List.of(10L, 20L)));
        } finally {
            server.stop(0);
        }
    }

    private static HttpServer startServer(com.sun.net.httpserver.HttpHandler handler) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/branches.json", handler);
        server.start();
        return server;
    }
}

