/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.repositories.azure;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.repositories.azure.AzureStorageTestServer.Response;
import org.elasticsearch.rest.RestStatus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

/**
 * {@link AzureStorageFixture} is a fixture that emulates an Azure Storage service.
 * <p>
 * It starts an asynchronous socket server that binds to a random local port. The server parses
 * HTTP requests and uses a {@link AzureStorageTestServer} to handle them before returning
 * them to the client as HTTP responses.
 */
public class AzureStorageFixture {

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException("AzureStorageFixture <working directory> <container>");
        }

        final InetSocketAddress socketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        final HttpServer httpServer = MockHttpServer.createHttp(socketAddress, 0);

        try {
            final Path workingDirectory = workingDir(args[0]);
            /// Writes the PID of the current Java process in a `pid` file located in the working directory
            writeFile(workingDirectory, "pid", ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);

            final String addressAndPort = addressToString(httpServer.getAddress());
            // Writes the address and port of the http server in a `ports` file located in the working directory
            writeFile(workingDirectory, "ports", addressAndPort);

            // Emulates Azure
            final String storageUrl = "http://" + addressAndPort;
            final AzureStorageTestServer testServer = new AzureStorageTestServer(storageUrl);
            testServer.createContainer(args[1]);

            httpServer.createContext("/", new ResponseHandler(testServer));
            httpServer.start();

            // Wait to be killed
            Thread.sleep(Long.MAX_VALUE);

        } finally {
            httpServer.stop(0);
        }
    }

    @SuppressForbidden(reason = "Paths#get is fine - we don't have environment here")
    private static Path workingDir(final String dir) {
        return Paths.get(dir);
    }

    private static void writeFile(final Path dir, final String fileName, final String content) throws IOException {
        final Path tempPidFile = Files.createTempFile(dir, null, null);
        Files.write(tempPidFile, singleton(content));
        Files.move(tempPidFile, dir.resolve(fileName), StandardCopyOption.ATOMIC_MOVE);
    }

    private static String addressToString(final SocketAddress address) {
        final InetSocketAddress inetSocketAddress = (InetSocketAddress) address;
        if (inetSocketAddress.getAddress() instanceof Inet6Address) {
            return "[" + inetSocketAddress.getHostString() + "]:" + inetSocketAddress.getPort();
        } else {
            return inetSocketAddress.getHostString() + ":" + inetSocketAddress.getPort();
        }
    }

    static class ResponseHandler implements HttpHandler {

        private final AzureStorageTestServer server;

        private ResponseHandler(final AzureStorageTestServer server) {
            this.server = server;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            String path = server.getEndpoint() + exchange.getRequestURI().getRawPath();
            String query = exchange.getRequestURI().getRawQuery();
            Map<String, List<String>> headers = exchange.getRequestHeaders();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(exchange.getRequestBody(), out);

            Response response = null;

            final String userAgent = exchange.getRequestHeaders().getFirst("User-Agent");
            if (userAgent != null && userAgent.startsWith("Apache Ant")) {
                // This is a request made by the AntFixture, just reply "OK"
                response = new Response(RestStatus.OK, emptyMap(), "text/plain; charset=utf-8", "OK".getBytes(UTF_8));
            } else {
                // Otherwise simulate a S3 response
                response = server.handle(method, path, query, headers, out.toByteArray());
            }

            Map<String, List<String>> responseHeaders = exchange.getResponseHeaders();
            responseHeaders.put("Content-Type", singletonList(response.contentType));
            response.headers.forEach((k, v) -> responseHeaders.put(k, singletonList(v)));
            exchange.sendResponseHeaders(response.status.getStatus(), response.body.length);
            if (response.body.length > 0) {
                exchange.getResponseBody().write(response.body);
            }
            exchange.close();
        }
    }
}
