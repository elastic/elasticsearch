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
package org.elasticsearch.repositories.gcs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.repositories.gcs.GoogleCloudStorageTestServer.Response;
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
 * {@link GoogleCloudStorageFixture} is a fixture that emulates a Google Cloud Storage service.
 * <p>
 * It starts an asynchronous socket server that binds to a random local port. The server parses
 * HTTP requests and uses a {@link GoogleCloudStorageTestServer} to handle them before returning
 * them to the client as HTTP responses.
 */
public class GoogleCloudStorageFixture {

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException("GoogleCloudStorageFixture <working directory> <bucket>");
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

            // Emulates a Google Cloud Storage server
            final String storageUrl = "http://" + addressAndPort;
            final GoogleCloudStorageTestServer storageTestServer = new GoogleCloudStorageTestServer(storageUrl);
            storageTestServer.createBucket(args[1]);

            httpServer.createContext("/", new ResponseHandler(storageTestServer));
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

        private final GoogleCloudStorageTestServer storageServer;

        private ResponseHandler(final GoogleCloudStorageTestServer storageServer) {
            this.storageServer = storageServer;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            String path = storageServer.getEndpoint() + exchange.getRequestURI().getRawPath();
            String query = exchange.getRequestURI().getRawQuery();
            Map<String, List<String>> headers = exchange.getRequestHeaders();

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(exchange.getRequestBody(), out);

            Response storageResponse = null;

            final String userAgent = exchange.getRequestHeaders().getFirst("User-Agent");
            if (userAgent != null && userAgent.startsWith("Apache Ant")) {
                // This is a request made by the AntFixture, just reply "OK"
                storageResponse = new Response(RestStatus.OK, emptyMap(), "text/plain; charset=utf-8", "OK".getBytes(UTF_8));
            } else {
                // Otherwise simulate a S3 response
                storageResponse = storageServer.handle(method, path, query, headers, out.toByteArray());
            }

            Map<String, List<String>> responseHeaders = exchange.getResponseHeaders();
            responseHeaders.put("Content-Type", singletonList(storageResponse.contentType));
            storageResponse.headers.forEach((k, v) -> responseHeaders.put(k, singletonList(v)));
            exchange.sendResponseHeaders(storageResponse.status.getStatus(), storageResponse.body.length);
            if (storageResponse.body.length > 0) {
                exchange.getResponseBody().write(storageResponse.body);
            }
            exchange.close();
        }
    }
}
