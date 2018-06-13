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
package org.elasticsearch.repositories.url;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;

/**
 * This {@link URLFixture} exposes a filesystem directory over HTTP. It is used in repository-url
 * integration tests to expose a directory created by a regular FS repository.
 */
public class URLFixture {

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException("URLFixture <working directory> <repository directory>");
        }

        final InetSocketAddress socketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        final HttpServer httpServer = MockHttpServer.createHttp(socketAddress, 0);

        try {
            final Path workingDirectory = dir(args[0]);
            /// Writes the PID of the current Java process in a `pid` file located in the working directory
            writeFile(workingDirectory, "pid", ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);

            final String addressAndPort = addressToString(httpServer.getAddress());
            // Writes the address and port of the http server in a `ports` file located in the working directory
            writeFile(workingDirectory, "ports", addressAndPort);

            // Exposes the repository over HTTP
            httpServer.createContext("/", new ResponseHandler(dir(args[1])));
            httpServer.start();

            // Wait to be killed
            Thread.sleep(Long.MAX_VALUE);

        } finally {
            httpServer.stop(0);
        }
    }

    @SuppressForbidden(reason = "Paths#get is fine - we don't have environment here")
    private static Path dir(final String dir) {
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

        private final Path repositoryDir;

        ResponseHandler(final Path repositoryDir) {
            this.repositoryDir = repositoryDir;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Response response;

            final String userAgent = exchange.getRequestHeaders().getFirst("User-Agent");
            if (userAgent != null && userAgent.startsWith("Apache Ant")) {
                // This is a request made by the AntFixture, just reply "OK"
                response = new Response(RestStatus.OK, emptyMap(), "text/plain; charset=utf-8", "OK".getBytes(UTF_8));

            } else if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                String path = exchange.getRequestURI().toString();
                if (path.length() > 0 && path.charAt(0) == '/') {
                    path = path.substring(1);
                }

                Path normalizedRepositoryDir = repositoryDir.normalize();
                Path normalizedPath = normalizedRepositoryDir.resolve(path).normalize();

                if (normalizedPath.startsWith(normalizedRepositoryDir)) {
                    if (Files.exists(normalizedPath) && Files.isReadable(normalizedPath) && Files.isRegularFile(normalizedPath)) {
                        byte[] content = Files.readAllBytes(normalizedPath);
                        Map<String, String> headers = singletonMap("Content-Length", String.valueOf(content.length));
                        response = new Response(RestStatus.OK, headers, "application/octet-stream", content);
                    } else {
                        response = new Response(RestStatus.NOT_FOUND, emptyMap(), "text/plain; charset=utf-8", new byte[0]);
                    }
                } else {
                    response = new Response(RestStatus.FORBIDDEN, emptyMap(), "text/plain; charset=utf-8", new byte[0]);
                }
            } else {
                response = new Response(RestStatus.INTERNAL_SERVER_ERROR, emptyMap(), "text/plain; charset=utf-8",
                    "Unsupported HTTP method".getBytes(StandardCharsets.UTF_8));
            }
            exchange.sendResponseHeaders(response.status.getStatus(), response.body.length);
            if (response.body.length > 0) {
                exchange.getResponseBody().write(response.body);
            }
            exchange.close();
        }
    }

    /**
     * Represents a HTTP Response.
     */
    static class Response {

        final RestStatus status;
        final Map<String, String> headers;
        final String contentType;
        final byte[] body;

        Response(final RestStatus status, final Map<String, String> headers, final String contentType, final byte[] body) {
            this.status = Objects.requireNonNull(status);
            this.headers = Objects.requireNonNull(headers);
            this.contentType = Objects.requireNonNull(contentType);
            this.body = Objects.requireNonNull(body);
        }
    }
}
