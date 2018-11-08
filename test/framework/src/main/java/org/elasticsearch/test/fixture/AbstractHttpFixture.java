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

package org.elasticsearch.test.fixture;

import com.sun.net.httpserver.HttpServer;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

/**
 * Base class for test fixtures that requires a {@link HttpServer} to work.
 */
@SuppressForbidden(reason = "uses httpserver by design")
public abstract class AbstractHttpFixture {

    protected static final Map<String, String> TEXT_PLAIN_CONTENT_TYPE = contentType("text/plain; charset=utf-8");
    protected static final Map<String, String> JSON_CONTENT_TYPE = contentType("application/json; charset=utf-8");

    protected static final byte[] EMPTY_BYTE = new byte[0];

    /** Increments for the requests ids **/
    private final AtomicLong requests = new AtomicLong(0);

    /** Current working directory of the fixture **/
    private final Path workingDirectory;

    protected AbstractHttpFixture(final String workingDir) {
        this.workingDirectory = PathUtils.get(Objects.requireNonNull(workingDir));
    }

    /**
     * Opens a {@link HttpServer} and start listening on a random port.
     */
    public final void listen() throws IOException, InterruptedException {
        final InetSocketAddress socketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        final HttpServer httpServer = HttpServer.create(socketAddress, 0);

        try {
            /// Writes the PID of the current Java process in a `pid` file located in the working directory
            writeFile(workingDirectory, "pid", ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);

            final String addressAndPort = addressToString(httpServer.getAddress());
            // Writes the address and port of the http server in a `ports` file located in the working directory
            writeFile(workingDirectory, "ports", addressAndPort);

            httpServer.createContext("/", exchange -> {
                try {
                    Response response;

                    // Check if this is a request made by the AntFixture
                    final String userAgent = exchange.getRequestHeaders().getFirst("User-Agent");
                    if (userAgent != null
                        && userAgent.startsWith("Apache Ant")
                        && "GET".equals(exchange.getRequestMethod())
                        && "/".equals(exchange.getRequestURI().getPath())) {
                        response = new Response(200, TEXT_PLAIN_CONTENT_TYPE, "OK".getBytes(UTF_8));

                    } else {
                        try {
                            final long requestId = requests.getAndIncrement();
                            final String method = exchange.getRequestMethod();


                            final Map<String, String> headers = new HashMap<>();
                            for (Map.Entry<String, List<String>> header : exchange.getRequestHeaders().entrySet()) {
                                headers.put(header.getKey(), exchange.getRequestHeaders().getFirst(header.getKey()));
                            }

                            final ByteArrayOutputStream body = new ByteArrayOutputStream();
                            try (InputStream requestBody = exchange.getRequestBody()) {
                                final byte[] buffer = new byte[1024];
                                int i;
                                while ((i = requestBody.read(buffer, 0, buffer.length)) != -1) {
                                    body.write(buffer, 0, i);
                                }
                                body.flush();
                            }

                            final Request request = new Request(requestId, method, exchange.getRequestURI(), headers, body.toByteArray());
                            response = handle(request);

                        } catch (Exception e) {
                            final String error = e.getMessage() != null ? e.getMessage() : "Exception when processing the request";
                            response = new Response(500, singletonMap("Content-Type", "text/plain; charset=utf-8"), error.getBytes(UTF_8));
                        }
                    }

                    if (response == null) {
                        response = new Response(400, TEXT_PLAIN_CONTENT_TYPE, EMPTY_BYTE);
                    }

                    response.headers.forEach((k, v) -> exchange.getResponseHeaders().put(k, singletonList(v)));
                    if (response.body.length > 0) {
                        exchange.sendResponseHeaders(response.status, response.body.length);
                        exchange.getResponseBody().write(response.body);
                    } else {
                        exchange.sendResponseHeaders(response.status, -1);
                    }
                } finally {
                    exchange.close();
                }
            });
            httpServer.start();

            // Wait to be killed
            Thread.sleep(Long.MAX_VALUE);

        } finally {
            httpServer.stop(0);
        }
    }

    protected abstract Response handle(Request request) throws IOException;

    @FunctionalInterface
    public interface RequestHandler {
        Response handle(Request request) throws IOException;
    }

    /**
     * Represents an HTTP Response.
     */
    protected static class Response {

        private final int status;
        private final Map<String, String> headers;
        private final byte[] body;

        public Response(final int status, final Map<String, String> headers, final byte[] body) {
            this.status = status;
            this.headers = Objects.requireNonNull(headers);
            this.body = Objects.requireNonNull(body);
        }

        public int getStatus() {
            return status;
        }

        public Map<String, String> getHeaders() {
            return headers;
        }

        public byte[] getBody() {
            return body;
        }

        public String getContentType() {
            for (String header : headers.keySet()) {
                if (header.equalsIgnoreCase("Content-Type")) {
                    return headers.get(header);
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return "Response{" +
                "status=" + status +
                ", headers=" + headers +
                ", body=" + new String(body, UTF_8) +
                '}';
        }
    }

    /**
     * Represents an HTTP Request.
     */
    protected static class Request {

        private final long id;
        private final String method;
        private final URI uri;
        private final Map<String, String> parameters;
        private final Map<String, String> headers;
        private final byte[] body;

        public Request(final long id, final String method, final URI uri, final Map<String, String> headers, final byte[] body) {
            this.id = id;
            this.method = Objects.requireNonNull(method);
            this.uri = Objects.requireNonNull(uri);
            this.headers =  Objects.requireNonNull(headers);
            this.body =  Objects.requireNonNull(body);

            final Map<String, String> params = new HashMap<>();
            if (uri.getQuery() != null && uri.getQuery().length() > 0) {
                for (String param : uri.getQuery().split("&")) {
                    int i = param.indexOf("=");
                    if (i > 0) {
                        params.put(param.substring(0, i), param.substring(i + 1));
                    } else {
                        params.put(param, "");
                    }
                }
            }
            this.parameters = params;
        }

        public long getId() {
            return id;
        }

        public String getMethod() {
            return method;
        }

        public Map<String, String> getHeaders() {
            return headers;
        }

        public String getHeader(final String headerName) {
            for (String header : headers.keySet()) {
                if (header.equalsIgnoreCase(headerName)) {
                    return headers.get(header);
                }
            }
            return null;
        }

        public byte[] getBody() {
            return body;
        }

        public String getPath() {
            return uri.getRawPath();
        }

        public Map<String, String> getParameters() {
            return parameters;
        }

        public String getParam(final String paramName) {
            for (String param : parameters.keySet()) {
                if (param.equals(paramName)) {
                    return parameters.get(param);
                }
            }
            return null;
        }

        public String getContentType() {
            return getHeader("Content-Type");
        }

        @Override
        public String toString() {
            return "Request{" +
                "method='" + method + '\'' +
                ", uri=" + uri +
                ", parameters=" + parameters +
                ", headers=" + headers +
                ", body=" + body +
                '}';
        }
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

    protected static Map<String, String> contentType(final String contentType) {
        return singletonMap("Content-Type", contentType);
    }
}
