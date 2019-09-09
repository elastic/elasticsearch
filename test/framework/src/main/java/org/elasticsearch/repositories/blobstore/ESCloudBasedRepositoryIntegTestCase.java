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
package org.elasticsearch.repositories.blobstore;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.HttpStatus;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Integration tests for {@link BlobStoreRepository} implementations that relies on cloud-based
 * services. This abstract class is responsible of starting and stopping an internal HTTP server
 * which is used to simulate an external storage service like S3.
 */
@SuppressForbidden(reason = "this test uses a HttpServer to emulate a cloud-based storage service")
public abstract class ESCloudBasedRepositoryIntegTestCase extends ESBlobStoreRepositoryIntegTestCase {

    private static HttpServer httpServer;
    private static Boolean randomServerErrors;
    private Map<String, HttpHandler> handlers;

    @BeforeClass
    public static void startHttpServer() throws Exception {
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        randomServerErrors = false;//randomBoolean();
    }

    @Before
    public void setUpHttpServer() {
        handlers = createHttpHandlers();
        handlers.forEach((c, h) -> {
            HttpHandler handler = h;
            if (randomServerErrors) {
                handler = createErroneousHttpHandler(handler);
            }
            httpServer.createContext(c, handler);
        });
    }

    @AfterClass
    public static void stopHttpServer() {
        httpServer.stop(0);
        httpServer = null;
        randomServerErrors = null;
    }

    @After
    public void tearDownHttpServer() {
        if (handlers != null) {
            handlers.keySet().forEach(context -> httpServer.removeContext(context));
        }
    }

    protected abstract Map<String, HttpHandler> createHttpHandlers();

    protected abstract HttpHandler createErroneousHttpHandler(HttpHandler delegate);

    protected static boolean hasRandomServerErrors() {
        assert randomServerErrors != null;
        return randomServerErrors;
    }

    protected static String httpServerUrl() {
        InetSocketAddress address = httpServer.getAddress();
        return "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
    }

    /**
     * HTTP handler that injects random service errors
     *
     * Note: it is not a good idea to allow this handler to simulate too many errors as it would
     * slow down the test suite.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate a cloud-based storage service")
    protected static class ErroneousHttpHandler implements HttpHandler {

        // first key is a unique identifier for the incoming HTTP request,
        // value is the number of times the request has been seen
        private final Map<String, AtomicInteger> requests;
        private final HttpHandler delegate;
        private final int maxErrorsPerRequest;

        @SuppressForbidden(reason = "this test uses a HttpServer to emulate a cloud-based storage service")
        protected ErroneousHttpHandler(final HttpHandler delegate, final int maxErrorsPerRequest) {
            this.requests = new ConcurrentHashMap<>();
            this.delegate = delegate;
            this.maxErrorsPerRequest = maxErrorsPerRequest;
            assert maxErrorsPerRequest > 1;
        }

        @Override
        public void handle(final HttpExchange exchange) throws IOException {
            final String requestId = requestUniqueId(exchange);
            assert Strings.hasText(requestId);

            final boolean canFailRequest = canFailRequest(exchange);
            final int count = requests.computeIfAbsent(requestId, req -> new AtomicInteger(0)).incrementAndGet();
            if (count >= maxErrorsPerRequest || canFailRequest == false || randomBoolean()) {
                requests.remove(requestId);
                delegate.handle(exchange);
            } else {
                handleAsError(exchange);
            }
        }

        private void handleAsError(final HttpExchange exchange) throws IOException {
            Streams.readFully(exchange.getRequestBody());
            exchange.sendResponseHeaders(HttpStatus.SC_INTERNAL_SERVER_ERROR, -1);
            exchange.close();
        }

        protected String requestUniqueId(final HttpExchange exchange) {
            return exchange.getRemoteAddress().toString()
                + " " + exchange.getRequestMethod()
                + " " + exchange.getRequestURI();
        }

        protected boolean canFailRequest(final HttpExchange exchange) {
            return true;
        }
    }
}
