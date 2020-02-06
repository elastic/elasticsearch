/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.mocksocket.MockHttpServer;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ESTestCase.terminate;

/**
 * A MockWebServer to test against. Holds a list of responses, which can be enqueed.
 * The webserver has to enqueue at least the amount of responses with the number of requests that happen, otherwise errors
 * will be returned.
 * <p>
 * Each response that was executed also contains the request, so you can check if requests happened in the correct order.
 */
@SuppressForbidden(reason = "use http server")
public class MockWebServer implements Closeable {

    private HttpServer server;
    private final Queue<MockResponse> responses = ConcurrentCollections.newQueue();
    private final Queue<MockRequest> requests = ConcurrentCollections.newQueue();
    private final Logger logger;
    private final SSLContext sslContext;
    private final boolean needClientAuth;
    private final Set<CountDownLatch> latches = ConcurrentCollections.newConcurrentSet();
    private String hostname;
    private int port;

    /**
     * Instantiates a webserver without https
     */
    public MockWebServer() {
        this(null, false);
    }

    /**
     * Instantiates a webserver with https
     * @param sslContext The SSL context to be used for encryption
     * @param needClientAuth Should clientAuth be used, which requires a client side certificate
     */
    public MockWebServer(SSLContext sslContext, boolean needClientAuth) {
        this.needClientAuth = needClientAuth;
        this.logger = LogManager.getLogger(this.getClass());
        this.sslContext = sslContext;
    }

    /**
     * Starts the webserver and binds it to an arbitrary ephemeral port
     * The webserver will be able to serve requests once this method returns
     *
     * @throws IOException in case of a binding or other I/O errors
     */
    public void start() throws IOException {
        InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress().getHostAddress(), 0);
        if (sslContext != null) {
            HttpsServer httpsServer = MockHttpServer.createHttps(address, 0);
            httpsServer.setHttpsConfigurator(new CustomHttpsConfigurator(sslContext, needClientAuth));
            server = httpsServer;
        } else {
            server = MockHttpServer.createHttp(address, 0);
        }

        server.start();
        // Uses #InetSocketAddress.getHostString() to prevent reverse dns lookups, eager binding, so we can find out host/port regardless
        // if the webserver was already shut down
        this.hostname = server.getAddress().getHostString();
        this.port = server.getAddress().getPort();
        server.createContext("/", s -> {
            try {
                MockResponse response = responses.poll();
                MockRequest request = createRequest(s);
                requests.add(request);

                if (logger.isDebugEnabled()) {
                    logger.debug("[{}:{}] incoming HTTP request [{} {}], returning status [{}] body [{}]", getHostName(), getPort(),
                            s.getRequestMethod(), s.getRequestURI(), response.getStatusCode(), getStartOfBody(response));
                }

                sleepIfNeeded(response.getBeforeReplyDelay());

                s.getResponseHeaders().putAll(response.getHeaders().headers);

                if (Strings.isEmpty(response.getBody())) {
                    s.sendResponseHeaders(response.getStatusCode(), 0);
                } else {
                    byte[] responseAsBytes = response.getBody().getBytes(StandardCharsets.UTF_8);
                    s.sendResponseHeaders(response.getStatusCode(), responseAsBytes.length);
                    sleepIfNeeded(response.getBodyDelay());
                    if ("HEAD".equals(request.getMethod()) == false) {
                        try (OutputStream responseBody = s.getResponseBody()) {
                            responseBody.write(responseAsBytes);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to respond to request [{} {}]",
                        s.getRequestMethod(), s.getRequestURI()), e);
            } finally {
                s.close();
            }

        });
        logger.info("bound HTTP mock server to [{}:{}]", getHostName(), getPort());
    }

    public SocketAddress getAddress() {
        return new InetSocketAddress(hostname, port);
    }

    public URI getUri(String path) throws URISyntaxException {
        if (hostname == null) {
            throw new IllegalStateException("Web server must be started in order to determine its URI");
        }
        final String scheme = this.sslContext == null ? "http" : "https";
        return new URI(scheme, null, hostname, port, path, null, null);
    }

    /**
     * A custom HttpsConfigurator that takes the SSL context and the required client authentication into account
     * Also configured the protocols and cipher suites to match the security default ones
     */
    @SuppressForbidden(reason = "use http server")
    private static final class CustomHttpsConfigurator extends HttpsConfigurator {

        private final boolean needClientAuth;

        CustomHttpsConfigurator(SSLContext sslContext, boolean needClientAuth) {
            super(sslContext);
            this.needClientAuth = needClientAuth;
        }

        @Override
        public void configure(HttpsParameters params) {
            params.setNeedClientAuth(needClientAuth);
        }
    }

    /**
     * Sleep the specified amount of time, if the time value is not null
     */
    private void sleepIfNeeded(TimeValue timeValue) throws InterruptedException {
        if (timeValue == null) {
            return;
        }

        CountDownLatch latch = new CountDownLatch(1);
        latches.add(latch);
        try {
            latch.await(timeValue.millis(), TimeUnit.MILLISECONDS);
        } finally {
            latches.remove(latch);
        }
    }

    /**
     * Creates a MockRequest from an incoming HTTP request, that can later be checked in your test assertions
     */
    private MockRequest createRequest(HttpExchange exchange) throws IOException {
        MockRequest request = new MockRequest(exchange.getRequestMethod(), exchange.getRequestURI(), exchange.getRequestHeaders());
        if (exchange.getRequestBody() != null) {
            String body = Streams.copyToString(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8));
            if (Strings.isEmpty(body) == false) {
                request.setBody(body);
            }
        }
        return request;
    }

    /**
     * @return The hostname the server is bound to.
     */
    public String getHostName() {
        return hostname;
    }

    /**
     * @return The tcp port that the server is bound to
     */
    public int getPort() {
        return port;
    }

    /**
     * Adds a response to the response queue that is used when a request comes in
     * Note: Every response is only processed once
     * @param response The created mock response
     */
    public void enqueue(MockResponse response) {
        if (logger.isTraceEnabled()) {
            logger.trace("[{}:{}] Enqueueing response [{}], status [{}] body [{}]", getHostName(), getPort(), responses.size(),
                    response.getStatusCode(), getStartOfBody(response));
        }
        responses.add(response);
    }

    /**
     * @return The requests that have been made to this mock web server
     */
    public List<MockRequest> requests() {
        return new ArrayList<>(requests);
    }

    /**
     * Removes the first request in the list of requests and returns it to the caller.
     * This can be used as a queue if you are sure the order of your requests.
     */
    public MockRequest takeRequest() {
        return requests.poll();
    }

    /**
     * Removes all requests from the queue.
     */
    public void clearRequests() {
        requests.clear();
    }

    /**
     * A utility method to peek into the requests and find out if #MockWebServer.takeRequests will not throw an out of bound exception
     * @return true if more requests are available, false otherwise
     */
    public boolean hasMoreRequests() {
        return requests.isEmpty() == false;
    }

    /**
     * Closes down the webserver. Also tries to stop all the currently sleeping requests first by counting down their respective
     * latches.
     */
    @Override
    public void close() {
        logger.debug("[{}:{}] Counting down all latches before terminating executor", getHostName(), getPort());
        latches.forEach(CountDownLatch::countDown);

        if (server.getExecutor() instanceof ExecutorService) {
            terminate((ExecutorService) server.getExecutor());
        }
        server.stop(0);
    }

    /**
     * Helper method to return the first 20 chars of a request's body
     * @param response The MockResponse to inspect
     * @return Returns the first 20 chars or an empty string if the response body is not configured
     */
    private String getStartOfBody(MockResponse response) {
        if (Strings.isEmpty(response.getBody())) {
            return "";
        }
        int length = Math.min(20, response.getBody().length());
        return response.getBody().substring(0, length).replaceAll("\n", "");
    }
}
