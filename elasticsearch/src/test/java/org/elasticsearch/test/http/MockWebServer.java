/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test.http;

import com.google.common.base.Charsets;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final AtomicInteger index = new AtomicInteger(0);
    private final List<MockResponse> responses = new ArrayList<>();
    private final List<MockRequest> requests = new ArrayList<>();
    private final Logger logger;
    private final SSLContext sslContext;
    private boolean needClientAuth;
    private Set<CountDownLatch> latches = ConcurrentCollections.newConcurrentSet();

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
        this.logger = ESLoggerFactory.getLogger(this.getClass());
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
            HttpsServer httpsServer = HttpsServer.create(address, 0);
            httpsServer.setHttpsConfigurator(new CustomHttpsConfigurator(sslContext, needClientAuth));
            server = httpsServer;
        } else {
            server = HttpServer.create(address, 0);
        }

        server.start();
        server.createContext("/", s -> {
            logger.debug("incoming HTTP request [{} {}]", s.getRequestMethod(), s.getRequestURI());

            try {
                MockResponse response = responses.get(index.getAndAdd(1));
                MockRequest request = createRequest(s);
                requests.add(request);

                sleepIfNeeded(response.getBeforeReplyDelay());

                s.getResponseHeaders().putAll(response.getHeaders().headers);

                if (Strings.isEmpty(response.getBody())) {
                    s.sendResponseHeaders(response.getStatusCode(), 0);
                } else {
                    byte[] responseAsBytes = response.getBody().getBytes(Charsets.UTF_8);
                    s.sendResponseHeaders(response.getStatusCode(), responseAsBytes.length);
                    sleepIfNeeded(response.getBodyDelay());
                    try (OutputStream responseBody = s.getResponseBody()) {
                        responseBody.write(responseAsBytes);
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

    /**
     * A custom HttpsConfigurator that takes the SSL context and the required client authentication into account
     * Also configured the protocols and cipher suites to match the security default ones
     */
    @SuppressForbidden(reason = "use http server")
    private static final class CustomHttpsConfigurator extends HttpsConfigurator {

        private final boolean needClientAuth;

        public CustomHttpsConfigurator(SSLContext sslContext, boolean needClientAuth) {
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
            String body = Streams.copyToString(new InputStreamReader(exchange.getRequestBody()));
            if (Strings.isEmpty(body) == false) {
                request.setBody(body);
            }
        }
        return request;
    }

    /**
     * @return The hostname the server is bound to. Uses #InetSocketAddress.getHostString() to prevent reverse dns lookups
     */
    public String getHostName() {
        return server.getAddress().getHostString();
    }

    /**
     * @return The tcp port that the server is bound to
     */
    public int getPort() {
        return server.getAddress().getPort();
    }

    /**
     * Adds a response to the response queue that is used when a request comes in
     * Note: Every response is only processed once
     * @param response The created mock response
     */
    public void enqueue(MockResponse response) {
        responses.add(response);
    }

    /**
     * @return The requests that have been made to this mock web server
     */
    public List<MockRequest> requests() {
        return requests;
    }

    /**
     * Removes the first request in the list of requests and returns it to the caller.
     * This can be used as a queue if you know the order of your requests deone.
     */
    public MockRequest takeRequest() {
        return requests.remove(0);
    }

    /**
     * Closes down the webserver. Also tries to stop all the currently sleeping requests first by counting down their respective
     * latches.
     */
    @Override
    public void close() {
        logger.debug("Counting down all latches before terminating executor");
        latches.forEach(CountDownLatch::countDown);

        if (server.getExecutor() instanceof ExecutorService) {
            try {
                terminate((ExecutorService) server.getExecutor());
            } catch (InterruptedException e) {
            }
        }
        server.stop(0);
    }
}
