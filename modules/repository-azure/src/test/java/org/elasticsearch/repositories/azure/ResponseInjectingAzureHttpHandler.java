/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.function.Predicate;

@SuppressForbidden(reason = "we use a HttpServer to emulate Azure")
class ResponseInjectingAzureHttpHandler implements ESMockAPIBasedRepositoryIntegTestCase.DelegatingHttpHandler {

    private final HttpHandler delegate;
    private final Queue<RequestHandler> requestHandlerQueue;

    ResponseInjectingAzureHttpHandler(Queue<RequestHandler> requestHandlerQueue, HttpHandler delegate) {
        this.delegate = delegate;
        this.requestHandlerQueue = requestHandlerQueue;
        AzureBlobContainerStatsTests test = new AzureBlobContainerStatsTests();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        RequestHandler nextHandler = requestHandlerQueue.peek();
        if (nextHandler != null && nextHandler.matchesRequest(exchange)) {
            requestHandlerQueue.poll().writeResponse(exchange, delegate);
        } else {
            delegate.handle(exchange);
        }
    }

    @Override
    public HttpHandler getDelegate() {
        return delegate;
    }

    /**
     * Creates a {@link ResponseInjectingAzureHttpHandler.RequestHandler} that will persistently fail the first <code>numberToFail</code>
     * distinct requests it sees. Any other requests are passed through to the delegate.
     *
     * @param numberToFail The number of requests to fail
     * @return the handler
     */
    static ResponseInjectingAzureHttpHandler.RequestHandler createFailNRequestsHandler(int numberToFail) {
        final List<String> requestsToFail = new ArrayList<>(numberToFail);
        return (exchange, delegate) -> {
            final Headers requestHeaders = exchange.getRequestHeaders();
            final String requestId = requestHeaders.get("X-ms-client-request-id").get(0);
            boolean failRequest = false;
            synchronized (requestsToFail) {
                if (requestsToFail.contains(requestId)) {
                    failRequest = true;
                } else if (requestsToFail.size() < numberToFail) {
                    requestsToFail.add(requestId);
                    failRequest = true;
                }
            }
            if (failRequest) {
                exchange.sendResponseHeaders(500, -1);
            } else {
                delegate.handle(exchange);
            }
        };
    }

    @SuppressForbidden(reason = "we use a HttpServer to emulate Azure")
    @FunctionalInterface
    interface RequestHandler {
        void writeResponse(HttpExchange exchange, HttpHandler delegate) throws IOException;

        default boolean matchesRequest(HttpExchange exchange) {
            return true;
        }
    }

    @SuppressForbidden(reason = "we use a HttpServer to emulate Azure")
    static class FixedRequestHandler implements RequestHandler {

        private final RestStatus status;
        private final String responseBody;
        private final Predicate<HttpExchange> requestMatcher;

        FixedRequestHandler(RestStatus status) {
            this(status, null, req -> true);
        }

        /**
         * Create a handler that only gets executed for requests that match the supplied predicate. Note
         * that because the errors are stored in a queue this will prevent any subsequently queued errors from
         * being returned until after it returns.
         */
        FixedRequestHandler(RestStatus status, String responseBody, Predicate<HttpExchange> requestMatcher) {
            this.status = status;
            this.responseBody = responseBody;
            this.requestMatcher = requestMatcher;
        }

        @Override
        public boolean matchesRequest(HttpExchange exchange) {
            return requestMatcher.test(exchange);
        }

        @Override
        public void writeResponse(HttpExchange exchange, HttpHandler delegateHandler) throws IOException {
            if (responseBody != null) {
                byte[] responseBytes = responseBody.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(status.getStatus(), responseBytes.length);
                exchange.getResponseBody().write(responseBytes);
            } else {
                exchange.sendResponseHeaders(status.getStatus(), -1);
            }
        }
    }
}
