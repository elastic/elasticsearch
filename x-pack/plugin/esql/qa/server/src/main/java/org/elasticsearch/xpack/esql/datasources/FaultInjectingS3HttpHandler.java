/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * HTTP handler wrapper that injects configurable faults into S3 requests.
 * Supports HTTP 503, HTTP 500, and connection reset fault types with
 * countdown-based auto-clearing and path-based filtering.
 */
@SuppressForbidden(reason = "uses HttpServer to emulate a faulty S3 endpoint")
public class FaultInjectingS3HttpHandler implements HttpHandler {

    private static final Logger logger = LogManager.getLogger(FaultInjectingS3HttpHandler.class);

    private final HttpHandler delegate;
    private volatile FaultConfig activeFault;

    public FaultInjectingS3HttpHandler(HttpHandler delegate) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate must not be null");
        }
        this.delegate = delegate;
    }

    public void setFault(FaultType type, int count) {
        setFault(type, count, path -> true);
    }

    public void setFault(FaultType type, int count, Predicate<String> pathFilter) {
        this.activeFault = new FaultConfig(type, new AtomicInteger(count), pathFilter);
    }

    public void clearFault() {
        this.activeFault = null;
    }

    public int remainingFaults() {
        FaultConfig fault = this.activeFault;
        return fault == null ? 0 : fault.remaining.get();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        FaultConfig fault = this.activeFault;
        if (fault != null && fault.remaining.get() > 0) {
            String path = exchange.getRequestURI().getPath();
            if (fault.pathFilter.test(path) && fault.remaining.decrementAndGet() >= 0) {
                logger.debug("injecting fault [{}] for path [{}], remaining [{}]", fault.type, path, fault.remaining.get());
                injectFault(exchange, fault.type);
                return;
            }
        }
        delegate.handle(exchange);
    }

    private static void injectFault(HttpExchange exchange, FaultType type) throws IOException {
        switch (type) {
            case HTTP_503 -> sendErrorResponse(exchange, 503, "Service Unavailable", "SlowDown", "Reduce your request rate");
            case HTTP_500 -> sendErrorResponse(exchange, 500, "Internal Server Error", "InternalError", "Internal server error");
            case CONNECTION_RESET -> {
                exchange.close();
            }
        }
    }

    private static void sendErrorResponse(HttpExchange exchange, int statusCode, String statusText, String code, String message)
        throws IOException {
        String body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            + "<Error><Code>"
            + code
            + "</Code><Message>"
            + message
            + "</Message></Error>";
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/xml");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    public enum FaultType {
        HTTP_503,
        HTTP_500,
        CONNECTION_RESET
    }

    private record FaultConfig(FaultType type, AtomicInteger remaining, Predicate<String> pathFilter) {}
}
