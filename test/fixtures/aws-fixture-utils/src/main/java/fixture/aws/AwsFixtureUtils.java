/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.aws;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

public enum AwsFixtureUtils {
    ;

    /**
     * @return an {@link InetSocketAddress} for a test fixture running on {@code localhost} which binds to any available port.
     */
    public static InetSocketAddress getLocalFixtureAddress() {
        try {
            return new InetSocketAddress(InetAddress.getByName("localhost"), 0);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Send an XML-formatted error response typical of an AWS service.
     */
    public static void sendError(final HttpExchange exchange, final RestStatus status, final String errorCode, final String message)
        throws IOException {
        final Headers headers = exchange.getResponseHeaders();
        headers.add("Content-Type", "application/xml");

        final String requestId = exchange.getRequestHeaders().getFirst("x-amz-request-id");
        if (requestId != null) {
            headers.add("x-amz-request-id", requestId);
        }

        if (errorCode == null || "HEAD".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(status.getStatus(), -1L);
            exchange.close();
        } else {
            final byte[] response = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error>"
                + "<Code>"
                + errorCode
                + "</Code>"
                + "<Message>"
                + message
                + "</Message>"
                + "<RequestId>"
                + requestId
                + "</RequestId>"
                + "</Error>").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status.getStatus(), response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        }
    }
}
