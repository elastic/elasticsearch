/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package fixture.azure;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

/**
 * Emulates the instance metadata service that runs on Azure
 */
@SuppressForbidden(reason = "Uses a HttpServer to emulate an Azure endpoint")
public class AzureOAuthTokenServiceHttpHandler implements HttpHandler {
    private static final Logger logger = LogManager.getLogger(AzureOAuthTokenServiceHttpHandler.class);

    private final String tenantId;
    private final String bearerToken;
    private final String federatedToken;

    public AzureOAuthTokenServiceHttpHandler(String tenantId, String bearerToken, String federatedToken) {
        this.tenantId = tenantId;
        this.bearerToken = bearerToken;
        this.federatedToken = federatedToken;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        // TODO assert on query params
        if ("POST".equals(exchange.getRequestMethod())
            && ("/" + tenantId + "/oauth2/v2.0/token").equals(exchange.getRequestURI().getPath())) {
            assertHasClientAssertionWithFederatedToken(exchange);
            try (exchange; var xcb = XContentBuilder.builder(XContentType.JSON.xContent())) {
                final BytesReference responseBytes = getAccessTokenBytes(xcb);
                writeResponse(exchange, responseBytes);
                return;
            }
        }

        final var msgBuilder = new StringWriter();
        msgBuilder.append("method: ").append(exchange.getRequestMethod()).append(System.lineSeparator());
        msgBuilder.append("uri: ").append(exchange.getRequestURI().toString()).append(System.lineSeparator());
        msgBuilder.append("headers:").append(System.lineSeparator());
        for (final var header : exchange.getRequestHeaders().entrySet()) {
            msgBuilder.append("- ").append(header.getKey()).append(System.lineSeparator());
            for (final var value : header.getValue()) {
                msgBuilder.append("  - ").append(value).append(System.lineSeparator());
            }
        }
        final var msg = msgBuilder.toString();
        logger.info("{}", msg);
        final var responseBytes = msg.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, responseBytes.length);
        new BytesArray(responseBytes).writeTo(exchange.getResponseBody());
        exchange.close();
    }

    private void assertHasClientAssertionWithFederatedToken(HttpExchange exchange) throws IOException {
        final String requestBody = Streams.copyToString(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8));
        if (false == requestBody.contains("client_assertion=" + federatedToken)) {
            throw new AssertionError("Expected request body to contain client_assertion with federated token");
        }
    }

    private void writeResponse(HttpExchange exchange, BytesReference responseBytes) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(200, responseBytes.length());
        responseBytes.writeTo(exchange.getResponseBody());
    }

    private BytesReference getAccessTokenBytes(XContentBuilder xcb) throws IOException {
        final var nowSeconds = System.currentTimeMillis() / 1000L;
        final var validitySeconds = 86400L;
        xcb.startObject();
        xcb.field("access_token", bearerToken);
        xcb.field("client_id", UUIDs.randomBase64UUID());
        xcb.field("expires_in", Long.toString(validitySeconds));
        xcb.field("expires_on", Long.toString(nowSeconds + validitySeconds));
        xcb.field("ext_expires_in", Long.toString(validitySeconds));
        xcb.field("not_before", Long.toString(nowSeconds));
        xcb.field("resource", "https://storage.azure.com");
        xcb.field("token_type", "Bearer");
        xcb.endObject();
        return BytesReference.bytes(xcb);
    }
}
