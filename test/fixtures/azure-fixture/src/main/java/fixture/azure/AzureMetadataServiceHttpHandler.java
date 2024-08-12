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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

/**
 * Emulates the instance metadata service that runs on Azure
 */
@SuppressForbidden(reason = "Uses a HttpServer to emulate an Azure endpoint")
public class AzureMetadataServiceHttpHandler implements HttpHandler {
    private static final Logger logger = LogManager.getLogger(AzureMetadataServiceHttpHandler.class);

    private final String bearerToken;

    public AzureMetadataServiceHttpHandler(String bearerToken) {
        this.bearerToken = bearerToken;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if ("GET".equals(exchange.getRequestMethod())
            && "/metadata/identity/oauth2/token".equals(exchange.getRequestURI().getPath())
            && "api-version=2018-02-01&resource=https://storage.azure.com".equals(exchange.getRequestURI().getQuery())) {
            AzureOAuthTokenServiceHttpHandler.respondWithValidAccessToken(exchange, bearerToken);
            return;
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
}
