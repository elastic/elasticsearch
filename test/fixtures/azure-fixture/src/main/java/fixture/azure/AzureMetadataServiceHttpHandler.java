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

import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Emulates the instance metadata service that runs on Azure
 */
@SuppressForbidden(reason = "Uses a HttpServer to emulate an Azure endpoint")
public class AzureMetadataServiceHttpHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
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
        throw new UnsupportedOperationException(msgBuilder.toString());
    }
}
