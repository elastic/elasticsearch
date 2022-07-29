/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package fixture.gcs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressForbidden(reason = "Uses a HttpServer to emulate a fake OAuth2 authentication service")
public class FakeOAuth2HttpHandler implements HttpHandler {

    private static final byte[] BUFFER = new byte[1024];

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        try {
            while (exchange.getRequestBody().read(BUFFER) >= 0) {
            }
            byte[] response = """
                {"access_token":"foo","token_type":"Bearer","expires_in":3600}""".getBytes(UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.getResponseHeaders().add("Metadata-Flavor", "Google");
            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
            exchange.getResponseBody().write(response);
        } finally {
            int read = exchange.getRequestBody().read();
            assert read == -1 : "Request body should have been fully read here but saw [" + read + "]";
            exchange.close();
        }
    }
}
