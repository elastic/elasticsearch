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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@SuppressForbidden(reason = "Uses a HttpServer to emulate an Azure endpoint")
public class AzureOAuthTokenServiceHttpHandler implements HttpHandler {
    private static final Logger logger = LogManager.getLogger(AzureOAuthTokenServiceHttpHandler.class);
    private static final String EXPECTED_SCOPE = "https://storage.azure.com/.default";

    private final String bearerToken;
    private final String federatedToken;
    @Nullable
    private final String tenantId;
    @Nullable
    private final String clientId;

    public AzureOAuthTokenServiceHttpHandler(
        String bearerToken,
        String federatedToken,
        @Nullable String tenantId,
        @Nullable String clientId
    ) {
        this.bearerToken = bearerToken;
        this.federatedToken = federatedToken;
        this.tenantId = tenantId;
        this.clientId = clientId;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (tenantId == null) {
            throw new AssertionError("OAuth token service handler should not be called if [tenantId] is null");
        }
        if (clientId == null) {
            throw new AssertionError("OAuth token service handler should not be called if [clientId] is null");
        }

        if ("POST".equals(exchange.getRequestMethod())
            && ("/" + tenantId + "/oauth2/v2.0/token").equals(exchange.getRequestURI().getPath())) {
            final String requestBody = Streams.copyToString(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8));

            final Map<String, String> params = new HashMap<>();
            RestUtils.decodeQueryString(requestBody, 0, params);

            if (clientId.equals(params.get("client_id"))
                && federatedToken.equals(params.get("client_assertion"))
                && EXPECTED_SCOPE.equals(params.get("scope"))) {
                respondWithValidAccessToken(exchange, bearerToken);
                return;
            }

            logger.error(
                "Request body did not contain expected [client_id], [client_assertion], or [scope]. Request data: [{}]",
                requestBody
            );
            // fall through to further logging and dummy response

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

    static void respondWithValidAccessToken(HttpExchange exchange, String bearerToken) throws IOException {
        try (exchange; var accessTokenXContent = accessTokenXContent(bearerToken)) {
            final var responseBytes = BytesReference.bytes(accessTokenXContent);
            exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(200, responseBytes.length());
            responseBytes.writeTo(exchange.getResponseBody());
        }
    }

    private static XContentBuilder accessTokenXContent(String bearerToken) throws IOException {
        final var xcb = XContentBuilder.builder(XContentType.JSON.xContent());
        xcb.startObject();
        xcb.field("access_token", bearerToken);
        xcb.field("client_id", UUIDs.randomBase64UUID());
        xcb.field("expires_in", 86400L);
        xcb.field("expires_on", System.currentTimeMillis() / 1000L + 86400L);
        xcb.field("ext_expires_in", 86400L);
        xcb.field("not_before", System.currentTimeMillis() / 1000L);
        xcb.field("resource", "https://storage.azure.com");
        xcb.field("token_type", "Bearer");
        xcb.endObject();
        return xcb;
    }
}
