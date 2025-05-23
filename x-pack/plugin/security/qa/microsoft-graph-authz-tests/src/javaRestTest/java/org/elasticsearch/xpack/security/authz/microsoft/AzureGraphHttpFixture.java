/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.microsoft;

import com.sun.net.httpserver.HttpExchange;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.ssl.KeyStoreUtil;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.rules.ExternalResource;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class AzureGraphHttpFixture extends ExternalResource {

    private static final Logger logger = LogManager.getLogger(AzureGraphHttpFixture.class);

    private final String tenantId;
    private final String clientId;
    private final String clientSecret;
    private final String principal;
    private final String displayName;
    private final String email;
    private final String jwt;

    private HttpsServer server;

    public AzureGraphHttpFixture(
        String tenantId,
        String clientId,
        String clientSecret,
        String principal,
        String displayName,
        String email
    ) {
        this.tenantId = tenantId;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.principal = principal;
        this.displayName = displayName;
        this.email = email;

        jwt = "test jwt";
    }

    @Override
    protected void before() throws Throwable {
        final var certificate = PemUtils.readCertificates(
            List.of(Path.of(getClass().getClassLoader().getResource("server/cert.pem").toURI()))
        ).getFirst();
        final var key = PemUtils.readPrivateKey(Path.of(getClass().getClassLoader().getResource("server/cert.key").toURI()), () -> null);
        final var sslContext = SSLContext.getInstance("TLS");
        sslContext.init(
            new KeyManager[] { KeyStoreUtil.createKeyManager(new Certificate[] { certificate }, key, null) },
            null,
            new SecureRandom()
        );

        server = HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(sslContext));

        registerGetAccessTokenHandler();
        registerGetUserHandler();
        registerGetUserMembershipHandler();

        server.createContext("/", exchange -> {
            logger.warn("Unhandled request for [{}]", exchange.getRequestURI());
            exchange.sendResponseHeaders(RestStatus.NOT_IMPLEMENTED.getStatus(), 0);
            exchange.close();
        });
        server.start();
    }

    @Override
    protected void after() {
        server.stop(0);
    }

    public String getBaseUrl() {
        return "https://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort();
    }

    private void registerGetAccessTokenHandler() {
        server.createContext("/" + tenantId + "/oauth2/v2.0/token", exchange -> {
            if (exchange.getRequestMethod().equals("POST") == false) {
                graphError(exchange, RestStatus.METHOD_NOT_ALLOWED, "Expected POST request");
                return;
            }

            final var requestBody = Streams.copyToString(new InputStreamReader(exchange.getRequestBody(), Charset.defaultCharset()));
            final var formFields = new HashMap<String, String>();
            RestUtils.decodeQueryString(requestBody, 0, formFields);

            if (formFields.get("grant_type").equals("client_credentials") == false) {
                graphError(exchange, RestStatus.BAD_REQUEST, Strings.format("Unexpected Grant Type: %s", formFields.get("grant_type")));
                return;
            }
            if (formFields.get("client_id").equals(clientId) == false) {
                graphError(exchange, RestStatus.BAD_REQUEST, Strings.format("Unexpected Client ID: %s", formFields.get("client_id")));
                return;
            }
            if (formFields.get("client_secret").equals(clientSecret) == false) {
                graphError(
                    exchange,
                    RestStatus.BAD_REQUEST,
                    Strings.format("Unexpected Client Secret: %s", formFields.get("client_secret"))
                );
                return;
            }
            if (formFields.get("scope").contains("https://graph.microsoft.com/.default") == false) {
                graphError(
                    exchange,
                    RestStatus.BAD_REQUEST,
                    Strings.format("Missing required https://graph.microsoft.com/.default scope: [%s]", formFields.get("scope"))
                );
                return;
            }

            final var token = XContentBuilder.builder(XContentType.JSON.xContent());
            token.startObject();
            token.field("access_token", jwt);
            token.field("expires_in", 86400L);
            token.field("ext_expires_in", 86400L);
            token.field("token_type", "Bearer");
            token.endObject();

            var responseBytes = BytesReference.bytes(token);

            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), responseBytes.length());
            responseBytes.writeTo(exchange.getResponseBody());
            exchange.close();
        });
    }

    private void registerGetUserHandler() {
        server.createContext("/v1.0/users/" + principal, exchange -> {
            if (exchange.getRequestMethod().equals("GET") == false) {
                graphError(exchange, RestStatus.METHOD_NOT_ALLOWED, "Expected GET request");
                return;
            }

            final var authorization = exchange.getRequestHeaders().getFirst("Authorization");
            if (authorization.equals("Bearer " + jwt) == false) {
                graphError(exchange, RestStatus.UNAUTHORIZED, Strings.format("Wrong Authorization header: %s", authorization));
                return;
            }

            if (exchange.getRequestURI().getQuery().contains("$select=displayName,mail") == false) {
                graphError(exchange, RestStatus.BAD_REQUEST, "Must filter fields using $select");
                return;
            }

            var userProperties = XContentBuilder.builder(XContentType.JSON.xContent());
            userProperties.startObject();
            userProperties.field("displayName", displayName);
            userProperties.field("mail", email);
            userProperties.endObject();

            var responseBytes = BytesReference.bytes(userProperties);

            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), responseBytes.length());
            responseBytes.writeTo(exchange.getResponseBody());

            exchange.close();
        });
    }

    private void registerGetUserMembershipHandler() {
        final var skipToken = UUID.randomUUID().toString();

        server.createContext("/v1.0/users/" + principal + "/memberOf", exchange -> {
            if (exchange.getRequestMethod().equals("GET") == false) {
                graphError(exchange, RestStatus.METHOD_NOT_ALLOWED, "Expected GET request");
                return;
            }

            final var authorization = exchange.getRequestHeaders().getFirst("Authorization");
            if (authorization.equals("Bearer " + jwt) == false) {
                graphError(exchange, RestStatus.UNAUTHORIZED, Strings.format("Wrong Authorization header: %s", authorization));
                return;
            }

            if (exchange.getRequestURI().getQuery().contains("$select=id") == false) {
                // this test server only returns `id`s, so if the client is expecting other fields, it won't work anyway
                graphError(exchange, RestStatus.BAD_REQUEST, "Must filter fields using $select");
                return;
            }

            var nextLink = getBaseUrl() + exchange.getRequestURI().toString() + "&$skiptoken=" + skipToken;
            var groups = new Object[] { Map.of("id", "group-id-1"), Map.of("id", "group-id-2") };

            // return multiple pages of results, to ensure client correctly supports paging
            if (exchange.getRequestURI().getQuery().contains("$skiptoken")) {
                groups = new Object[] { Map.of("id", "group-id-3") };
                nextLink = null;
            }

            final var groupMembership = XContentBuilder.builder(XContentType.JSON.xContent());
            groupMembership.startObject();
            groupMembership.field("@odata.nextLink", nextLink);
            groupMembership.array("value", groups);
            groupMembership.endObject();

            var responseBytes = BytesReference.bytes(groupMembership);

            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), responseBytes.length());
            responseBytes.writeTo(exchange.getResponseBody());

            exchange.close();
        });
    }

    // attempt to comply with https://learn.microsoft.com/en-us/graph/errors
    private void graphError(HttpExchange exchange, RestStatus statusCode, String message) throws IOException {
        logger.warn(message);

        final var errorResponse = XContentBuilder.builder(XContentType.JSON.xContent());
        errorResponse.startObject();
        errorResponse.startObject("error");
        errorResponse.field("code", statusCode.toString());
        errorResponse.field("message", message);
        errorResponse.endObject();
        errorResponse.endObject();

        final var responseBytes = message.getBytes();
        exchange.sendResponseHeaders(statusCode.getStatus(), responseBytes.length);
        exchange.getResponseBody().write(responseBytes);

        exchange.close();
    }
}
