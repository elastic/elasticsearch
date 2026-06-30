/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3.qa;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Minimal HTTP fixture emulating the EKS Pod Identity Agent / ECS task role credentials endpoint.
 *
 * <p>The AWS SDK's {@code ContainerCredentialsProvider} resolves the endpoint URL from
 * {@code AWS_CONTAINER_CREDENTIALS_FULL_URI} / {@code AWS_CONTAINER_CREDENTIALS_RELATIVE_URI},
 * loads an auth token from the file pointed at by {@code AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE}
 * (or the JVM sysprop our plugin sets), and issues a GET against the endpoint with
 * {@code Authorization: <token>}. The response JSON shape is:
 *
 * <pre>{@code
 * { "AccessKeyId": "...", "SecretAccessKey": "...", "Token": "...", "Expiration": "..." }
 * }</pre>
 *
 * <p>This fixture validates the {@code Authorization} header against {@code expectedTokenSupplier}
 * and emits a fresh access-key/session-token pair on every request, registering them via
 * {@code newCredentialsConsumer} (so the S3 fixture can authorize the resulting signed S3
 * requests). Mirrors the structure of {@code AwsStsHttpFixture} but for container credentials.
 */
@SuppressForbidden(reason = "test fixture uses HttpServer to emulate the EKS Pod Identity / ECS credentials endpoint")
public final class PodIdentityCredentialsHttpFixture extends ExternalResource {

    private final Supplier<String> expectedTokenSupplier;
    private final BiConsumer<String, String> newCredentialsConsumer;
    private final Supplier<String> secretKeySupplier;

    private HttpServer server;

    public PodIdentityCredentialsHttpFixture(
        Supplier<String> expectedTokenSupplier,
        BiConsumer<String, String> newCredentialsConsumer,
        Supplier<String> secretKeySupplier
    ) {
        this.expectedTokenSupplier = Objects.requireNonNull(expectedTokenSupplier);
        this.newCredentialsConsumer = Objects.requireNonNull(newCredentialsConsumer);
        this.secretKeySupplier = Objects.requireNonNull(secretKeySupplier);
    }

    public String getCredentialsUri() {
        InetSocketAddress addr = server.getAddress();
        return "http://" + InetAddresses.toUriString(addr.getAddress()) + ":" + addr.getPort() + "/eks-pod-identity-credentials";
    }

    @Override
    protected void before() throws Throwable {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/eks-pod-identity-credentials", this::handle);
        server.start();
    }

    @Override
    protected void after() {
        if (server != null) {
            server.stop(0);
            server = null;
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if ("GET".equals(exchange.getRequestMethod()) == false) {
                exchange.sendResponseHeaders(RestStatus.METHOD_NOT_ALLOWED.getStatus(), 0);
                return;
            }
            String authHeader = exchange.getRequestHeaders().getFirst("Authorization");
            String expectedToken = expectedTokenSupplier.get();
            if (expectedToken.equals(authHeader) == false) {
                exchange.sendResponseHeaders(RestStatus.UNAUTHORIZED.getStatus(), 0);
                return;
            }
            String accessKeyId = "test_key_PI_" + Long.toHexString(System.nanoTime());
            String sessionToken = "test_session_PI_" + Long.toHexString(System.nanoTime());
            newCredentialsConsumer.accept(accessKeyId, sessionToken);
            String body = String.format(
                Locale.ROOT,
                "{\"AccessKeyId\":\"%s\",\"SecretAccessKey\":\"%s\",\"Token\":\"%s\",\"Expiration\":\"%s\"}",
                accessKeyId,
                secretKeySupplier.get(),
                sessionToken,
                Instant.now().plus(15, ChronoUnit.MINUTES).toString()
            );
            byte[] response = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
            exchange.getResponseBody().write(response);
        }
    }
}
