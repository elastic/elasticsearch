/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package fixture.aws.imds;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.randomIdentifier;
import static org.elasticsearch.test.ESTestCase.randomSecretKey;

/**
 * Minimal HTTP handler that emulates the EC2 IMDS server
 */
@SuppressForbidden(reason = "this test uses a HttpServer to emulate the EC2 IMDS endpoint")
public class Ec2ImdsHttpHandler implements HttpHandler {

    private static final String IMDS_SECURITY_CREDENTIALS_PATH = "/latest/meta-data/iam/security-credentials/";

    private final Ec2ImdsVersion ec2ImdsVersion;
    private final Set<String> validImdsTokens = ConcurrentCollections.newConcurrentSet();

    private final BiConsumer<String, String> newCredentialsConsumer;
    private final Map<String, String> instanceAddresses;
    private final Set<String> validCredentialsEndpoints;
    private final boolean dynamicProfileNames;
    private final Supplier<String> availabilityZoneSupplier;
    @Nullable // if instance identity document not available
    private final ToXContent instanceIdentityDocument;

    public Ec2ImdsHttpHandler(
        Ec2ImdsVersion ec2ImdsVersion,
        BiConsumer<String, String> newCredentialsConsumer,
        Collection<String> alternativeCredentialsEndpoints,
        Supplier<String> availabilityZoneSupplier,
        @Nullable ToXContent instanceIdentityDocument,
        Map<String, String> instanceAddresses
    ) {
        this.ec2ImdsVersion = Objects.requireNonNull(ec2ImdsVersion);
        this.newCredentialsConsumer = Objects.requireNonNull(newCredentialsConsumer);
        this.instanceAddresses = instanceAddresses;

        if (alternativeCredentialsEndpoints.isEmpty()) {
            dynamicProfileNames = true;
            validCredentialsEndpoints = ConcurrentCollections.newConcurrentSet();
        } else if (ec2ImdsVersion == Ec2ImdsVersion.V2) {
            throw new IllegalArgumentException(
                Strings.format("alternative credentials endpoints %s requires IMDSv1", alternativeCredentialsEndpoints)
            );
        } else {
            dynamicProfileNames = false;
            validCredentialsEndpoints = Set.copyOf(alternativeCredentialsEndpoints);
        }

        this.availabilityZoneSupplier = availabilityZoneSupplier;
        this.instanceIdentityDocument = instanceIdentityDocument;
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        // http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

        try (exchange) {
            final var path = exchange.getRequestURI().getPath();
            final var requestMethod = exchange.getRequestMethod();

            if ("PUT".equals(requestMethod) && "/latest/api/token".equals(path)) {
                switch (ec2ImdsVersion) {
                    case V1 -> exchange.sendResponseHeaders(RestStatus.METHOD_NOT_ALLOWED.getStatus(), -1);
                    case V2 -> {
                        final var token = randomSecretKey();
                        validImdsTokens.add(token);
                        final var responseBody = token.getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().add("Content-Type", "text/plain");
                        exchange.getResponseHeaders()
                            .add("x-aws-ec2-metadata-token-ttl-seconds", Long.toString(TimeValue.timeValueDays(1).seconds()));
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), responseBody.length);
                        exchange.getResponseBody().write(responseBody);
                    }
                }
                return;
            }

            if (ec2ImdsVersion == Ec2ImdsVersion.V2) {
                final var token = exchange.getRequestHeaders().getFirst("X-aws-ec2-metadata-token");
                if (token == null) {
                    exchange.sendResponseHeaders(RestStatus.UNAUTHORIZED.getStatus(), -1);
                    return;
                }
                if (validImdsTokens.contains(token) == false) {
                    exchange.sendResponseHeaders(RestStatus.FORBIDDEN.getStatus(), -1);
                    return;
                }
            }

            if ("GET".equals(requestMethod)) {
                if (path.equals(IMDS_SECURITY_CREDENTIALS_PATH) && dynamicProfileNames) {
                    final var profileName = "imds_profile_" + randomIdentifier();
                    validCredentialsEndpoints.add(IMDS_SECURITY_CREDENTIALS_PATH + profileName);
                    sendStringResponse(exchange, profileName);
                    return;
                } else if (path.equals("/latest/meta-data/placement/availability-zone")) {
                    final var availabilityZone = availabilityZoneSupplier.get();
                    sendStringResponse(exchange, availabilityZone);
                    return;
                } else if (instanceIdentityDocument != null && path.equals("/latest/dynamic/instance-identity/document")) {
                    sendStringResponse(exchange, Strings.toString(instanceIdentityDocument));
                    return;
                } else if (validCredentialsEndpoints.contains(path)) {
                    final String accessKey = "test_key_imds_" + randomIdentifier();
                    final String sessionToken = randomIdentifier();
                    newCredentialsConsumer.accept(accessKey, sessionToken);
                    final byte[] response = Strings.format(
                        """
                            {
                              "AccessKeyId": "%s",
                              "Expiration": "%s",
                              "RoleArn": "%s",
                              "SecretAccessKey": "%s",
                              "Token": "%s"
                            }""",
                        accessKey,
                        ZonedDateTime.now(Clock.systemUTC()).plusDays(1L).format(DateTimeFormatter.ISO_DATE_TIME),
                        randomIdentifier(),
                        randomSecretKey(),
                        sessionToken
                    ).getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);
                    return;
                } else if (instanceAddresses.get(path) instanceof String instanceAddress) {
                    sendStringResponse(exchange, instanceAddress);
                    return;
                }
            }

            ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError("not supported: " + requestMethod + " " + path));
        }
    }

    private void sendStringResponse(HttpExchange exchange, String value) throws IOException {
        final byte[] response = value.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/plain");
        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
        exchange.getResponseBody().write(response);
    }
}
