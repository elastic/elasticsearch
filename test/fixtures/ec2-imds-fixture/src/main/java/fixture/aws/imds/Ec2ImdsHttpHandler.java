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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomIdentifier;

/**
 * Minimal HTTP handler that emulates the EC2 IMDS server
 */
@SuppressForbidden(reason = "this test uses a HttpServer to emulate the EC2 IMDS endpoint")
public class Ec2ImdsHttpHandler implements HttpHandler {

    private static final String IMDS_SECURITY_CREDENTIALS_PATH = "/latest/meta-data/iam/security-credentials/";

    private final String accessKey;
    private final String sessionToken;
    private final Set<String> validCredentialsEndpoints = ConcurrentCollections.newConcurrentSet();

    public Ec2ImdsHttpHandler(String accessKey, String sessionToken, Collection<String> alternativeCredentialsEndpoints) {
        this.accessKey = Objects.requireNonNull(accessKey);
        this.sessionToken = Objects.requireNonNull(sessionToken);
        this.validCredentialsEndpoints.addAll(alternativeCredentialsEndpoints);
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        // http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

        try (exchange) {
            final var path = exchange.getRequestURI().getPath();
            final var requestMethod = exchange.getRequestMethod();

            if ("PUT".equals(requestMethod) && "/latest/api/token".equals(path)) {
                // Reject IMDSv2 probe
                exchange.sendResponseHeaders(RestStatus.METHOD_NOT_ALLOWED.getStatus(), -1);
                return;
            }

            if ("GET".equals(requestMethod)) {
                if (path.equals(IMDS_SECURITY_CREDENTIALS_PATH)) {
                    final var profileName = randomIdentifier();
                    validCredentialsEndpoints.add(IMDS_SECURITY_CREDENTIALS_PATH + profileName);
                    final byte[] response = profileName.getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "text/plain");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);
                    return;
                } else if (validCredentialsEndpoints.contains(path)) {
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
                        randomIdentifier(),
                        sessionToken
                    ).getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);
                    return;
                }
            }

            ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError("not supported: " + requestMethod + " " + path));
        }
    }
}
