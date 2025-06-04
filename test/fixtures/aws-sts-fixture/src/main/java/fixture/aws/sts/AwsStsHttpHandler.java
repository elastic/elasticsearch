/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package fixture.aws.sts;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomIdentifier;
import static org.elasticsearch.test.ESTestCase.randomSecretKey;

/**
 * Minimal HTTP handler that emulates the AWS STS server
 */
@SuppressForbidden(reason = "this test uses a HttpServer to emulate the AWS STS endpoint")
public class AwsStsHttpHandler implements HttpHandler {

    public static final String ROLE_ARN = "arn:aws:iam::123456789012:role/FederatedWebIdentityRole";
    public static final String ROLE_NAME = "sts-fixture-test";

    private final BiConsumer<String, String> newCredentialsConsumer;
    private final String webIdentityToken;

    public AwsStsHttpHandler(BiConsumer<String, String> newCredentialsConsumer, String webIdentityToken) {
        this.newCredentialsConsumer = Objects.requireNonNull(newCredentialsConsumer);
        this.webIdentityToken = Objects.requireNonNull(webIdentityToken);
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        // https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html

        try (exchange) {
            final var requestMethod = exchange.getRequestMethod();
            final var path = exchange.getRequestURI().getPath();

            if ("POST".equals(requestMethod) && "/".equals(path)) {

                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                Map<String, String> params = Arrays.stream(body.split("&"))
                    .map(e -> e.split("="))
                    .collect(Collectors.toMap(e -> e[0], e -> URLDecoder.decode(e[1], StandardCharsets.UTF_8)));
                if ("AssumeRoleWithWebIdentity".equals(params.get("Action")) == false) {
                    exchange.sendResponseHeaders(RestStatus.BAD_REQUEST.getStatus(), 0);
                    exchange.close();
                    return;
                }
                if (ROLE_NAME.equals(params.get("RoleSessionName")) == false
                    || webIdentityToken.equals(params.get("WebIdentityToken")) == false
                    || ROLE_ARN.equals(params.get("RoleArn")) == false) {
                    exchange.sendResponseHeaders(RestStatus.UNAUTHORIZED.getStatus(), 0);
                    exchange.close();
                    return;
                }
                final var accessKey = "test_key_STS_" + randomIdentifier();
                final var sessionToken = randomIdentifier();
                newCredentialsConsumer.accept(accessKey, sessionToken);
                final byte[] response = String.format(
                    Locale.ROOT,
                    """
                        <AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
                          <AssumeRoleWithWebIdentityResult>
                            <SubjectFromWebIdentityToken>amzn1.account.AF6RHO7KZU5XRVQJGXK6HB56KR2A</SubjectFromWebIdentityToken>
                            <Audience>client.5498841531868486423.1548@apps.example.com</Audience>
                            <AssumedRoleUser>
                              <Arn>%s</Arn>
                              <AssumedRoleId>AROACLKWSDQRAOEXAMPLE:%s</AssumedRoleId>
                            </AssumedRoleUser>
                            <Credentials>
                              <SessionToken>%s</SessionToken>
                              <SecretAccessKey>%s</SecretAccessKey>
                              <Expiration>%s</Expiration>
                              <AccessKeyId>%s</AccessKeyId>
                            </Credentials>
                            <SourceIdentity>SourceIdentityValue</SourceIdentity>
                            <Provider>www.amazon.com</Provider>
                          </AssumeRoleWithWebIdentityResult>
                          <ResponseMetadata>
                            <RequestId>ad4156e9-bce1-11e2-82e6-6b6efEXAMPLE</RequestId>
                          </ResponseMetadata>
                        </AssumeRoleWithWebIdentityResponse>""",
                    ROLE_ARN,
                    ROLE_NAME,
                    sessionToken,
                    randomSecretKey(),
                    ZonedDateTime.now(Clock.systemUTC()).plusDays(1L).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ")),
                    accessKey
                ).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "text/xml; charset=UTF-8");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);
                exchange.close();
                return;
            }

            ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError("not supported: " + requestMethod + " " + path));
        }
    }
}
