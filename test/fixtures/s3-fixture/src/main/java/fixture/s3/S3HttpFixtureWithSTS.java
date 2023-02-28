/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package fixture.s3;

import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.rest.RestStatus;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class S3HttpFixtureWithSTS extends S3HttpFixture {

    private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/FederatedWebIdentityRole";
    private static final String ROLE_NAME = "sts-fixture-test";

    private S3HttpFixtureWithSTS(final String[] args) throws Exception {
        super(args);
    }

    @Override
    protected HttpHandler createHandler(final String[] args) {
        String accessKey = Objects.requireNonNull(args[4]);
        String sessionToken = Objects.requireNonNull(args[5], "session token is missing");
        String webIdentityToken = Objects.requireNonNull(args[6], "web identity token is missing");
        final HttpHandler delegate = super.createHandler(args);

        return exchange -> {
            // https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html
            // It's run as a separate service, but we emulate it under the `assume-role-with-web-identity` endpoint
            // of the S3 serve for the simplicity sake
            if ("POST".equals(exchange.getRequestMethod())
                && exchange.getRequestURI().getPath().startsWith("/assume-role-with-web-identity")) {
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
                              <SecretAccessKey>secret_access_key</SecretAccessKey>
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
                    ZonedDateTime.now().plusDays(1L).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ")),
                    accessKey
                ).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "text/xml; charset=UTF-8");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);
                exchange.close();
                return;
            }
            delegate.handle(exchange);
        };
    }

    public static void main(final String[] args) throws Exception {
        if (args == null || args.length < 7) {
            throw new IllegalArgumentException(
                "S3HttpFixtureWithSTS expects 7 arguments [address, port, bucket, base path, sts access id, sts session token, web identity token]"
            );
        }
        final S3HttpFixtureWithSTS fixture = new S3HttpFixtureWithSTS(args);
        fixture.start();
    }
}
