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

import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class S3HttpFixtureWithEC2 extends S3HttpFixtureWithSessionToken {

    private static final String EC2_PATH = "/latest/meta-data/iam/security-credentials/";
    private static final String EC2_PROFILE = "ec2Profile";

    S3HttpFixtureWithEC2(final String[] args) throws Exception {
        super(args);
    }

    @Override
    protected HttpHandler createHandler(final String[] args) {
        final String ec2AccessKey = Objects.requireNonNull(args[4]);
        final String ec2SessionToken = Objects.requireNonNull(args[5], "session token is missing");
        final HttpHandler delegate = super.createHandler(args);

        return exchange -> {
            final String path = exchange.getRequestURI().getPath();
            // http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html
            if ("GET".equals(exchange.getRequestMethod()) && path.startsWith(EC2_PATH)) {
                if (path.equals(EC2_PATH)) {
                    final byte[] response = EC2_PROFILE.getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "text/plain");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);
                    exchange.close();
                    return;

                } else if (path.equals(EC2_PATH + EC2_PROFILE)) {
                    final byte[] response = buildCredentialResponse(ec2AccessKey, ec2SessionToken).getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);
                    exchange.close();
                    return;
                }

                final byte[] response = "unknown profile".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "text/plain");
                exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), response.length);
                exchange.getResponseBody().write(response);
                exchange.close();
                return;

            }
            delegate.handle(exchange);
        };
    }

    protected String buildCredentialResponse(final String ec2AccessKey, final String ec2SessionToken) {
        return """
            {
              "AccessKeyId": "%s",
              "Expiration": "%s",
              "RoleArn": "arn",
              "SecretAccessKey": "secret_access_key",
              "Token": "%s"
            }""".formatted(ec2AccessKey, ZonedDateTime.now().plusDays(1L).format(DateTimeFormatter.ISO_DATE_TIME), ec2SessionToken);
    }

    public static void main(final String[] args) throws Exception {
        if (args == null || args.length < 6) {
            throw new IllegalArgumentException(
                "S3HttpFixtureWithEC2 expects 6 arguments " + "[address, port, bucket, base path, ec2 access id, ec2 session token]"
            );
        }
        final S3HttpFixtureWithEC2 fixture = new S3HttpFixtureWithEC2(args);
        fixture.start();
    }
}
