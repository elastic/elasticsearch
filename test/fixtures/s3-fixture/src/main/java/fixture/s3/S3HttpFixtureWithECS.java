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

public class S3HttpFixtureWithECS extends S3HttpFixtureWithEC2 {

    public S3HttpFixtureWithECS() {
        this(true);
    }

    public S3HttpFixtureWithECS(boolean enabled) {
        this(enabled, "ecs_bucket", "ecs_base_path", "ecs_access_key", "ecs_session_token");
    }

    public S3HttpFixtureWithECS(boolean enabled, String bucket, String basePath, String accessKey, String sessionToken) {
        super(enabled, bucket, basePath, accessKey, sessionToken);
    }

    @Override
    protected HttpHandler createHandler() {
        final HttpHandler delegate = super.createHandler();

        return exchange -> {
            // https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html
            if ("GET".equals(exchange.getRequestMethod()) && exchange.getRequestURI().getPath().equals("/ecs_credentials_endpoint")) {
                final byte[] response = buildCredentialResponse(accessKey, sessionToken).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);
                exchange.close();
                return;
            }
            delegate.handle(exchange);
        };
    }
}
