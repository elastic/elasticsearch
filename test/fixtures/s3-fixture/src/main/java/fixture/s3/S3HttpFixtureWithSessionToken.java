/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package fixture.s3;

import com.sun.net.httpserver.HttpHandler;
import org.elasticsearch.rest.RestStatus;

import java.util.Objects;

import static fixture.s3.S3HttpHandler.sendError;

public class S3HttpFixtureWithSessionToken extends S3HttpFixture {

    S3HttpFixtureWithSessionToken(final String[] args) throws Exception {
        super(args);
    }

    @Override
    protected HttpHandler createHandler(final String[] args) {
        final String sessionToken = Objects.requireNonNull(args[5], "session token is missing");
        final HttpHandler delegate = super.createHandler(args);
        return exchange -> {
            final String securityToken = exchange.getRequestHeaders().getFirst("x-amz-security-token");
            if (securityToken == null) {
                sendError(exchange, RestStatus.FORBIDDEN, "AccessDenied", "No session token");
                return;
            }
            if (securityToken.equals(sessionToken) == false) {
                sendError(exchange, RestStatus.FORBIDDEN, "AccessDenied", "Bad session token");
                return;
            }
            delegate.handle(exchange);
        };
    }

    public static void main(final String[] args) throws Exception {
        if (args == null || args.length < 6) {
            throw new IllegalArgumentException("S3HttpFixtureWithSessionToken expects 6 arguments " +
                "[address, port, bucket, base path, access key, session token]");
        }
        final S3HttpFixtureWithSessionToken fixture = new S3HttpFixtureWithSessionToken(args);
        fixture.start();
    }
}
