/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package fixture.s3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.rest.RestStatus;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

public class S3HttpFixture extends ExternalResource {

    private HttpServer server;

    private final boolean enabled;
    private final String bucket;
    private final String basePath;
    private final BiPredicate<String, String> authorizationPredicate;

    public S3HttpFixture(boolean enabled) {
        this(enabled, "bucket", "base_path_integration_tests", fixedAccessKey("s3_test_access_key"));
    }

    public S3HttpFixture(boolean enabled, String bucket, String basePath, BiPredicate<String, String> authorizationPredicate) {
        this.enabled = enabled;
        this.bucket = bucket;
        this.basePath = basePath;
        this.authorizationPredicate = authorizationPredicate;
    }

    protected HttpHandler createHandler() {
        return new S3HttpHandler(bucket, basePath) {
            @Override
            public void handle(final HttpExchange exchange) throws IOException {
                try {
                    if (authorizationPredicate.test(
                        exchange.getRequestHeaders().getFirst("Authorization"),
                        exchange.getRequestHeaders().getFirst("x-amz-security-token")
                    ) == false) {
                        sendError(exchange, RestStatus.FORBIDDEN, "AccessDenied", "Access denied by " + authorizationPredicate);
                        return;
                    }
                    super.handle(exchange);
                } catch (Error e) {
                    // HttpServer catches Throwable, so we must throw errors on another thread
                    ExceptionsHelper.maybeDieOnAnotherThread(e);
                    throw e;
                }
            }
        };
    }

    public String getAddress() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort();
    }

    public void stop(int delay) {
        server.stop(delay);
    }

    protected void before() throws Throwable {
        if (enabled) {
            InetSocketAddress inetSocketAddress = resolveAddress();
            this.server = HttpServer.create(inetSocketAddress, 0);
            HttpHandler handler = createHandler();
            this.server.createContext("/", Objects.requireNonNull(handler));
            server.start();
        }
    }

    @Override
    protected void after() {
        if (enabled) {
            stop(0);
        }
    }

    private static InetSocketAddress resolveAddress() {
        try {
            return new InetSocketAddress(InetAddress.getByName("localhost"), 0);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public static BiPredicate<String, String> fixedAccessKey(String accessKey) {
        return mutableAccessKey(() -> accessKey);
    }

    public static BiPredicate<String, String> mutableAccessKey(Supplier<String> accessKeySupplier) {
        return (authorizationHeader, sessionTokenHeader) -> authorizationHeader != null
            && authorizationHeader.contains(accessKeySupplier.get());
    }

    public static BiPredicate<String, String> fixedAccessKeyAndToken(String accessKey, String sessionToken) {
        Objects.requireNonNull(sessionToken);
        final var accessKeyPredicate = fixedAccessKey(accessKey);
        return (authorizationHeader, sessionTokenHeader) -> accessKeyPredicate.test(authorizationHeader, sessionTokenHeader)
            && sessionToken.equals(sessionTokenHeader);
    }
}
