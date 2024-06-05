/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

public class S3HttpFixture extends ExternalResource {

    private HttpServer server;

    private boolean enabled;
    private final String bucket;
    private final String basePath;
    protected final String accessKey;

    public S3HttpFixture() {
        this(true);
    }

    public S3HttpFixture(boolean enabled) {
        this(enabled, "bucket", "base_path_integration_tests", "s3_test_access_key");
    }

    public S3HttpFixture(boolean enabled, String bucket, String basePath, String accessKey) {
        this.enabled = enabled;
        this.bucket = bucket;
        this.basePath = basePath;
        this.accessKey = accessKey;
    }

    protected HttpHandler createHandler() {
        return new S3HttpHandler(bucket, basePath) {
            @Override
            public void handle(final HttpExchange exchange) throws IOException {
                try {
                    final String authorization = exchange.getRequestHeaders().getFirst("Authorization");
                    if (authorization == null || authorization.contains(accessKey) == false) {
                        sendError(exchange, RestStatus.FORBIDDEN, "AccessDenied", "Bad access key");
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
            InetSocketAddress inetSocketAddress = resolveAddress("localhost", 0);
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

    private static InetSocketAddress resolveAddress(String address, int port) {
        try {
            return new InetSocketAddress(InetAddress.getByName(address), port);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}
