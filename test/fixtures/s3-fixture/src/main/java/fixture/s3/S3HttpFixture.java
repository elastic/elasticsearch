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

import org.elasticsearch.rest.RestStatus;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Objects;

public class S3HttpFixture extends ExternalResource {

    private final HttpServer server;

    private boolean enabled;

    public S3HttpFixture() {
        this(true);
    }

    public S3HttpFixture(boolean enabled) {
        this(enabled, "bucket", "base_path_integration_tests", "s3_test_access_key");
    }

    public S3HttpFixture(boolean enabled, String... args) {
        this(resolveAddress("localhost", 0), args);
        this.enabled = enabled;
    }

    public S3HttpFixture(final String[] args) throws Exception {
        this(resolveAddress(args[0], Integer.parseInt(args[1])), args);
    }

    public S3HttpFixture(InetSocketAddress inetSocketAddress, String... args) {
        try {
            this.server = HttpServer.create(inetSocketAddress, 0);
            this.server.createContext("/", Objects.requireNonNull(createHandler(args)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    final void startWithWait() throws Exception {
        try {
            server.start();
            // wait to be killed
            Thread.sleep(Long.MAX_VALUE);
        } finally {
            server.stop(0);
        }
    }

    public final void start() throws Exception {
        server.start();
    }

    protected HttpHandler createHandler(final String[] args) {
        final String bucket = Objects.requireNonNull(args[0]);
        final String basePath = args[1];
        final String accessKey = Objects.requireNonNull(args[2]);

        return new S3HttpHandler(bucket, basePath) {
            @Override
            public void handle(final HttpExchange exchange) throws IOException {
                final String authorization = exchange.getRequestHeaders().getFirst("Authorization");
                if (authorization == null || authorization.contains(accessKey) == false) {
                    sendError(exchange, RestStatus.FORBIDDEN, "AccessDenied", "Bad access key");
                    return;
                }
                super.handle(exchange);
            }
        };
    }

    public static void main(final String[] args) throws Exception {
        if (args == null || args.length < 5) {
            throw new IllegalArgumentException("S3HttpFixture expects 5 arguments [address, port, bucket, base path, access key]");
        }
        InetSocketAddress inetSocketAddress = resolveAddress(args[0], Integer.parseInt(args[1]));
        final S3HttpFixture fixture = new S3HttpFixture(inetSocketAddress, Arrays.copyOfRange(args, 2, args.length));
        fixture.startWithWait();
    }

    public String getAddress() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort();
    }

    public void stop(int delay) {
        server.stop(delay);
    }

    protected void before() throws Throwable {
        if (enabled) {
            start();
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
