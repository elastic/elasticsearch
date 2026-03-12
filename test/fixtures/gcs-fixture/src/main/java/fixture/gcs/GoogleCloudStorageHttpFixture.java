/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package fixture.gcs;

import com.sun.net.httpserver.HttpServer;

import org.junit.rules.ExternalResource;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class GoogleCloudStorageHttpFixture extends ExternalResource {

    private final boolean enabled;
    private final String bucket;
    private final String token;
    private HttpServer server;
    private GoogleCloudStorageHttpHandler handler;

    public GoogleCloudStorageHttpFixture(boolean enabled, final String bucket, final String token) {
        this.enabled = enabled;
        this.bucket = bucket;
        this.token = token;
    }

    public String getAddress() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort();
    }

    /**
     * Get the underlying HTTP handler for direct blob manipulation (e.g., loading test fixtures).
     */
    public GoogleCloudStorageHttpHandler getHandler() {
        return handler;
    }

    /**
     * Get the token path used for OAuth2 authentication.
     */
    public String getToken() {
        return token;
    }

    @Override
    protected void before() throws Throwable {
        if (enabled) {
            this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            this.handler = new GoogleCloudStorageHttpHandler(bucket);
            server.createContext("/" + token, new FakeOAuth2HttpHandler());
            server.createContext("/computeMetadata/v1/project/project-id", new FakeProjectIdHttpHandler());
            server.createContext("/", handler);
            server.start();
        }
    }

    @Override
    protected void after() {
        if (enabled) {
            server.stop(0);
        }
    }
}
