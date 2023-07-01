/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

    public GoogleCloudStorageHttpFixture(boolean enabled, final String bucket, final String token) {
        this.enabled = enabled;
        this.bucket = bucket;
        this.token = token;
    }

    public String getAddress() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort();
    }

    @Override
    protected void before() throws Throwable {
        if (enabled) {
            this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            server.createContext("/" + token, new FakeOAuth2HttpHandler());
            server.createContext("/computeMetadata/v1/project/project-id", new FakeProjectIdHttpHandler());
            server.createContext("/", new GoogleCloudStorageHttpHandler(bucket));
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
