/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package fixture.gcs;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class GoogleCloudStorageHttpFixture {

    private final HttpServer server;

    private GoogleCloudStorageHttpFixture(final String address, final int port, final String bucket, final String token)
        throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(address), port), 0);
        server.createContext("/" + token, new FakeOAuth2HttpHandler());
        server.createContext("/computeMetadata/v1/project/project-id", new FakeProjectIdHttpHandler());
        server.createContext("/", new GoogleCloudStorageHttpHandler(bucket));
    }

    private void start() throws Exception {
        try {
            server.start();
            // wait to be killed
            Thread.sleep(Long.MAX_VALUE);
        } finally {
            server.stop(0);
        }
    }

    public static void main(final String[] args) throws Exception {
        if (args == null || args.length != 4) {
            throw new IllegalArgumentException("GoogleCloudStorageHttpFixture expects 4 arguments [address, port, bucket, token]");
        }
        GoogleCloudStorageHttpFixture fixture = new GoogleCloudStorageHttpFixture(args[0], Integer.parseInt(args[1]), args[2], args[3]);
        fixture.start();
    }
}
