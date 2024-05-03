/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.gcs;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.impl.bootstrap.HttpServer;
import org.apache.http.impl.bootstrap.ServerBootstrap;
import org.apache.http.protocol.HttpContext;
import org.elasticsearch.common.network.NetworkAddress;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

/**
 * A mock HTTP Proxy server for testing of support of HTTP proxies in various SDKs
 */
abstract class MockHttpProxyServer implements Closeable {

    private final HttpServer httpServer;

    MockHttpProxyServer() {
        httpServer = ServerBootstrap.bootstrap()
            .setLocalAddress(InetAddress.getLoopbackAddress())
            .setListenerPort(0)
            .registerHandler("*", this::handle)
            .create();
        try {
            httpServer.start();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start HTTP proxy server", e);
        }
    }

    public abstract void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException;

    int getPort() {
        return httpServer.getLocalPort();
    }

    String getHost() {
        return NetworkAddress.format(httpServer.getInetAddress());
    }

    @Override
    public void close() throws IOException {
        httpServer.shutdown(10, TimeUnit.SECONDS);
    }
}
