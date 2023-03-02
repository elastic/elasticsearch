/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.gcs;

import org.apache.http.impl.bootstrap.HttpServer;
import org.apache.http.impl.bootstrap.ServerBootstrap;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.network.NetworkAddress;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

/**
 * A mock HTTP Proxy server for testing of support of HTTP proxies in various SDKs
 */
class MockHttpProxyServer implements Closeable {

    private static final Logger log = LogManager.getLogger(MockHttpProxyServer.class);

    private HttpServer httpServer;

    MockHttpProxyServer handler(HttpRequestHandler handler) throws IOException {
        httpServer = ServerBootstrap.bootstrap()
            .setLocalAddress(InetAddress.getLoopbackAddress())
            .setListenerPort(0)
            .registerHandler("*", handler)
            .create();
        httpServer.start();
        return this;
    }

    int getPort() {
        return httpServer.getLocalPort();
    }

    String getHost() {
        return NetworkAddress.format(httpServer.getInetAddress());
    }

    @Override
    public void close() throws IOException {
        httpServer.shutdown(0, TimeUnit.SECONDS);
    }
}
