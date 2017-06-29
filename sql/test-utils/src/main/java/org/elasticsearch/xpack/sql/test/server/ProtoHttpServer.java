/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.test.server;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.client.Client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public abstract class ProtoHttpServer<R> {

    private final ProtoHandler<R> handler;
    private final String defaultPrefix, protoPrefix;
    private final Client client;
    private HttpServer server;

    public ProtoHttpServer(Client client, ProtoHandler<R> handler, String defaultPrefix, String protoPrefix) {
        this.client = client;
        this.handler = handler;
        this.defaultPrefix = defaultPrefix;
        this.protoPrefix = protoPrefix;
    }

    public void start(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
        server.createContext(defaultPrefix, new RootHttpHandler());
        server.createContext(defaultPrefix + protoPrefix, handler);
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
    }

    public void stop() {
        server.stop(1);
        server = null;
    }

    public InetSocketAddress address() {
        return server != null ? server.getAddress() : null;
    }

    public String url() {
        return server != null ? "localhost:" + address().getPort() + defaultPrefix + protoPrefix : "<not started>";
    }

    public Client client() {
        return client;
    }
}