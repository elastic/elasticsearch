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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class ProtoHttpServer<R> {

    private final ProtoHandler<R> handler;
    private final String protoSuffix;
    private final Client client;
    private HttpServer server;
    private ExecutorService executor;

    public ProtoHttpServer(Client client, ProtoHandler<R> handler, String protoSuffix) {
        this.client = client;
        this.handler = handler;
        this.protoSuffix = protoSuffix;
    }

    public void start(int port) throws IOException {
        // similar to Executors.newCached but with a smaller bound and much smaller keep-alive
        executor = new ThreadPoolExecutor(0, 10, 250, TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>());

        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
        server.createContext(protoSuffix, handler);
        server.setExecutor(executor);
        server.start();
    }

    public void stop() {
        server.stop(1);
        server = null;
        executor.shutdownNow();
        executor = null;
    }

    public InetSocketAddress address() {
        return server != null ? server.getAddress() : null;
    }

    public String url() {
        return server != null ? "localhost:" + address().getPort() + protoSuffix : "<not started>";
    }

    public Client client() {
        return client;
    }
}