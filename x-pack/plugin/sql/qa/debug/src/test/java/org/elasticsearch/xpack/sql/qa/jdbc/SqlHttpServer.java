/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.jdbc;

import com.sun.net.httpserver.HttpServer;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.xpack.sql.plugin.RestSqlClearCursorAction;
import org.elasticsearch.xpack.sql.plugin.RestSqlQueryAction;
import org.elasticsearch.xpack.sql.proto.Protocol;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@SuppressForbidden(reason = "use http server")
@SuppressWarnings("restriction")
class SqlHttpServer {

    private final SqlNodeClient sqlClient;

    private HttpServer server;
    private ExecutorService executor;

    SqlHttpServer(Client client) {
        this.sqlClient = client instanceof SqlNodeClient ? (SqlNodeClient) client : new SqlNodeClient(client);
    }


    void start(int port) throws IOException {
        // similar to Executors.newCached but with a smaller bound and much smaller keep-alive
        executor = new ThreadPoolExecutor(1, 10, 250, TimeUnit.MILLISECONDS, new SynchronousQueue<>());

        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
        server.createContext("/", new RootHandler());
        initialize(server);
        server.setExecutor(executor);
        server.start();
    }

    private void initialize(HttpServer server) {
        // initialize cursor
        initializeActions(server);
    }


    private void initializeActions(HttpServer server) {
        RestController mock = Mockito.mock(RestController.class);
        server.createContext(Protocol.SQL_QUERY_REST_ENDPOINT,
            new SqlHandler(sqlClient, new RestSqlQueryAction(Settings.EMPTY, mock)));
        server.createContext(Protocol.CLEAR_CURSOR_REST_ENDPOINT,
            new SqlHandler(sqlClient, new RestSqlClearCursorAction(Settings.EMPTY, mock)));
    }


    void stop() {
        server.stop(1);
        server = null;
        executor.shutdownNow();
        executor = null;
    }

    private InetSocketAddress address() {
        return server != null ? server.getAddress() : null;
    }

    String url() {
        InetSocketAddress address = address();
        return address != null ? "localhost:" + address.getPort() : "<not started>";
    }

    String jdbcUrl() {
        return "jdbc:es://" + url();
    }
}
