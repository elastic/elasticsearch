/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import org.h2.tools.Server;

import java.net.InetAddress;
import java.net.ServerSocket;

/**
 * A tiny in-process H2 TCP server for the standalone JDBC ITs that manage their own H2 lifecycle (rather than going
 * through {@link JdbcDatabaseFixture}). It exists because ESQL's external-source resolver parses every dataset
 * resource through {@code StoragePath.of}, which requires a {@code ://} authority separator; H2's opaque
 * {@code jdbc:h2:mem:...} URL has none and cannot flow through the resolver, whereas a {@code jdbc:h2:tcp://…} URL
 * can. The server runs in the same JVM as the cluster node (this is an {@code internalClusterTest}) so the connector
 * still reaches the database over a loopback socket with no external process.
 * <p>
 * Started with {@code -ifNotExists} so a client connecting to a not-yet-created {@code mem:} database creates it, and
 * {@code -tcpDaemon} so its threads do not block JVM shutdown.
 */
public final class H2TcpTestServer implements AutoCloseable {

    private final Server server;

    private H2TcpTestServer(Server server) {
        this.server = server;
    }

    /** Boots a fresh H2 TCP server on a free loopback port. The caller owns it and must {@link #close()} it. */
    public static H2TcpTestServer start() throws Exception {
        int port = findFreePort();
        Server server = Server.createTcpServer("-tcpPort", Integer.toString(port), "-ifNotExists", "-tcpDaemon");
        server.start();
        return new H2TcpTestServer(server);
    }

    /**
     * A {@code jdbc:h2:tcp://localhost:<port>/mem:<dbName>} URL for an in-memory database on this server, kept alive
     * for the server's lifetime ({@code DB_CLOSE_DELAY=-1}) and preserving lower-case identifiers
     * ({@code DATABASE_TO_UPPER=false}) so resolved column names match the ES|QL {@code outputType()} names.
     */
    public String urlFor(String dbName) {
        return "jdbc:h2:tcp://localhost:" + server.getPort() + "/mem:" + dbName + ";DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false";
    }

    @Override
    public void close() {
        server.stop();
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
