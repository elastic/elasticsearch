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
import java.util.UUID;

/**
 * In-process H2 {@link JdbcDatabaseFixture} — the baseline database for the shared JDBC correctness matrix. It needs
 * no external process (and so no Docker), but it does <b>not</b> use H2's opaque {@code jdbc:h2:mem:...} URL, because
 * ESQL's external-source resolver parses every dataset resource through {@code StoragePath.of}, which requires a
 * {@code ://} authority separator that the opaque in-memory form lacks. Instead the whole database lives in the test
 * JVM behind an in-process <b>H2 TCP server</b>, reached by a {@code jdbc:h2:tcp://localhost:<port>/mem:<db>} URL — a
 * genuine {@code ://} URL the resolver can parse and whose compound scheme is {@code jdbc:h2:tcp}
 * (see {@code JdbcConnectorFactory.SUPPORTED_SCHEMES}). The server runs in the same JVM as the cluster node, so the
 * connector still reaches the database over a loopback socket with no external process — which is why the H2 suites
 * enable {@code esql.jdbc.ssrf.allow_loopback} and allow the {@code jdbc:h2:tcp://} subprotocol.
 * <p>
 * <b>One shared server, one database per fixture instance.</b> {@link AbstractJdbcDatabaseIT} creates one fixture per
 * matrix table; all instances share a single reference-counted TCP server (the first {@link #startDatabase()} boots
 * it, later ones attach, and it is stopped when the last fixture stops), and each instance gets a fresh random
 * {@code mem:} database name so the tables never collide. The server is started with {@code -ifNotExists} so a client
 * connecting to a not-yet-created {@code mem:} database creates it, and {@code -tcpDaemon} so its threads do not block
 * JVM shutdown.
 * <p>
 * <b>Why the URL parameters.</b> {@code DB_CLOSE_DELAY=-1} keeps the in-memory database alive for the server's
 * lifetime even between connections (H2 otherwise discards an in-mem database when its last connection drops); the
 * fixture's keep-alive control connection plus this flag pin it open until {@link #stop()}. {@code
 * DATABASE_TO_UPPER=false} preserves the lower-case identifiers the portable fixtures declare (e.g. {@code emp_no}),
 * so the resolved column names match the ES|QL {@code outputType()} names the matrix asserts.
 */
public final class H2Fixture extends JdbcDatabaseFixture {

    /** Guards {@link #tcpServer} and {@link #refCount}: instances are created/stopped across the test lifecycle. */
    private static final Object LOCK = new Object();

    /** The single shared in-process H2 TCP server, or {@code null} when no fixture currently holds it. */
    private static Server tcpServer;

    /** Number of started (not-yet-stopped) fixtures attached to {@link #tcpServer}; the server dies at 0. */
    private static int refCount;

    /** Whether this instance has incremented {@link #refCount} and so must decrement exactly once in {@link #stopDatabase()}. */
    private boolean counted;

    /** Fresh random in-memory database name for this fixture instance, so two instances never share tables. */
    private final String dbName = "h2jdbc_" + UUID.randomUUID().toString().replace("-", "");

    private volatile String url;

    @Override
    protected void startDatabase() throws Exception {
        synchronized (LOCK) {
            if (tcpServer == null) {
                int port = findFreePort();
                Server pending = Server.createTcpServer("-tcpPort", Integer.toString(port), "-ifNotExists", "-tcpDaemon");
                // put-before-start: publish the reference before start() so a partially-started server is still
                // reachable for teardown.
                tcpServer = pending;
                try {
                    pending.start();
                } catch (Exception e) {
                    tcpServer = null;
                    try {
                        pending.stop();
                    } catch (Exception suppressed) {
                        e.addSuppressed(suppressed);
                    }
                    throw e;
                }
            }
            refCount++;
            counted = true;
            url = "jdbc:h2:tcp://localhost:" + tcpServer.getPort() + "/mem:" + dbName + ";DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false";
        }
    }

    @Override
    protected void stopDatabase() {
        synchronized (LOCK) {
            if (counted == false) {
                // startDatabase() never completed for this instance: nothing to release, and we must not touch the count.
                return;
            }
            counted = false;
            refCount--;
            if (refCount == 0 && tcpServer != null) {
                try {
                    tcpServer.stop();
                } finally {
                    tcpServer = null;
                }
            }
        }
    }

    @Override
    public String esqlJdbcUrl() {
        String local = url;
        if (local == null) {
            throw new IllegalStateException("H2 TCP server not started; call start() first");
        }
        return local;
    }

    @Override
    protected String driverClassName() {
        return "org.h2.Driver";
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
