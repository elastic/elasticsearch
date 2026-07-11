/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Framework-agnostic base for a per-database test fixture that a JDBC-backed ES|QL integration test drives. A concrete
 * subclass supplies the {@code jdbc:...} endpoint (and, for out-of-process databases, brings that endpoint up/down);
 * this base owns the boilerplate of loading the driver, holding a keep-alive control connection, and running DDL/DML
 * to seed a dataset.
 * <p>
 * <b>Why this shape.</b> It is modelled directly on how {@code JdbcDatasetIT} drives H2:
 * {@code Class.forName(driver)} so the test classloader owns the driver, then a single keep-alive {@link Connection}
 * (opened for the fixture's lifetime) that pins an in-memory database open and is reused to run {@code CREATE TABLE}
 * / {@code INSERT}. Capturing that pattern behind {@link #esqlJdbcUrl()} + {@link #start()}/{@link #stop()} +
 * {@link #load(String...)} lets the H2 baseline and the Postgres testcontainers path share one
 * contract: H2 returns an in-mem URL and needs no {@link #startDatabase()}; Postgres starts a container in
 * {@link #startDatabase()}, derives its URL, and returns credentials from {@link #connectionProperties()}.
 * <p>
 * <b>Deliberately depends only on {@code java.sql}.</b> There is no coupling to the Elasticsearch test framework, so
 * a later change can lift this class verbatim into a {@code qa/server} module; for now the
 * harness lives in the {@code internalClusterTest} source set.
 * <p>
 * Not thread-safe: a fixture is started, seeded, and torn down by a single test lifecycle thread.
 */
public abstract class JdbcDatabaseFixture implements AutoCloseable {

    private Connection controlConnection;

    /**
     * The {@code jdbc:...} URL an ES|QL {@code FROM} statement uses to reach this database. For out-of-process
     * databases this is only well-defined after {@link #start()} has brought the endpoint up.
     */
    public abstract String esqlJdbcUrl();

    /** Fully-qualified JDBC {@link java.sql.Driver} class backing {@link #esqlJdbcUrl()}, loaded in {@link #start()}. */
    protected abstract String driverClassName();

    /**
     * Connection properties (e.g. user/password) applied to every JDBC connection this fixture opens. Defaults to
     * empty, which suits credential-less in-memory H2; container-backed fixtures override to supply their credentials.
     */
    protected Properties connectionProperties() {
        return new Properties();
    }

    /**
     * Extra {@code WITH (...)} entries — besides {@code table} — a dataset needs so the in-cluster JDBC connector can
     * reach this database. Empty for credential-less in-memory H2; container-backed fixtures (Postgres) override to
     * supply {@code user}/{@code password} so {@code JdbcConnectorFactory} can authenticate at resolve/open time.
     * Returned as plain strings; the connector accepts either a plain string or a {@code SecureString} for these keys.
     */
    protected Map<String, String> datasetConfigOverrides() {
        return Map.of();
    }

    /**
     * Brings the database endpoint up (e.g. starts a container). Runs before the control connection is opened.
     * No-op for library-backed databases such as in-memory H2, which need no external process.
     */
    protected void startDatabase() throws Exception {}

    /** Releases whatever {@link #startDatabase()} brought up. Runs after the control connection is closed. */
    protected void stopDatabase() throws Exception {}

    /**
     * Loads the JDBC driver, brings the database up ({@link #startDatabase()}), and opens the keep-alive control
     * connection used for DDL/DML. Idempotency is not required: a fixture is started exactly once per test lifecycle.
     */
    public final void start() throws Exception {
        // Force the driver class to load in the test classloader (where it lives) so DriverManager can resolve
        // esqlJdbcUrl(); mirrors JdbcDatasetIT#setUpH2.
        Class.forName(driverClassName());
        startDatabase();
        controlConnection = newConnection();
    }

    /**
     * Closes the control connection and then the database endpoint. Both steps are attempted even if the first throws;
     * the first failure is rethrown (a database-teardown failure is suppressed onto it) so a leak is never silent.
     */
    public final void stop() throws Exception {
        Exception failure = null;
        if (controlConnection != null) {
            try {
                controlConnection.close();
            } catch (SQLException e) {
                failure = e;
            } finally {
                controlConnection = null;
            }
        }
        try {
            stopDatabase();
        } catch (Exception e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Opens a fresh JDBC connection to {@link #esqlJdbcUrl()} using {@link #connectionProperties()}. The caller owns
     * and must close the returned connection; this is distinct from the fixture's own keep-alive control connection.
     */
    public final Connection newConnection() throws SQLException {
        return DriverManager.getConnection(esqlJdbcUrl(), connectionProperties());
    }

    /** DDL/DML load seam: runs each statement, in order, on the fixture's keep-alive control connection. */
    public final void load(String... statements) throws SQLException {
        load(List.of(statements));
    }

    /** DDL/DML load seam: runs each statement, in order, on the fixture's keep-alive control connection. */
    public final void load(List<String> statements) throws SQLException {
        try (Statement stmt = controlConnection().createStatement()) {
            for (String sql : statements) {
                stmt.execute(sql);
            }
        }
    }

    /**
     * Strips a trailing {@code ?query=string} from a JDBC URL so the dataset resource is a clean
     * {@code jdbc:vendor://host:port/db} URL. On {@code main} the {@code ExternalSourceResolver} treats {@code '?'} in
     * the {@code StoragePath} path component as a glob metacharacter, so a query-string-bearing URL would be
     * misclassified as a glob and rejected before reaching the JDBC connector. testcontainers' {@code getJdbcUrl()}
     * appends a {@code ?loggerLevel=OFF} query string, so the container-backed fixtures route it through here; the
     * control connection this fixture opens for DDL still works against the clean URL (the query string only tuned
     * driver logging).
     */
    protected static String stripQueryString(String url) {
        if (url == null) {
            return null;
        }
        int q = url.indexOf('?');
        return q < 0 ? url : url.substring(0, q);
    }

    /** The keep-alive control connection; only valid between {@link #start()} and {@link #stop()}. */
    protected final Connection controlConnection() {
        if (controlConnection == null) {
            throw new IllegalStateException("fixture not started; call start() before running DDL/DML");
        }
        return controlConnection;
    }

    @Override
    public final void close() throws Exception {
        stop();
    }
}
