/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * A {@link JdbcDatabaseFixture} that stands in for Amazon Redshift using a real {@code postgres:16.4} container
 * container. There is no local Redshift and no anonymous public endpoint, but Redshift is a Postgres fork, so
 * a Postgres backend is a faithful stand-in for everything
 * {@link org.elasticsearch.xpack.esql.datasource.jdbc.RedshiftDialect} <em>inherits</em> from {@code PostgresDialect}.
 * This fixture's job is to make the connector traverse the whole {@code jdbc:redshift://} production path against that
 * backend, so the shared correctness matrix proves the inherited path is unbroken and that RedshiftDialect's deltas
 * (dropping the session {@code statement_timeout}, using {@code SET timezone TO 'UTC'}) are harmless on a real server.
 * <p>
 * <b>How the Redshift scheme reaches a Postgres backend.</b> {@link #esqlJdbcUrl()} returns a {@code jdbc:redshift://}
 * URL (the container's pgjdbc URL with only the scheme swapped). That URL flows through the REAL production machinery:
 * the {@code SsrfGuard} allowlist (which now contains {@code jdbc:redshift://}), the {@code DialectRegistry} (which
 * resolves it to {@code RedshiftDialect}), and the {@code JdbcDriverRegistry}'s {@code acceptsURL} dispatch. The only
 * test-only artifact is {@link RedshiftStandinDriver}, which claims {@code jdbc:redshift://} and delegates to pgjdbc by
 * swapping the scheme back — exactly the "user supplies a Redshift driver in {@code drivers/}" production model, but
 * pointed at Postgres. Redshift-specific type refusals ({@code SUPER}/{@code VARBYTE}/{@code GEOMETRY}) are not
 * expressible on a Postgres backend, so they are covered by {@code RedshiftDialectTests} rather than here.
 * <p>
 * <b>DriverManager registration.</b> The fixture's own keep-alive control connection is opened via
 * {@link JdbcDatabaseFixture#newConnection()} → {@link DriverManager}, so a {@link RedshiftStandinDriver} instance is
 * registered with {@code DriverManager} in a static initializer (scoped to this suite — the class only loads here).
 * The in-cluster connector does NOT use {@code DriverManager}; it finds the same driver via {@code ServiceLoader}
 * (the {@code META-INF/services/java.sql.Driver} entry).
 * <p>
 * <b>One shared container, many tables (reference-counted).</b> Identical rationale/teardown discipline to
 * {@link PostgresFixture}: the shared matrix builds one fixture per table, so all tables
 * live in one container behind one URL, differing only by the {@code table} WITH option; put-before-start publishing
 * and once-only decrement keep teardown leak-free.
 */
public final class RedshiftStandinFixture extends JdbcDatabaseFixture {

    static {
        // Register the stand-in driver with DriverManager so the fixture's control connection (opened via
        // DriverManager) can resolve the jdbc:redshift:// URL. Scoped to this suite: the class only loads here.
        try {
            DriverManager.registerDriver(new RedshiftStandinDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final DockerImageName IMAGE = DockerImageName.parse("postgres:16.4");

    private static final String CONTAINER_PASSWORD = "redshift_standin_pw_9c1f";

    /** Guards {@link #container} and {@link #refCount}: instances are created/stopped across the test lifecycle. */
    private static final Object LOCK = new Object();

    /** The single shared container, or {@code null} when no fixture currently holds it. */
    private static PostgreSQLContainer<?> container;

    /** Number of started (not-yet-stopped) fixtures attached to {@link #container}; the container dies at 0. */
    private static int refCount;

    /** Whether this instance incremented {@link #refCount} and so must decrement exactly once in {@link #stopDatabase()}. */
    private boolean counted;

    @Override
    protected void startDatabase() throws Exception {
        synchronized (LOCK) {
            if (container == null) {
                PostgreSQLContainer<?> pending = new PostgreSQLContainer<>(IMAGE).withPassword(CONTAINER_PASSWORD);
                // put-before-start: publish before start() so a partially-started container is still reachable for teardown.
                container = pending;
                try {
                    pending.start();
                } catch (Exception e) {
                    container = null;
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
        }
    }

    @Override
    protected void stopDatabase() {
        synchronized (LOCK) {
            if (counted == false) {
                return;
            }
            counted = false;
            refCount--;
            if (refCount == 0 && container != null) {
                try {
                    container.stop();
                } finally {
                    container = null;
                }
            }
        }
    }

    @Override
    public String esqlJdbcUrl() {
        synchronized (LOCK) {
            if (container == null) {
                throw new IllegalStateException("Redshift stand-in (Postgres) container not started; call start() first");
            }
            // Swap only the scheme: jdbc:postgresql://host:port/db -> jdbc:redshift://host:port/db. The
            // RedshiftStandinDriver reverses this to reach pgjdbc; everything in between sees a genuine Redshift URL.
            // Strip the testcontainers query string first so the dataset resource is a clean URL (main treats '?' in
            // the path as a glob metacharacter and would reject a query-string-bearing URL).
            String pgUrl = stripQueryString(container.getJdbcUrl());
            return "jdbc:redshift://" + pgUrl.substring("jdbc:postgresql://".length());
        }
    }

    @Override
    protected String driverClassName() {
        return RedshiftStandinDriver.class.getName();
    }

    @Override
    protected Properties connectionProperties() {
        Properties props = new Properties();
        synchronized (LOCK) {
            if (container != null) {
                props.setProperty("user", container.getUsername());
                props.setProperty("password", container.getPassword());
            }
        }
        return props;
    }

    @Override
    protected Map<String, String> datasetConfigOverrides() {
        synchronized (LOCK) {
            if (container == null) {
                return Map.of();
            }
            return Map.of("user", container.getUsername(), "password", container.getPassword());
        }
    }
}
