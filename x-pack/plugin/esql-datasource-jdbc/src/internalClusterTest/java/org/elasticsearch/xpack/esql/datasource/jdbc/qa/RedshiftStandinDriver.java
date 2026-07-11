/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Test-only {@link Driver} that accepts the {@code jdbc:redshift://} scheme and serves it by rewriting the scheme to
 * {@code jdbc:postgresql://} and delegating to the real pgjdbc {@link org.postgresql.Driver}. It exists SOLELY to let
 * {@code RedshiftDialectStandinIT} exercise the whole production path for the Redshift scheme — the SSRF allowlist
 * entry, the {@code DialectRegistry} → {@link org.elasticsearch.xpack.esql.datasource.jdbc.RedshiftDialect} dispatch,
 * and the driver-registry {@code acceptsURL} dispatch — against a real {@code postgres:16.4} backend, since there is
 * no local Amazon Redshift and the production model is a user-supplied Redshift driver in the {@code drivers/} dir.
 * <p>
 * This is analogous to reusing pgjdbc for a non-Postgres pg-wire store: Redshift is a
 * Postgres fork, so a real Postgres backend is a faithful stand-in for everything {@link
 * org.elasticsearch.xpack.esql.datasource.jdbc.RedshiftDialect} inherits from {@code PostgresDialect}. The rewrite is
 * a pure scheme swap (both schemes are authority-based {@code jdbc:<scheme>://host:port/db?query}), so host/port/db
 * and any query string are preserved verbatim.
 * <p>
 * It is discovered two ways: (1) the in-cluster connector's {@code JdbcDriverRegistry} finds it via
 * {@link java.util.ServiceLoader} (the {@code META-INF/services/java.sql.Driver} entry in this source set), and (2)
 * the fixture's own control connection reaches it through {@link java.sql.DriverManager} after
 * {@code RedshiftStandinFixture} registers an instance. It deliberately does NOT self-register in a static block, so
 * merely being on the classpath of the other JDBC IT suites (H2/Postgres) has no effect there — it only
 * ever claims {@code jdbc:redshift://} URLs, which those suites never use.
 */
public final class RedshiftStandinDriver implements Driver {

    private static final String REDSHIFT_PREFIX = "jdbc:redshift://";
    private static final String POSTGRES_PREFIX = "jdbc:postgresql://";

    private final org.postgresql.Driver delegate = new org.postgresql.Driver();

    public RedshiftStandinDriver() {}

    /** Returns the pgjdbc URL for a {@code jdbc:redshift://} URL, or {@code null} if the URL is not a Redshift URL. */
    private static String toPostgresUrl(String url) {
        if (url == null) {
            return null;
        }
        if (url.regionMatches(true, 0, REDSHIFT_PREFIX, 0, REDSHIFT_PREFIX.length())) {
            return POSTGRES_PREFIX + url.substring(REDSHIFT_PREFIX.length());
        }
        return null;
    }

    @Override
    public boolean acceptsURL(String url) {
        // Never throws: mirror the driver contract (false for URLs this driver does not handle).
        return url != null && url.toLowerCase(Locale.ROOT).startsWith(REDSHIFT_PREFIX);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        String pg = toPostgresUrl(url);
        // Per the Driver contract, return null (not throw) when the URL is not ours, so DriverManager can try others.
        return pg == null ? null : delegate.connect(pg, info);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        String pg = toPostgresUrl(url);
        return delegate.getPropertyInfo(pg == null ? url : pg, info);
    }

    @Override
    public int getMajorVersion() {
        return delegate.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return delegate.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        // pgjdbc reports false; the stand-in is not claiming full JDBC compliance either.
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        // Delegate to pgjdbc rather than allocating our own java.util.logging tree; the stand-in adds no logging.
        return delegate.getParentLogger();
    }
}
