/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import com.carrotsearch.randomizedtesting.ThreadFilter;

import java.util.Locale;

/**
 * Ignores the JVM-lifetime daemon threads that the PostgreSQL JDBC driver and testcontainers start, so the
 * randomized-runner's SUITE-scope thread-leak detector does not flag them for {@code PostgresJdbcIT}.
 * <p>
 * These threads are legitimately shared and long-lived, and are <em>not</em> owned by any single test:
 * <ul>
 *   <li>{@code PostgreSQL-JDBC-Cleaner} — the pgjdbc {@code org.postgresql.util.LazyCleaner} reference-cleaner
 *       daemon, started lazily on first driver use and parked for the life of the JVM.</li>
 *   <li>{@code PostgreSQL-JDBC-SharedTimer-*} — the pgjdbc {@code org.postgresql.Driver} shared {@code Timer} that
 *       backs statement/login timeouts and {@code CopyManager} bookkeeping; created lazily on first timer use (the
 *       perf suite's {@code COPY} bulk load triggers it) and parked for the life of the JVM. Both are matched by the
 *       common {@code postgresql-jdbc} name prefix below.</li>
 *   <li>{@code testcontainers-*} / {@code ducttape-*} / {@code docker-java-*} — testcontainers' Docker client and
 *       its polling/lifecycle helpers. The container itself is stopped deterministically by the fixture; these are
 *       the client-side pools that outlive an individual container.</li>
 * </ul>
 * Filtering is by thread-name prefix only (never by ignoring all leaks), so a genuine leak from the connector or the
 * ES node is still reported.
 */
public final class PostgresTestThreadLeakFilter implements ThreadFilter {

    @Override
    public boolean reject(Thread t) {
        String name = t.getName();
        if (name == null) {
            return false;
        }
        String lower = name.toLowerCase(Locale.ROOT);
        return lower.startsWith("postgresql-jdbc")
            || lower.startsWith("testcontainers")
            || lower.startsWith("ducttape")
            || lower.startsWith("docker-java");
    }
}
