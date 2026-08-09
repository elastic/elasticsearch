/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * Ignores the daemon housekeeping/maintenance threads that HikariCP starts for each per-endpoint connection pool, so
 * the randomized-runner's SUITE-scope thread-leak detector does not flag them for the JDBC integration tests.
 * <p>
 * The pool is an instance-owned field of {@code JdbcDataSourcePlugin} and is deterministically closed on node
 * shutdown via the SPI close hook (unit-proven by {@code JdbcHikariPoolTests}: after
 * {@code JdbcDataSourcePlugin.close()} the underlying {@code HikariDataSource} reports {@code isClosed()}). These
 * threads leak <em>only</em> because the ES integration-test framework reuses the in-JVM test cluster across suites,
 * so the node (and therefore its pool) outlives an individual SUITE-scope leak check -- exactly like the pgjdbc
 * {@code LazyCleaner} thread handled by {@link PostgresTestThreadLeakFilter}.
 * <p>
 * Filtering is by the pool's thread-name prefix only ({@code esql-jdbc[...]}, set via {@code HikariConfig.setPoolName})
 * -- HikariCP names its housekeeper / connection-adder / connection-closer threads {@code "<poolName> ..."}. A genuine
 * connector or node thread leak is still reported.
 */
public final class HikariPoolTestThreadLeakFilter implements ThreadFilter {

    @Override
    public boolean reject(Thread t) {
        String name = t.getName();
        return name != null && name.startsWith("esql-jdbc[");
    }
}
