/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Maps a {@link SQLException} to an actionable {@link JdbcSqlStateCategory} using its {@code SQLState} (and, for a
 * few vendor-specific rollbacks, its vendor error code). Pure and side-effect free.
 * <p>
 * <b>SQLState is the primary signal.</b> The two-character class prefix of a {@code SQLState} is standardized by
 * SQL:2016 (and JDBC 4.x recommends drivers use the "X/Open SQLState" or "SQL:2003" convention). The full-code
 * carve-outs and vendor codes below are sourced as follows:
 * <ul>
 *   <li>PostgreSQL error codes: PostgreSQL manual, Appendix A "PostgreSQL Error Codes"
 *       (https://www.postgresql.org/docs/current/errcodes-appendix.html). Amazon Redshift is Postgres-derived and
 *       reuses the same {@code SQLState}s (Redshift docs, "Serializable isolation" / error handling).</li>
 *   <li>MySQL vendor codes: MySQL Server Error Reference
 *       (https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html) — {@code 1213}
 *       (ER_LOCK_DEADLOCK) and {@code 1205} (ER_LOCK_WAIT_TIMEOUT), both InnoDB lock-contention rollbacks; MySQL
 *       reports {@code SQLState 40001} for {@code 1213} but the vendor-code check is kept for driver-version
 *       robustness. MySQL auth failure {@code 1045} maps to {@code SQLState 28000}.</li>
 *   <li>Category grouping mirrors Spring JDBC's {@code SQLExceptionSubclassTranslator} /
 *       {@code sql-error-codes.xml} state-class conventions.</li>
 * </ul>
 * <p>
 * <b>Cause-chain walking.</b> Drivers (and the framework's async producer path) frequently wrap the real
 * {@link SQLException} inside a runtime exception, or chain multiple {@link SQLException}s via
 * {@link SQLException#getNextException()}. {@link #classify(Throwable)} therefore walks both {@code getCause()} and
 * {@code getNextException()}, outermost-first, and returns the category of the first node whose {@code SQLState}
 * (or vendor code) maps to a recognized (non-{@link JdbcSqlStateCategory#UNKNOWN UNKNOWN}) category. A node with a
 * {@code null}/blank {@code SQLState} and no recognized vendor code contributes nothing and the walk continues.
 * If nothing in the chain is recognized, the result is {@link JdbcSqlStateCategory#UNKNOWN}. Cycles (a driver that
 * sets {@code e.getNextException() == e}) and pathological depth are bounded by an identity-visited set.
 */
public final class JdbcSqlStateClassifier {

    /** Defensive cap so a maliciously/accidentally deep chain cannot spin the CPU. Real chains are &lt; 5 deep. */
    private static final int MAX_CHAIN_DEPTH = 64;

    private JdbcSqlStateClassifier() {}

    /**
     * Classifies a {@link SQLException} (walking its cause / {@code getNextException} chain). Convenience overload
     * for the common case where the caught type is already a {@link SQLException}.
     */
    public static JdbcSqlStateCategory classify(SQLException e) {
        return classify((Throwable) e);
    }

    /**
     * Classifies an arbitrary {@link Throwable} by locating {@link SQLException}s anywhere in its cause /
     * {@code getNextException} chain. Handles the "runtime exception wrapping a SQLException" shape some drivers
     * produce. A {@code null} argument classifies as {@link JdbcSqlStateCategory#UNKNOWN}.
     */
    public static JdbcSqlStateCategory classify(Throwable t) {
        if (t == null) {
            return JdbcSqlStateCategory.UNKNOWN;
        }
        // Breadth-first walk over both the getCause() and SQLException.getNextException() edges, so the recognized
        // category NEAREST to the top (outermost) wins. A shared identity-visited set makes the walk cycle-safe even
        // for mutually-referential chains (e1.next==e2, e2.next==e1) or a self-referential one (e.next==e) that some
        // drivers produce; MAX_CHAIN_DEPTH bounds the total nodes examined as a second belt.
        Map<Throwable, Boolean> visited = new IdentityHashMap<>();
        Deque<Throwable> queue = new ArrayDeque<>();
        queue.add(t);
        visited.put(t, Boolean.TRUE);
        int examined = 0;
        Throwable node;
        while ((node = queue.poll()) != null && examined++ < MAX_CHAIN_DEPTH) {
            if (node instanceof SQLException sqlException) {
                JdbcSqlStateCategory category = classifyOne(sqlException);
                if (category != JdbcSqlStateCategory.UNKNOWN) {
                    return category;
                }
                enqueue(sqlException.getNextException(), queue, visited);
            }
            enqueue(node.getCause(), queue, visited);
        }
        return JdbcSqlStateCategory.UNKNOWN;
    }

    private static void enqueue(Throwable t, Deque<Throwable> queue, Map<Throwable, Boolean> visited) {
        if (t != null && visited.put(t, Boolean.TRUE) == null) {
            queue.add(t);
        }
    }

    /**
     * Classifies a single {@link SQLException} by its own {@code SQLState}/vendor code only (no chain walking).
     * Returns {@link JdbcSqlStateCategory#UNKNOWN} when this node carries no recognized signal, which tells the
     * chain walker to keep looking.
     */
    private static JdbcSqlStateCategory classifyOne(SQLException e) {
        String sqlState = e.getSQLState();
        if (sqlState != null) {
            String state = sqlState.trim().toUpperCase(Locale.ROOT);
            if (state.isEmpty() == false) {
                JdbcSqlStateCategory byState = fromSqlState(state);
                if (byState != JdbcSqlStateCategory.UNKNOWN) {
                    return byState;
                }
            }
        }
        // Fall back to vendor error code only when SQLState gave us nothing recognized, so a recognized SQLState is
        // never overridden by a coincidental vendor-code collision from a different vendor.
        return fromVendorCode(e.getErrorCode());
    }

    /** SQLState → category. {@code state} is already trimmed + upper-cased and non-empty. */
    private static JdbcSqlStateCategory fromSqlState(String state) {
        // Full-code carve-outs first: they are more specific than the two-character class prefix below.

        // Postgres protocol_violation: a connection-class code that a retry will NOT fix -> deliberately NOT
        // TRANSIENT_NETWORK. Must be checked before the "08" prefix.
        if (state.equals("08P01")) {
            return JdbcSqlStateCategory.UNKNOWN;
        }
        // Deadlock / serialization rollback (Postgres 40P01 deadlock_detected, 40001 serialization_failure). Only
        // these two of state class 40 are deadlocks; other class-40 rollbacks are not mapped here.
        if (state.equals("40001") || state.equals("40P01")) {
            return JdbcSqlStateCategory.DEADLOCK;
        }
        // Query cancellation (Postgres 57014 query_canceled) — a specific member of class 57.
        if (state.equals("57014")) {
            return JdbcSqlStateCategory.CANCELLED_BY_USER;
        }
        // Insufficient privilege split out of class 42 (Postgres 42501 insufficient_privilege, 42P02). Note: per the
        // PostgreSQL error-code appendix 42P02 is actually "undefined_parameter"; it is grouped under PERMISSION
        // here under the PERMISSION category (both are non-retryable, fail-fast anyway).
        if (state.equals("42501") || state.equals("42P02")) {
            return JdbcSqlStateCategory.PERMISSION;
        }

        // Two-character SQL:2016 state-class prefixes.
        String prefix = state.length() >= 2 ? state.substring(0, 2) : state;
        return switch (prefix) {
            case "28" -> JdbcSqlStateCategory.AUTH_FAILED;            // invalid authorization specification
            case "08" -> JdbcSqlStateCategory.TRANSIENT_NETWORK;     // connection exception (08P01 handled above)
            case "53" -> JdbcSqlStateCategory.RESOURCE_EXHAUSTED;    // insufficient resources
            case "42" -> JdbcSqlStateCategory.SYNTAX_ERROR;          // syntax error / access rule (42501/42P02 above)
            case "22" -> JdbcSqlStateCategory.DATA_ERROR;            // data exception
            case "23" -> JdbcSqlStateCategory.INTEGRITY_VIOLATION;   // integrity constraint violation
            default -> JdbcSqlStateCategory.UNKNOWN;
        };
    }

    /**
     * Vendor error code → category, consulted only when {@code SQLState} yielded nothing recognized. Currently
     * covers MySQL InnoDB lock-contention rollbacks (see MySQL Server Error Reference): {@code 1213}
     * (ER_LOCK_DEADLOCK) and {@code 1205} (ER_LOCK_WAIT_TIMEOUT). Both are read-only-connector fail-fast
     * (DEADLOCK). {@code 0} (the default when a driver sets no vendor code) maps to {@link JdbcSqlStateCategory#UNKNOWN}.
     */
    private static JdbcSqlStateCategory fromVendorCode(int vendorCode) {
        return switch (vendorCode) {
            case 1213, 1205 -> JdbcSqlStateCategory.DEADLOCK;
            default -> JdbcSqlStateCategory.UNKNOWN;
        };
    }
}
