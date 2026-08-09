/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

/**
 * Actionable classification of a {@link java.sql.SQLException} produced by a JDBC driver, derived from its
 * {@code SQLState} (plus, for a few vendor-specific rollbacks, its vendor error code) by
 * {@link JdbcSqlStateClassifier}.
 * <p>
 * The categories distill 20 years of per-vendor {@code SQLState} knowledge (Spring's {@code SQLExceptionTranslator}
 * + {@code sql-error-codes.xml}, Trino's error mapping, and the JDBC 4.x / SQL:2016 standard state classes) down to
 * the handful of outcomes the connector can actually act on. {@link JdbcConnector} maps two of them
 * ({@link #AUTH_FAILED}, {@link #TRANSIENT_NETWORK}) to a single-shot retry; every other category is a fail-fast
 * that propagates a sanitized error to the operator. The enum intentionally carries no policy itself — the retry
 * decision lives in {@link JdbcConnector} so the classifier stays a pure, side-effect-free categorizer.
 */
public enum JdbcSqlStateCategory {

    /**
     * Invalid authorization: bad user/password or an expired/rejected credential. SQL-standard state class
     * {@code 28} ("invalid authorization specification"); e.g. Postgres {@code 28P01} (invalid_password) /
     * {@code 28000}, MySQL {@code 28000} (ER_ACCESS_DENIED_ERROR, code 1045), H2 {@code 28000}. Retryable exactly
     * once, but only against a credential source that can actually produce a fresh credential (see
     * {@link JdbcConnector} — per-query credentials are not refreshable).
     */
    AUTH_FAILED,

    /**
     * Transient connection-layer failure: connection refused/dropped/reset before or during the exchange.
     * SQL-standard state class {@code 08} ("connection exception"), e.g. {@code 08001}, {@code 08006},
     * {@code 08003}, {@code 08004}, MySQL {@code 08S01} (communication link failure). Retryable exactly once with a
     * fresh pooled connection. <b>Excludes {@code 08P01}</b> (Postgres protocol_violation), which is a
     * non-retryable protocol error classified as {@link #UNKNOWN}.
     */
    TRANSIENT_NETWORK,

    /**
     * Deadlock / serialization rollback. Postgres {@code 40P01} (deadlock_detected) and {@code 40001}
     * (serialization_failure); MySQL InnoDB lock-contention rollbacks code {@code 1213} (ER_LOCK_DEADLOCK) and
     * {@code 1205} (ER_LOCK_WAIT_TIMEOUT). Fail fast: the connector is read-only, so a deadlock is not ours to
     * resolve by retry (a serializable-isolation false positive is the caller's to handle).
     */
    DEADLOCK,

    /**
     * Server-side resource exhaustion: out of memory / disk / connection slots. SQL-standard state class
     * {@code 53} ("insufficient resources"), e.g. Postgres {@code 53000}, {@code 53100} (disk_full),
     * {@code 53200} (out_of_memory), {@code 53300} (too_many_connections). Fail fast with a retry-after hint;
     * an immediate retry would only add load.
     */
    RESOURCE_EXHAUSTED,

    /**
     * SQL syntax or access-rule violation. SQL-standard state class {@code 42}, e.g. Postgres {@code 42601}
     * (syntax_error), {@code 42P01} (undefined_table), MySQL {@code 42000} / {@code 42S02}. Fail fast: this is a
     * bug in our SQL renderer or a mismatch with the target schema, not something a retry fixes. The
     * privilege-specific codes ({@code 42501}, {@code 42P02}) are split out as {@link #PERMISSION}.
     */
    SYNTAX_ERROR,

    /**
     * Data exception: bad value conversion, numeric out-of-range, invalid text representation. SQL-standard state
     * class {@code 22}, e.g. Postgres {@code 22P02} (invalid_text_representation), {@code 22003}
     * (numeric_value_out_of_range), {@code 22007} (invalid_datetime_format). Fail fast: this points at a dialect
     * type-mapping bug (a narrowing that should not have happened), not a transient condition.
     */
    DATA_ERROR,

    /**
     * Integrity constraint violation. SQL-standard state class {@code 23}, e.g. Postgres {@code 23505}
     * (unique_violation), {@code 23503} (foreign_key_violation), MySQL {@code 23000}. Fail fast: the connector is
     * read-only, so surfacing a constraint violation means something is very wrong upstream.
     */
    INTEGRITY_VIOLATION,

    /**
     * Insufficient privilege for the requested object/operation. Postgres {@code 42501} (insufficient_privilege)
     * and {@code 42P02}. Fail fast — a retry with the same identity cannot succeed; the operator must grant access.
     */
    PERMISSION,

    /**
     * Query cancelled by user/admin request. Postgres {@code 57014} (query_canceled), state class {@code 57}
     * ("operator intervention"). Distinguished from a generic failure so cursor teardown during a cancel is not
     * logged as an error and so the caller can tell "you asked for this" apart from "the DB broke".
     */
    CANCELLED_BY_USER,

    /**
     * Anything else, including a {@code null}/blank {@code SQLState}, an unrecognized vendor code, and the
     * deliberately-excluded {@code 08P01} protocol error. Fail fast.
     */
    UNKNOWN
}
