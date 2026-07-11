/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.xpack.esql.core.type.DataType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Set;

/**
 * Vendor-specific behaviors required by the ESQL JDBC connector. {@link GenericDialect} is the ANSI fallback; vendor
 * dialects (Postgres, MySQL, Snowflake) extend it ({@link PostgresDialect} is the first).
 * <p>
 * The interface is deliberately narrow: identifier quoting (the only safe defense against injection through table /
 * column names since they cannot be parameterized), JDBC-to-ESQL type mapping (returns {@code null} for unsupported
 * types so the caller can skip with a WARN rather than fail the query), per-statement / per-connection configuration
 * hooks, a parameter-binding hook reserved for filter pushdown, and -- for the vendor
 * dialects -- a per-connection init-SQL hook plus lightweight observability/version-discipline metadata.
 * <p>
 * Implementations MUST reject identifiers that would break out of the dialect's quoting -- e.g. those containing the
 * dialect's own quote character -- by throwing {@link IllegalArgumentException}.
 */
public interface JdbcDialect {

    String quoteIdentifier(String identifier);

    DataType mapJdbcType(int jdbcType, int columnSize, int decimalDigits);

    default void configureStatement(java.sql.Statement statement) throws SQLException {
        statement.setFetchSize(10_000);
        statement.setQueryTimeout(300);
    }

    default void configureConnection(Connection connection) throws SQLException {
        connection.setReadOnly(true);
    }

    /**
     * SQL statements executed once per connection, immediately after {@link #configureConnection}, on a scratch
     * {@link java.sql.Statement}. Used to establish deterministic session state that has no JDBC-API equivalent (e.g.
     * {@code SET TIME ZONE 'UTC'} on Postgres). A failure executing any of these is a hard, sanitized failure of the
     * query's connection open -- the connection is unusable in a half-configured state. The default is empty, so a
     * plain {@link GenericDialect} connection is left exactly as the driver hands it back.
     */
    default List<String> initStatements() {
        return List.of();
    }

    /**
     * Short, stable dialect identifier used only for observability (the {@code JdbcConnector} query-start line and the
     * database-version WARN). Never parsed. Defaults to {@code "generic"}.
     */
    default String name() {
        return "generic";
    }

    /**
     * The database major versions this dialect has been verified against. An empty set (the default) means "no version
     * discipline" -- the connector will not warn regardless of the server version. A vendor dialect returns the exact
     * majors it was tested with; the connector then logs a WARN (once per URL) when a server reports a major outside
     * this set, so a silent behavior change on an unverified major is surfaced without failing the query.
     */
    default Set<Integer> supportedDatabaseMajorVersions() {
        return Set.of();
    }

    /**
     * Binds one pushed-down filter parameter. The default coerces ESQL's temporal type {@code DATETIME} to a
     * {@link Timestamp} built from the {@link Instant}, because driver handling of a bare {@link Instant} via
     * {@code setObject} varies subtly across versions (pgjdbc in particular). Every other type, and {@code null},
     * falls back to {@code setObject}, which is correct for the numeric / boolean / string types we push.
     * <p>
     * Note there is deliberately no separate {@code DATE} branch: ESQL has no standalone {@code DATE}
     * {@link DataType} (SQL {@code DATE}/{@code TIME}/{@code TIMESTAMP} all map to {@code DATETIME} in
     * {@link GenericDialect#mapJdbcType}), and {@code DATETIME} is the only temporal type in the pushdown
     * {@code SUPPORTED_TYPES} set, so a {@code Date.valueOf((LocalDate) value)} branch would be dead code. Vendor
     * dialects override only for a driver-specific quirk the default does not cover.
     */
    default void bindParam(PreparedStatement stmt, int paramIndex, Object value, DataType esqlType) throws SQLException {
        if (value == null) {
            stmt.setObject(paramIndex, null);
        } else if (esqlType == DataType.DATETIME) {
            stmt.setTimestamp(paramIndex, Timestamp.from((Instant) value));
        } else {
            stmt.setObject(paramIndex, value);
        }
    }
}
