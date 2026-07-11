/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.xpack.esql.core.type.DataType;

import java.sql.Types;
import java.util.List;
import java.util.Set;

/**
 * PostgreSQL {@link JdbcDialect}. Extends {@link GenericDialect} and overrides only what Postgres genuinely needs;
 * everything not listed under DIFFERENCES below is deliberately inherited unchanged.
 *
 * <h2>Value</h2>
 * The one headline correctness win over {@link GenericDialect} is that an exact integer-scale {@code NUMERIC} (a
 * common shape for big-integer surrogate keys) round-trips <em>exactly</em> as an ES|QL {@code LONG} instead of being
 * silently mangled through {@code DOUBLE}. A {@code NUMERIC(18,0)} holding {@code 9007199254740993} (2^53 + 1) is not
 * representable as an IEEE-754 double; {@link GenericDialect} maps every {@code NUMERIC} to {@code DOUBLE} and loses
 * the {@code +1}, whereas this dialect maps it to {@code LONG} and preserves it. The rest of the class is about making
 * Postgres sessions <em>deterministic and disciplined</em> (UTC, server-side timeout, verified-version WARN) rather
 * than about adding new type coverage -- native {@code BOOLEAN}, {@code TEXT}/{@code VARCHAR}, {@code BIGINT},
 * {@code TIMESTAMP}/{@code TIMESTAMPTZ}, {@code SERIAL} already map correctly through the inherited generic mapping.
 *
 * <h2>Differences from {@link GenericDialect}</h2>
 * <ul>
 *   <li><b>{@link #name()} = {@code "postgresql"}</b> -- observability only (the connector's query-start line and the
 *       version WARN carry it). Never parsed.</li>
 *   <li><b>{@link #initStatements()} = {@code ["SET TIME ZONE 'UTC'", "SET statement_timeout = '300000'"]}</b> --
 *       run once per connection right after {@link #configureConnection}. Two distinct, verified-on-Postgres-16.4
 *       reasons:
 *       <ol>
 *         <li><b>{@code SET TIME ZONE 'UTC'}</b> pins the session time zone so a naive {@code TIMESTAMP} (without
 *             time zone) is interpreted deterministically regardless of the server's configured {@code TimeZone} --
 *             a correctness guard, since the same stored value would otherwise shift by the server's offset.</li>
 *         <li><b>{@code SET statement_timeout = '300000'}</b> (milliseconds; resolves to 5&nbsp;min) is a
 *             <em>server-side</em> cap that complements the JDBC-side {@link #configureStatement} {@code setQueryTimeout(300)}.
 *             Belt-and-suspenders: if the client-side timeout ever fails to fire (driver quirk, cancel lost), the
 *             server still aborts a runaway query. Kept equal to the JDBC-side 300&nbsp;s so the two agree.</li>
 *       </ol>
 *       A failure executing either statement is a hard, sanitized failure of the connection open (a half-configured
 *       session must not serve a query).</li>
 *   <li><b>{@link #supportedDatabaseMajorVersions()} = {@code {12,13,14,15,16}}</b> -- the majors this connector has
 *       been verified against. The connector WARNs once per URL when a server reports a major outside this set, so a
 *       silent behavior change on an unverified major (older EOL server, or a newer major we have not vetted) is
 *       surfaced without failing the query.</li>
 *   <li><b>{@link #mapJdbcType} {@code NUMERIC}/{@code DECIMAL} scoping</b> -- see Value above and the method javadoc.
 *       {@code scale == 0 && 1 <= precision <= 18} maps to {@code LONG}; every other {@code NUMERIC}/{@code DECIMAL}
 *       (scale &gt; 0, precision &gt; 18, or unconstrained precision 0) stays {@code DOUBLE} exactly as the generic
 *       dialect had it. All other JDBC types delegate to {@code super}.</li>
 * </ul>
 *
 * <h2>Deliberately inherited (NOT overridden)</h2>
 * <ul>
 *   <li><b>{@link #quoteIdentifier}</b> -- Postgres uses ANSI double-quote identifiers, identical to
 *       {@link GenericDialect}. Overriding just to "be explicit" would be noise.</li>
 *   <li><b>{@link #configureStatement}</b> -- {@code fetchSize=10000} + {@code queryTimeout=300s} is exactly what
 *       pgjdbc needs for server-side cursor streaming (a positive fetch size, combined with the read-only connection
 *       below, drives pgjdbc's cursor mode), so the generic values are correct as-is.</li>
 *   <li><b>{@link #configureConnection}</b> -- {@code setReadOnly(true)} suffices; pgjdbc streams a forward-only,
 *       read-only result set at the configured fetch size without needing {@code autoCommit=false} to be toggled
 *       here for our single-statement read path.</li>
 *   <li><b>{@link #bindParam}</b> -- inherits the new {@code DATETIME}-aware default ({@code Timestamp.from(Instant)}),
 *       which is exactly the coercion pgjdbc wants. No Postgres-specific override is added speculatively; one would be
 *       introduced only if a round-trip test proved pgjdbc needs it.</li>
 * </ul>
 *
 * <h2>Refused types (fall through to {@code null} = skip column with WARN)</h2>
 * Postgres {@code ARRAY} (pgjdbc {@code Types.ARRAY}) and {@code JSON}/{@code JSONB}/{@code INTERVAL}/geometric
 * {@code POINT} (all reported by pgjdbc as {@code Types.OTHER}) are <em>not</em> mapped: they hit
 * {@code GenericDialect}'s {@code default -> null} and the column is skipped with a WARN rather than mis-mapped. This
 * is intentional -- ES|QL has no equivalent type -- and is covered end-to-end by {@code PostgresJdbcIT}.
 * <p>
 * Note that {@code NUMERIC}/{@code DECIMAL} is <em>never</em> refused: the scoped rule above maps it to {@code LONG}
 * or {@code DOUBLE} in every case (an oversized precision, a non-zero scale, or an unconstrained {@code NUMERIC}
 * falls back to {@code DOUBLE}, not to the refused {@code null} path). Only the types listed here are skipped.
 */
public class PostgresDialect extends GenericDialect {

    public static final PostgresDialect INSTANCE = new PostgresDialect();

    /** Majors verified for this connector; drives the once-per-URL version WARN in {@link JdbcConnector}. */
    private static final Set<Integer> SUPPORTED_MAJOR_VERSIONS = Set.of(12, 13, 14, 15, 16);

    /**
     * Per-connection session setup. {@code statement_timeout} is expressed in milliseconds (300000 = 5 min), matching
     * the JDBC-side {@code setQueryTimeout(300)}. Order matters only in that both must succeed; they are independent.
     */
    private static final List<String> INIT_STATEMENTS = List.of("SET TIME ZONE 'UTC'", "SET statement_timeout = '300000'");

    @Override
    public String name() {
        return "postgresql";
    }

    @Override
    public List<String> initStatements() {
        return INIT_STATEMENTS;
    }

    @Override
    public Set<Integer> supportedDatabaseMajorVersions() {
        return SUPPORTED_MAJOR_VERSIONS;
    }

    /**
     * Postgres-scoped {@code NUMERIC}/{@code DECIMAL} mapping (precision = {@code columnSize}, scale =
     * {@code decimalDigits}):
     * <ul>
     *   <li>{@code scale == 0 && 1 <= precision <= 18} → {@code LONG}. Exact: 10^18 &lt; 2^63, so every value fits a
     *       signed 64-bit long, fixing the big-integer-key case where {@code DOUBLE} loses precision past 2^53.</li>
     *   <li>otherwise (scale &gt; 0, precision &gt; 18, or unconstrained precision 0) → {@code DOUBLE}. Unchanged from
     *       {@link GenericDialect}: ES|QL has no arbitrary-precision decimal, so we do not attempt &gt;18-digit or
     *       high-scale exactness. This stays the honest approximate fallback -- we deliberately do NOT force
     *       {@code NUMERIC(38,0)} to {@code LONG} (would overflow) or introduce a {@code BigDecimal} path.</li>
     * </ul>
     * Every other JDBC type delegates to {@link GenericDialect#mapJdbcType} unchanged.
     */
    @Override
    public DataType mapJdbcType(int jdbcType, int columnSize, int decimalDigits) {
        if (jdbcType == Types.NUMERIC || jdbcType == Types.DECIMAL) {
            if (decimalDigits == 0 && columnSize >= 1 && columnSize <= 18) {
                return DataType.LONG;
            }
            return DataType.DOUBLE;
        }
        return super.mapJdbcType(jdbcType, columnSize, decimalDigits);
    }
}
