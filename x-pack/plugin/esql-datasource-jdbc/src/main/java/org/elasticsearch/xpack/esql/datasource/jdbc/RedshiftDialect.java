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
 * Amazon Redshift {@link JdbcDialect}. Extends {@link PostgresDialect} because Redshift is a Postgres fork: its wire
 * protocol, identifier quoting, session/read-path handling, and the {@code NUMERIC} scoping are all identical, so the
 * overwhelming majority of behaviour is <em>inherited unchanged</em>. This class overrides only the genuine, small
 * Redshift deltas — the tininess is the point (it confirms the Stage-3 research thesis that Redshift is "the one real
 * but small dialect delta" on top of Postgres). It is reached only through the dedicated {@code jdbc:redshift://} URL
 * prefix ({@link DialectRegistry}); a Redshift cluster reached via plain pgjdbc + {@code jdbc:postgresql://} resolves
 * to {@link PostgresDialect} instead and is served by the inherited Postgres behaviour (which works for user/password
 * connections — see the product-name advisory in {@link JdbcConnector} for why that path cannot auto-detect Redshift).
 *
 * <h2>Deltas from {@link PostgresDialect}</h2>
 * <ul>
 *   <li><b>{@link #name()} = {@code "redshift"}</b> — observability only (query-start line + advisory). Never parsed.</li>
 *   <li><b>{@link #initStatements()} = {@code ["SET timezone TO 'UTC'"]}</b> — two deltas from Postgres:
 *       <ol>
 *         <li><b>DROP {@code SET statement_timeout}.</b> Redshift manages query time limits through WLM (workload
 *             management) queue/QMR rules, not a session {@code statement_timeout} the way Postgres does. While
 *             Redshift does accept a session {@code SET statement_timeout} (it is one of the times folded into the
 *             overall limit; see the Redshift {@code statement_timeout} reference), the canonical Redshift mechanism
 *             is WLM, so this dialect does not set a session statement timeout and relies on the JDBC-side
 *             {@code setQueryTimeout(300)} (inherited via {@link #configureStatement}) plus the operator's WLM
 *             configuration. This keeps the connector from fighting the cluster's WLM policy.</li>
 *         <li><b>{@code SET timezone TO 'UTC'} (not Postgres' {@code SET TIME ZONE 'UTC'}).</b> The
 *             <em>documented</em> Redshift syntax for pinning the session zone is {@code SET timezone { TO | = }
 *             value} (one word {@code timezone}); the {@code SET time zone <value>} form (two words, no {@code TO})
 *             is documented to accept an {@code INTERVAL} and is a different grammar branch. We therefore use the
 *             unambiguous {@code SET timezone TO 'UTC'} spelling. (It is also valid Postgres — {@code timezone} is a
 *             standard GUC — which is what lets the Postgres-standin IT force this dialect against a real Postgres
 *             backend.) Reference: Amazon Redshift Database Developer Guide, "timezone" configuration parameter.</li>
 *       </ol>
 *       Keeping the UTC pin preserves the same naive-{@code TIMESTAMP} determinism {@link PostgresDialect} documents.
 *       A failure executing it is a hard, sanitized failure of the connection open, exactly as for Postgres.</li>
 *   <li><b>{@link #supportedDatabaseMajorVersions()} = {@code {}} (empty).</b> Redshift's version numbering is opaque
 *       and Amazon-internal (patch strings like {@code 1.0.xxxxx}, not a stable major line like Postgres 12–16), so
 *       there is no meaningful "verified major" set to check. An empty set disables the once-per-URL version WARN in
 *       {@link JdbcConnector#checkDatabaseVersion} (same contract {@link GenericDialect} uses), rather than warning
 *       spuriously on every Redshift connection.</li>
 *   <li><b>{@link #mapJdbcType} refuses Redshift {@code SUPER}.</b> See the method javadoc + "Refused types" below.</li>
 * </ul>
 *
 * <h2>Deliberately inherited from {@link PostgresDialect} (NOT overridden)</h2>
 * <ul>
 *   <li><b>{@code NUMERIC}/{@code DECIMAL} scoping</b> — {@code scale == 0 && 1 <= precision <= 18} → {@code LONG},
 *       otherwise {@code DOUBLE}. Redshift caps {@code NUMERIC} at 38 digits (128-bit); the inherited rule already
 *       lives strictly inside that envelope (it only promotes to {@code LONG} at ≤ 18 digits), so no Redshift-specific
 *       adjustment is needed — a happy consequence of the ES|QL-scoped mapping.</li>
 *   <li><b>{@link #quoteIdentifier} / {@link #configureConnection} / {@link #configureStatement} / {@link #bindParam}</b>
 *       — all identical to Postgres; Redshift speaks the same wire protocol and cursor semantics.</li>
 * </ul>
 *
 * <h2>Refused types (fall through to {@code null} = skip column with WARN)</h2>
 * Redshift's non-relational types have no ES|QL equivalent and are refused, but the refusal is split by <em>how</em>
 * the Redshift JDBC driver reports each one through {@code DatabaseMetaData.getColumns()} {@code DATA_TYPE} (verified
 * against {@code com.amazon.redshift.jdbc.RedshiftDatabaseMetaData}, driver 2.1.0.x):
 * <ul>
 *   <li><b>{@code SUPER} (semi-structured PartiQL, 16&nbsp;MB) — EXPLICIT override.</b> The driver reports it as
 *       {@code DATA_TYPE = -16} ({@link Types#LONGNVARCHAR}). {@link GenericDialect} maps {@code LONGNVARCHAR} →
 *       {@code KEYWORD}, which would silently mis-represent a {@code SUPER} document as a plain string. To prevent
 *       that mis-mapping this dialect overrides {@link #mapJdbcType} to return {@code null} for {@code LONGNVARCHAR}
 *       (Redshift has no representable type that reports {@code LONGNVARCHAR} other than {@code SUPER}, so refusing
 *       the whole code is exact for Redshift). This is the one case that needs a code override rather than the
 *       inherited default.</li>
 *   <li><b>{@code VARBYTE} (binary), {@code GEOMETRY}, {@code GEOGRAPHY} — INHERITED refusal.</b> The driver reports
 *       all three as {@code DATA_TYPE = -4} ({@link Types#LONGVARBINARY}). {@link GenericDialect} has no case for
 *       {@code LONGVARBINARY}, so they already hit its {@code default -> null} skip-with-WARN path — no override is
 *       needed and none is added.</li>
 * </ul>
 * Anything else Redshift cannot describe is reported by the driver as {@code DATA_TYPE = 1111} ({@link Types#OTHER}),
 * which likewise hits {@link GenericDialect}'s {@code default -> null}. As on Postgres, {@code NUMERIC}/{@code DECIMAL}
 * is never refused (the scoped rule above always yields {@code LONG} or {@code DOUBLE}).
 */
public class RedshiftDialect extends PostgresDialect {

    public static final RedshiftDialect INSTANCE = new RedshiftDialect();

    /**
     * Per-connection session setup: pin the session zone to UTC using Redshift's documented {@code SET timezone TO
     * '...'} spelling. Deliberately does NOT include {@code SET statement_timeout} (Redshift uses WLM; see the class
     * javadoc). Single statement, so ordering is moot.
     */
    private static final List<String> INIT_STATEMENTS = List.of("SET timezone TO 'UTC'");

    @Override
    public String name() {
        return "redshift";
    }

    @Override
    public List<String> initStatements() {
        return INIT_STATEMENTS;
    }

    /**
     * Empty verified-major set: Redshift's version numbering is opaque/Amazon-internal, so there is no stable major
     * line to check. Returning an empty set disables the once-per-URL unsupported-version WARN (same contract as
     * {@link GenericDialect}), rather than warning on every Redshift connection. This intentionally re-widens
     * {@link PostgresDialect}'s {@code {12..16}} set back to empty.
     */
    @Override
    public Set<Integer> supportedDatabaseMajorVersions() {
        return Set.of();
    }

    /**
     * Refuses Redshift {@code SUPER} explicitly, then delegates everything else to {@link PostgresDialect} (which in
     * turn delegates all non-{@code NUMERIC} types to {@link GenericDialect}).
     * <p>
     * {@code SUPER} is reported by the Redshift JDBC driver's {@code getColumns()} as {@link Types#LONGNVARCHAR}
     * ({@code -16}), which {@link GenericDialect} would map to {@code KEYWORD} — a silent mis-mapping of a
     * semi-structured PartiQL document to a string. Returning {@code null} makes the connector skip the column with a
     * WARN instead. {@code VARBYTE}/{@code GEOMETRY}/{@code GEOGRAPHY} ({@link Types#LONGVARBINARY}) and any other
     * non-relational type ({@link Types#OTHER}) are already refused by the inherited {@code default -> null} path, so
     * they need no branch here (see the class-level "Refused types" section).
     */
    @Override
    public DataType mapJdbcType(int jdbcType, int columnSize, int decimalDigits) {
        if (jdbcType == Types.LONGNVARCHAR) {
            return null;
        }
        return super.mapJdbcType(jdbcType, columnSize, decimalDigits);
    }
}
