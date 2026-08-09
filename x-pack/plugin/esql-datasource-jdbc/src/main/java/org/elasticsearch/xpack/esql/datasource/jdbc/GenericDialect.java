/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.xpack.esql.core.type.DataType;

import java.sql.Types;
import java.util.Set;

/**
 * ANSI-default {@link JdbcDialect} that targets every JDBC driver. Used as the fallback (for example against H2);
 * vendor dialects (Postgres / MySQL / Snowflake) extend this and override the few hooks that diverge.
 * <p>
 * Identifier quoting follows ANSI: a double-quote on each side ({@code "foo"}). Identifiers that themselves contain
 * a double-quote are <b>rejected</b>; we will not emit them with escape-doubling because:
 * <ul>
 *   <li>The standard identifier escape ({@code "foo""bar"}) is supported unevenly across vendors.</li>
 *   <li>The ESQL surface today never asks for column or table names containing double-quotes; rejecting is a
 *       shippable safety net for hostile WITH (table=...) inputs without growing a dialect-specific escape table.</li>
 * </ul>
 *
 * Type mapping is deliberately conservative: we project only the types we have block-builder support for. Any
 * unmapped {@code java.sql.Types} constant returns {@code null} so callers can skip the column with a WARN.
 * <p>
 * <b>FLOAT / REAL fix:</b> ANSI is famously inconsistent about which is single-precision and which is double-precision
 * across vendors. We follow the JDBC 4 spec normalization: {@code Types.FLOAT} → ESQL DOUBLE (8-byte),
 * {@code Types.REAL} → ESQL FLOAT (4-byte). Drivers that report {@code DOUBLE} for double-precision are also
 * mapped to ESQL DOUBLE, so the only loss is the single-precision/double-precision boundary where a vendor
 * reports {@code FLOAT} for true 4-byte storage (rare; such vendors must override this method).
 */
public class GenericDialect implements JdbcDialect {

    public static final GenericDialect INSTANCE = new GenericDialect();

    /** ANSI/generic dialect identifier, used only in log lines. */
    @Override
    public String name() {
        return "generic";
    }

    /**
     * No version discipline: the generic dialect targets any JDBC driver, so there is no verified-major set and the
     * connector never warns on the server version.
     */
    @Override
    public Set<Integer> supportedDatabaseMajorVersions() {
        return Set.of();
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            throw new IllegalArgumentException("identifier must not be null or empty");
        }
        if (identifier.indexOf('"') >= 0) {
            throw new IllegalArgumentException(
                "identifier contains a double-quote which would break ANSI quoting; rejecting hostile name [" + identifier + "]"
            );
        }
        if (identifier.indexOf('\u0000') >= 0) {
            throw new IllegalArgumentException("identifier contains a NUL byte; rejecting hostile name");
        }
        return "\"" + identifier + "\"";
    }

    @Override
    public DataType mapJdbcType(int jdbcType, int columnSize, int decimalDigits) {
        return switch (jdbcType) {
            // Booleans
            case Types.BOOLEAN, Types.BIT -> DataType.BOOLEAN;
            // Integers
            case Types.TINYINT -> DataType.BYTE;
            case Types.SMALLINT -> DataType.SHORT;
            case Types.INTEGER -> DataType.INTEGER;
            case Types.BIGINT -> DataType.LONG;
            // Decimals -- map to DOUBLE; precision is preserved in the result-set extraction layer when possible.
            case Types.DECIMAL, Types.NUMERIC, Types.DOUBLE -> DataType.DOUBLE;
            // FLOAT/REAL: see class javadoc.
            case Types.FLOAT -> DataType.DOUBLE;
            case Types.REAL -> DataType.FLOAT;
            // Strings
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> DataType.KEYWORD;
            // Temporals: produce DATETIME (millis-since-epoch in ESQL today; the value is read via
            // getTimestamp(col, <UTC Calendar>).toInstant() -- see ColumnReader for the tz-vs-naive contract).
            case Types.DATE, Types.TIME, Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE, Types.TIME_WITH_TIMEZONE -> DataType.DATETIME;
            // Anything else (BINARY, BLOB, CLOB, ROWID, REF, ARRAY, STRUCT, ...) is currently unsupported.
            default -> null;
        };
    }
}
