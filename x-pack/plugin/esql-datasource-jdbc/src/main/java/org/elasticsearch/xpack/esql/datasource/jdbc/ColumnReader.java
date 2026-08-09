/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Per-column extractor that reads the current row of a {@link ResultSet} into a typed {@link Block.Builder}.
 * One instance is built per column at cursor open (column index is 1-based) and reused for every row -- replaces a
 * per-row {@code switch (DataType)} with a fixed dispatch chosen once.
 * <p>
 * Implementations MUST handle {@link ResultSet#wasNull()} explicitly: every getter is followed by a
 * {@code rs.wasNull()} check that branches to {@link Block.Builder#appendNull()}. Failing to do so produces silent
 * zero/empty values for SQL NULLs.
 * <p>
 * {@link #forType(DataType)} returns {@code null} for unsupported types -- the metadata-resolution path skips such
 * columns with a WARN, and {@link JdbcResultCursor} treats a late {@code null} here as a programming error.
 */
@FunctionalInterface
interface ColumnReader {

    void read(ResultSet rs, int col, Block.Builder builder) throws SQLException;

    static ColumnReader forType(DataType type) {
        if (type == null) {
            return null;
        }
        return switch (type) {
            case BOOLEAN -> (rs, col, b) -> {
                boolean v = rs.getBoolean(col);
                if (rs.wasNull()) {
                    b.appendNull();
                } else {
                    ((org.elasticsearch.compute.data.BooleanBlock.Builder) b).appendBoolean(v);
                }
            };
            case BYTE -> (rs, col, b) -> {
                int v = rs.getByte(col);
                if (rs.wasNull()) {
                    b.appendNull();
                } else {
                    ((org.elasticsearch.compute.data.IntBlock.Builder) b).appendInt(v);
                }
            };
            case SHORT -> (rs, col, b) -> {
                int v = rs.getShort(col);
                if (rs.wasNull()) {
                    b.appendNull();
                } else {
                    ((org.elasticsearch.compute.data.IntBlock.Builder) b).appendInt(v);
                }
            };
            case INTEGER -> (rs, col, b) -> {
                int v = rs.getInt(col);
                if (rs.wasNull()) {
                    b.appendNull();
                } else {
                    ((org.elasticsearch.compute.data.IntBlock.Builder) b).appendInt(v);
                }
            };
            case LONG -> (rs, col, b) -> {
                long v = rs.getLong(col);
                if (rs.wasNull()) {
                    b.appendNull();
                } else {
                    ((org.elasticsearch.compute.data.LongBlock.Builder) b).appendLong(v);
                }
            };
            case FLOAT -> (rs, col, b) -> {
                float v = rs.getFloat(col);
                if (rs.wasNull()) {
                    b.appendNull();
                } else {
                    ((org.elasticsearch.compute.data.FloatBlock.Builder) b).appendFloat(v);
                }
            };
            case DOUBLE -> (rs, col, b) -> {
                double v = rs.getDouble(col);
                if (rs.wasNull()) {
                    b.appendNull();
                } else {
                    ((org.elasticsearch.compute.data.DoubleBlock.Builder) b).appendDouble(v);
                }
            };
            case KEYWORD -> (rs, col, b) -> {
                String v = rs.getString(col);
                if (v == null) {
                    b.appendNull();
                } else {
                    ((org.elasticsearch.compute.data.BytesRefBlock.Builder) b).appendBytesRef(new org.apache.lucene.util.BytesRef(v));
                }
            };
            case DATETIME -> {
                // ESQL DATETIME is the ONLY temporal type a ColumnReader ever sees: mapJdbcType folds SQL DATE / TIME /
                // TIMESTAMP / TIMESTAMP_WITH_TIMEZONE / TIME_WITH_TIMEZONE all into DATETIME (see GenericDialect), so the
                // tz-vs-naive distinction is already lost by the time we read. We therefore extract every temporal through
                // ONE driver-portable path -- rs.getTimestamp(col, <Calendar in UTC>).toInstant() -- which is correct for
                // both underlying shapes:
                // * a value WITH a time zone (e.g. Postgres timestamptz) already denotes an absolute instant; the driver
                // ignores the Calendar and returns that exact instant.
                // * a naive value WITHOUT a time zone (e.g. Postgres/H2 TIMESTAMP, SQL DATE) carries only a wall clock;
                // the UTC Calendar anchors that wall clock to UTC. That matches PostgresDialect's per-connection
                // `SET TIME ZONE 'UTC'` session AND -- crucially -- makes the result deterministic regardless of the
                // JVM default time zone. Plain getTimestamp(col) (no Calendar) would interpret a naive value in the
                // JVM default zone and silently shift it for a node running in a non-UTC TZ.
                // We deliberately do NOT use getObject(col, java.time.Instant.class): pgjdbc 42.7.3's
                // PgResultSet.getObject(int, Class) has no Instant branch (its chain ends at OffsetDateTime/OffsetTime then
                // throws), so that path fails EVERY Postgres temporal VALUE read. getTimestamp(col, Calendar) is JDBC 4.0
                // core and is implemented by every driver we target (H2 2.x + pgjdbc verified).
                // The Calendar is allocated once per column reader and mutated only by getTimestamp on the single cursor
                // thread that owns this reader, so it needs no synchronization.
                java.util.Calendar utcCalendar = java.util.Calendar.getInstance(
                    java.util.TimeZone.getTimeZone("UTC"),
                    java.util.Locale.ROOT
                );
                yield (rs, col, b) -> {
                    java.sql.Timestamp ts = rs.getTimestamp(col, utcCalendar);
                    if (ts == null || rs.wasNull()) {
                        b.appendNull();
                    } else {
                        ((org.elasticsearch.compute.data.LongBlock.Builder) b).appendLong(ts.toInstant().toEpochMilli());
                    }
                };
            }
            default -> null;
        };
    }
}
