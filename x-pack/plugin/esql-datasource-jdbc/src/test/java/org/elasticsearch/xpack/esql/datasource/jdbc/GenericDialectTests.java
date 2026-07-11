/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.sql.Types;

public class GenericDialectTests extends ESTestCase {

    private final GenericDialect dialect = GenericDialect.INSTANCE;

    public void testQuoteIdentifierAnsi() {
        String name = randomAlphaOfLengthBetween(1, 20);
        assertEquals("\"" + name + "\"", dialect.quoteIdentifier(name));
    }

    public void testQuoteIdentifierRejectsDoubleQuote() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> dialect.quoteIdentifier("bad\"name"));
        assertTrue(e.getMessage().contains("double-quote"));
    }

    public void testQuoteIdentifierRejectsNul() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> dialect.quoteIdentifier("bad\u0000name"));
        assertTrue(e.getMessage().contains("NUL"));
    }

    public void testQuoteIdentifierRejectsClassicSqlInjectionPayload() {
        // Pinned acceptance-checklist case (plan: "hostile-identifier test passes: quoteIdentifier blocks
        // foo\"; DROP TABLE x;--"). The mechanism is the same as the generic double-quote rejection above, but the
        // exact payload is worth pinning so a future relaxation of the rule (e.g. opt-in escape-doubling for some
        // dialect) doesn't silently let this through.
        String payload = "foo\"; DROP TABLE x;--";
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> dialect.quoteIdentifier(payload));
        assertTrue(e.getMessage().contains("double-quote"));
    }

    public void testMapJdbcTypeFloatVsReal() {
        assertEquals(DataType.DOUBLE, dialect.mapJdbcType(Types.FLOAT, 0, 0));
        assertEquals(DataType.FLOAT, dialect.mapJdbcType(Types.REAL, 0, 0));
    }

    public void testMapJdbcTypeAllSupported() {
        assertEquals(DataType.BOOLEAN, dialect.mapJdbcType(Types.BOOLEAN, 0, 0));
        assertEquals(DataType.BOOLEAN, dialect.mapJdbcType(Types.BIT, 0, 0));
        assertEquals(DataType.BYTE, dialect.mapJdbcType(Types.TINYINT, 0, 0));
        assertEquals(DataType.SHORT, dialect.mapJdbcType(Types.SMALLINT, 0, 0));
        assertEquals(DataType.INTEGER, dialect.mapJdbcType(Types.INTEGER, 0, 0));
        assertEquals(DataType.LONG, dialect.mapJdbcType(Types.BIGINT, 0, 0));
        assertEquals(DataType.DOUBLE, dialect.mapJdbcType(Types.DECIMAL, 10, 2));
        assertEquals(DataType.DOUBLE, dialect.mapJdbcType(Types.NUMERIC, 10, 2));
        assertEquals(DataType.DOUBLE, dialect.mapJdbcType(Types.DOUBLE, 0, 0));
        assertEquals(DataType.KEYWORD, dialect.mapJdbcType(Types.CHAR, 10, 0));
        assertEquals(DataType.KEYWORD, dialect.mapJdbcType(Types.VARCHAR, 255, 0));
        assertEquals(DataType.KEYWORD, dialect.mapJdbcType(Types.LONGVARCHAR, 0, 0));
        assertEquals(DataType.KEYWORD, dialect.mapJdbcType(Types.NCHAR, 10, 0));
        assertEquals(DataType.KEYWORD, dialect.mapJdbcType(Types.NVARCHAR, 255, 0));
        assertEquals(DataType.KEYWORD, dialect.mapJdbcType(Types.LONGNVARCHAR, 0, 0));
        assertEquals(DataType.DATETIME, dialect.mapJdbcType(Types.DATE, 0, 0));
        assertEquals(DataType.DATETIME, dialect.mapJdbcType(Types.TIME, 0, 0));
        assertEquals(DataType.DATETIME, dialect.mapJdbcType(Types.TIMESTAMP, 0, 0));
        assertEquals(DataType.DATETIME, dialect.mapJdbcType(Types.TIMESTAMP_WITH_TIMEZONE, 0, 0));
        assertEquals(DataType.DATETIME, dialect.mapJdbcType(Types.TIME_WITH_TIMEZONE, 0, 0));
    }

    public void testMapJdbcTypeUnsupportedReturnsNull() {
        assertNull(dialect.mapJdbcType(Types.BLOB, 0, 0));
        assertNull(dialect.mapJdbcType(Types.BINARY, 0, 0));
        assertNull(dialect.mapJdbcType(Types.VARBINARY, 0, 0));
        assertNull(dialect.mapJdbcType(Types.CLOB, 0, 0));
        assertNull(dialect.mapJdbcType(Types.ARRAY, 0, 0));
        assertNull(dialect.mapJdbcType(Types.STRUCT, 0, 0));
        assertNull(dialect.mapJdbcType(Types.ROWID, 0, 0));
        assertNull(dialect.mapJdbcType(Types.REF, 0, 0));
        assertNull(dialect.mapJdbcType(randomIntBetween(10000, 20000), 0, 0));
    }
}
