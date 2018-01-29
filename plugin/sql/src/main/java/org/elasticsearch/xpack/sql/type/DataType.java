/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;
import java.util.Locale;

/**
 * Elasticsearch data types that supported by SQL interface
 */
public enum DataType {
    // @formatter:off
    //           jdbc type,          size,              defPrecision, dispSize, sig, int, rat, docvals
    NULL(        JDBCType.NULL,      0,                 0,                 0),
    UNSUPPORTED( JDBCType.OTHER,     0,                 0,                 0),
    BOOLEAN(     JDBCType.BOOLEAN,   1,                 1,                 1),
    BYTE(        JDBCType.TINYINT,   Byte.BYTES,        3,                 5,  true, true, false, true),
    SHORT(       JDBCType.SMALLINT,  Short.BYTES,       5,                 6,  true, true, false, true),
    INTEGER(     JDBCType.INTEGER,   Integer.BYTES,     10,                11, true, true, false, true),
    LONG(        JDBCType.BIGINT,    Long.BYTES,        19,                20, true, true, false, true),
    // 53 bits defaultPrecision ~ 16(15.95) decimal digits (53log10(2)),
    DOUBLE(      JDBCType.DOUBLE,    Double.BYTES,      16,                25, true, false, true, true),
    // 24 bits defaultPrecision - 24*log10(2) =~ 7 (7.22)
    FLOAT(       JDBCType.REAL,      Float.BYTES,       7,                 15, true, false, true, true),
    HALF_FLOAT(  JDBCType.FLOAT,     Double.BYTES,      16,                25, true, false, true, true),
    // precision is based on long
    SCALED_FLOAT(JDBCType.FLOAT,     Double.BYTES,      19,                25, true, false, true, true),
    KEYWORD(     JDBCType.VARCHAR,   Integer.MAX_VALUE, 256,               0),
    TEXT(        JDBCType.VARCHAR,   Integer.MAX_VALUE, Integer.MAX_VALUE, 0,  false, false, false, false),
    OBJECT(      JDBCType.STRUCT,    -1,                0,                 0),
    NESTED(      JDBCType.STRUCT,    -1,                0,                 0),
    BINARY(      JDBCType.VARBINARY, -1,                Integer.MAX_VALUE, 0),
    DATE(        JDBCType.TIMESTAMP, Long.BYTES,        19,                20);
    // @formatter:on

    /**
     * Elasticsearch type name
     */
    public final String esType;

    /**
     * Compatible JDBC type
     */
    public final JDBCType jdbcType;


    /**
     * Size of the type in bytes
     * <p>
     * -1 if the size can vary
     */
    public final int size;

    /**
     * Precision
     * <p>
     * Specified column size. For numeric data, this is the maximum precision. For character
     * data, this is the length in characters. For datetime datatypes, this is the length in characters of the
     * String representation (assuming the maximum allowed defaultPrecision of the fractional seconds component).
     */
    public final int defaultPrecision;


    /**
     * Display Size
     * <p>
     * Normal maximum width in characters.
     */
    public final int displaySize;

    /**
     * True if the type represents a signed number
     */
    public final boolean isSigned;

    /**
     * True if the type represents an integer number
     */
    public final boolean isInteger;

    /**
     * True if the type represents a rational number
     */
    public final boolean isRational;

    /**
     * True if the type supports doc values by default
     */
    public final boolean defaultDocValues;

    DataType(JDBCType jdbcType, int size, int defaultPrecision, int displaySize, boolean isSigned, boolean isInteger, boolean isRational,
             boolean defaultDocValues) {
        this.esType = name().toLowerCase(Locale.ROOT);
        this.jdbcType = jdbcType;
        this.size = size;
        this.defaultPrecision = defaultPrecision;
        this.displaySize = displaySize;
        this.isSigned = isSigned;
        this.isInteger = isInteger;
        this.isRational = isRational;
        this.defaultDocValues = defaultDocValues;
    }

    DataType(JDBCType jdbcType, int size, int defaultPrecision, int displaySize) {
        this(jdbcType, size, defaultPrecision, displaySize, false, false, false, true);
    }

    public String sqlName() {
        return jdbcType.getName();
    }

    public boolean isNumeric() {
        return isInteger || isRational;
    }

    public boolean isString() {
        return this == KEYWORD || this == TEXT;
    }

    public boolean isPrimitive() {
        return this != OBJECT && this != NESTED;
    }
}