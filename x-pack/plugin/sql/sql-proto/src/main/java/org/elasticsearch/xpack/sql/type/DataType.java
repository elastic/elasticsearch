/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Elasticsearch data types that supported by SQL interface
 */
public enum DataType {
    // @formatter:off
    //           jdbc type,          Java Class       size,              defPrecision, dispSize, int, rat, docvals
    NULL(        JDBCType.NULL,      null,            0,                 0,                 0),
    UNSUPPORTED( JDBCType.OTHER,     null,            0,                 0,                 0),
    BOOLEAN(     JDBCType.BOOLEAN,   Boolean.class,   1,                 1,                 1),
    BYTE(        JDBCType.TINYINT,   Byte.class,      Byte.BYTES,        3,                 5, true, false, true),
    SHORT(       JDBCType.SMALLINT,  Short.class,     Short.BYTES,       5,                 6, true, false, true),
    INTEGER(     JDBCType.INTEGER,   Integer.class,   Integer.BYTES,     10,                11, true, false, true),
    LONG(        JDBCType.BIGINT,    Long.class,      Long.BYTES,        19,                20, true, false, true),
    // 53 bits defaultPrecision ~ 15(15.95) decimal digits (53log10(2)),
    DOUBLE(      JDBCType.DOUBLE,    Double.class,    Double.BYTES,      15,                25, false, true, true),
    // 24 bits defaultPrecision - 24*log10(2) =~ 7 (7.22)
    FLOAT(       JDBCType.REAL,      Float.class,     Float.BYTES,       7,                 15, false, true, true),
    HALF_FLOAT(  JDBCType.FLOAT,     Double.class,    Double.BYTES,      16,                25, false, true, true),
    // precision is based on long
    SCALED_FLOAT(JDBCType.FLOAT,     Double.class,    Double.BYTES,      19,                25, false, true, true),
    KEYWORD(     JDBCType.VARCHAR,   String.class,    Integer.MAX_VALUE, 256,               0),
    TEXT(        JDBCType.VARCHAR,   String.class,    Integer.MAX_VALUE, Integer.MAX_VALUE, 0, false, false, false),
    OBJECT(      JDBCType.STRUCT,    null,            -1,                0,                 0),
    NESTED(      JDBCType.STRUCT,    null,            -1,                0,                 0),
    BINARY(      JDBCType.VARBINARY, byte[].class,    -1,                Integer.MAX_VALUE, 0),
    // since ODBC and JDBC interpret precision for Date as display size,
    // the precision is 23 (number of chars in ISO8601 with millis) + Z (the UTC timezone)
    // see https://github.com/elastic/elasticsearch/issues/30386#issuecomment-386807288
    DATE(        JDBCType.TIMESTAMP, Timestamp.class, Long.BYTES,        24,                24);
    // @formatter:on

    private static final Map<JDBCType, DataType> jdbcToEs;

    static {
        jdbcToEs = Arrays.stream(DataType.values())
                .filter(dataType -> dataType != TEXT && dataType != NESTED && dataType != SCALED_FLOAT) // Remove duplicates
                .collect(Collectors.toMap(dataType -> dataType.jdbcType, dataType -> dataType));
    }

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
     * String representation (assuming the maximum allowed defaultPrecision of the fractional milliseconds component).
     */
    public final int defaultPrecision;


    /**
     * Display Size
     * <p>
     * Normal maximum width in characters.
     */
    public final int displaySize;

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

    private final Class<?> javaClass;

    DataType(JDBCType jdbcType, Class<?> javaClass, int size, int defaultPrecision, int displaySize, boolean isInteger, boolean isRational,
             boolean defaultDocValues) {
        this.esType = name().toLowerCase(Locale.ROOT);
        this.javaClass = javaClass;
        this.jdbcType = jdbcType;
        this.size = size;
        this.defaultPrecision = defaultPrecision;
        this.displaySize = displaySize;
        this.isInteger = isInteger;
        this.isRational = isRational;
        this.defaultDocValues = defaultDocValues;
    }

    DataType(JDBCType jdbcType, Class<?> javaClass, int size, int defaultPrecision, int displaySize) {
        this(jdbcType, javaClass, size, defaultPrecision, displaySize, false, false, true);
    }

    public String sqlName() {
        return jdbcType.getName();
    }
    
    public Class<?> javaClass() {
        return javaClass;
    }

    public boolean isNumeric() {
        return isInteger || isRational;
    }

    /**
     * Returns true if value is signed, false otherwise (including if the type is not numeric)
     */
    public boolean isSigned() {
        // For now all numeric values that es supports are signed
        return isNumeric();
    }

    public boolean isString() {
        return this == KEYWORD || this == TEXT;
    }

    public boolean isPrimitive() {
        return this != OBJECT && this != NESTED;
    }

    public static DataType fromJdbcType(JDBCType jdbcType) {
        if (jdbcToEs.containsKey(jdbcType) == false) {
            throw new IllegalArgumentException("Unsupported JDBC type [" + jdbcType + "]");
        }
        return jdbcToEs.get(jdbcType);
    }
    
    public static Class<?> fromJdbcTypeToJava(JDBCType jdbcType) {
        if (jdbcToEs.containsKey(jdbcType) == false) {
            throw new IllegalArgumentException("Unsupported JDBC type [" + jdbcType + "]");
        }
        return jdbcToEs.get(jdbcType).javaClass();
    }

    /**
     * Creates returns DataType enum coresponding to the specified es type
     * <p>
     * For any dataType DataType.fromEsType(dataType.esType) == dataType
     */
    public static DataType fromEsType(String esType) {
        return DataType.valueOf(esType.toUpperCase(Locale.ROOT));
    }
}