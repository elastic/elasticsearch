/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;
import java.sql.SQLType;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

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
    OBJECT(      JDBCType.STRUCT,    null,            -1,                0,                 0, false, false, false),
    NESTED(      JDBCType.STRUCT,    null,            -1,                0,                 0, false, false, false),
    BINARY(      JDBCType.VARBINARY, byte[].class,    -1,                Integer.MAX_VALUE, 0),
    // since ODBC and JDBC interpret precision for Date as display size,
    // the precision is 23 (number of chars in ISO8601 with millis) + Z (the UTC timezone)
    // see https://github.com/elastic/elasticsearch/issues/30386#issuecomment-386807288
    DATE(        JDBCType.TIMESTAMP, Timestamp.class, Long.BYTES,        24,                24),
    //
    // specialized types
    //
    // IP can be v4 or v6. The latter has 2^128 addresses or 340,282,366,920,938,463,463,374,607,431,768,211,456
    // aka 39 chars
    IP(          JDBCType.VARCHAR,   String.class,     39,               39,                0,false, false, true);
    // @formatter:on

    public static final String ODBC_DATATYPE_PREFIX = "SQL_";

    private static final Map<SQLType, DataType> jdbcToEs;
    private static final Map<String, DataType> odbcToEs;

    static {
        jdbcToEs = Arrays.stream(DataType.values())
                .filter(type -> type != TEXT && type != NESTED
                                && type != SCALED_FLOAT && type != IP) // Remove duplicates
                .collect(toMap(dataType -> dataType.jdbcType, dataType -> dataType));

        odbcToEs = new HashMap<>(36);

        // Numeric
        odbcToEs.put("SQL_BIT", BOOLEAN);
        odbcToEs.put("SQL_TINYINT", BYTE);
        odbcToEs.put("SQL_SMALLINT", SHORT);
        odbcToEs.put("SQL_INTEGER", INTEGER);
        odbcToEs.put("SQL_BIGINT", LONG);
        odbcToEs.put("SQL_FLOAT", FLOAT);
        odbcToEs.put("SQL_REAL", FLOAT);
        odbcToEs.put("SQL_DOUBLE", DOUBLE);
        odbcToEs.put("SQL_DECIMAL", DOUBLE);
        odbcToEs.put("SQL_NUMERIC", DOUBLE);

        // String
        odbcToEs.put("SQL_GUID", KEYWORD);
        odbcToEs.put("SQL_CHAR", KEYWORD);
        odbcToEs.put("SQL_WCHAR", KEYWORD);
        odbcToEs.put("SQL_VARCHAR", TEXT);
        odbcToEs.put("SQL_WVARCHAR", TEXT);
        odbcToEs.put("SQL_LONGVARCHAR", TEXT);
        odbcToEs.put("SQL_WLONGVARCHAR", TEXT);

        // Binary
        odbcToEs.put("SQL_BINARY", BINARY);
        odbcToEs.put("SQL_VARBINARY", BINARY);
        odbcToEs.put("SQL_LONGVARBINARY", BINARY);

        // Date
        odbcToEs.put("SQL_DATE", DATE);
        odbcToEs.put("SQL_TIME", DATE);
        odbcToEs.put("SQL_TIMESTAMP", DATE);

        // Intervals - Currently Not Supported
        odbcToEs.put("SQL_INTERVAL_HOUR_TO_MINUTE", UNSUPPORTED);
        odbcToEs.put("SQL_INTERVAL_HOUR_TO_SECOND", UNSUPPORTED);
        odbcToEs.put("SQL_INTERVAL_MINUTE_TO_SECOND", UNSUPPORTED);
        odbcToEs.put("SQL_INTERVAL_MONTH", UNSUPPORTED);
        odbcToEs.put("SQL_INTERVAL_YEAR", UNSUPPORTED);
        odbcToEs.put("SQL_INTERVAL_YEAR_TO_MONTH", UNSUPPORTED);
        odbcToEs.put("SQL_INTERVAL_DAY", UNSUPPORTED);
        odbcToEs.put("SQL_INTERVAL_HOUR", UNSUPPORTED);
        odbcToEs.put("SQL_INTERVAL_MINUTE", UNSUPPORTED);
        odbcToEs.put("SQL_INTERVAL_SECOND", UNSUPPORTED);
        odbcToEs.put("SQL_INTERVAL_DAY_TO_HOUR", UNSUPPORTED);
        odbcToEs.put("SQL_INTERVAL_DAY_TO_MINUTE", UNSUPPORTED);
        odbcToEs.put("SQL_INTERVAL_DAY_TO_SECOND", UNSUPPORTED);
    }

    /**
     * Elasticsearch type name
     */
    public final String esType;

    /**
     * Compatible JDBC type
     */
    public final SQLType jdbcType;

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

    DataType(SQLType jdbcType, Class<?> javaClass, int size, int defaultPrecision, int displaySize, boolean isInteger, boolean isRational,
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

    DataType(SQLType jdbcType, Class<?> javaClass, int size, int defaultPrecision, int displaySize) {
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

    public static DataType fromJdbcType(SQLType jdbcType) {
        if (jdbcToEs.containsKey(jdbcType) == false) {
            throw new IllegalArgumentException("Unsupported JDBC type [" + jdbcType + "]");
        }
        return jdbcToEs.get(jdbcType);
    }
    
    public static Class<?> fromJdbcTypeToJava(SQLType jdbcType) {
        if (jdbcToEs.containsKey(jdbcType) == false) {
            throw new IllegalArgumentException("Unsupported JDBC type [" + jdbcType + "]");
        }
        return jdbcToEs.get(jdbcType).javaClass();
    }

    public static DataType fromODBCType(String odbcType) {
        return odbcToEs.get(odbcType);
    }
    /**
     * Creates returns DataType enum coresponding to the specified es type
     * <p>
     * For any dataType DataType.fromEsType(dataType.esType) == dataType
     */
    public static DataType fromEsType(String esType) {
        try {
            return DataType.valueOf(esType.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            return DataType.UNSUPPORTED;
        }
    }

    public boolean isCompatibleWith(DataType other) {
        if (this == other) {
            return true;
        } else return
            (this == NULL || other == NULL) ||
            (isString() && other.isString()) ||
            (isNumeric() && other.isNumeric());
    }
}
