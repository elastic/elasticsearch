/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;
import java.sql.SQLType;
import java.sql.Types;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Elasticsearch SQL data types.
 * This class also implements JDBC {@link SQLType} for properly receiving and setting values.
 * Where possible, please use the build-in, JDBC {@link Types} and {@link JDBCType} to avoid coupling
 * to the API.
 */
public enum DataType {

    // @formatter:off
    //                     jdbc type,          size,              defPrecision,dispSize, int,   rat,   docvals
    NULL(                  JDBCType.NULL,      0,                 0,                 0,  false, false, false),
    UNSUPPORTED(           JDBCType.OTHER,     0,                 0,                 0,  false, false, false),
    BOOLEAN(               JDBCType.BOOLEAN,   1,                 1,                 1,  false, false, false),
    BYTE(                  JDBCType.TINYINT,   Byte.BYTES,        3,                 5,  true,  false, true),
    SHORT(                 JDBCType.SMALLINT,  Short.BYTES,       5,                 6,  true,  false, true),
    INTEGER(               JDBCType.INTEGER,   Integer.BYTES,     10,                11, true,  false, true),
    LONG(                  JDBCType.BIGINT,    Long.BYTES,        19,                20, true,  false, true),
    // 53 bits defaultPrecision ~ 15(15.95) decimal digits (53log10(2)),
    DOUBLE(                JDBCType.DOUBLE,    Double.BYTES,      15,                25, false, true,  true),
    // 24 bits defaultPrecision - 24*log10(2) =~ 7 (7.22)
    FLOAT(                 JDBCType.REAL,      Float.BYTES,       7,                 15, false, true,  true),
    HALF_FLOAT(            JDBCType.FLOAT,     Double.BYTES,      16,                25, false, true,  true),
    // precision is based on long
    SCALED_FLOAT(          JDBCType.FLOAT,     Double.BYTES,      19,                25, false, true,  true),
    KEYWORD(               JDBCType.VARCHAR,   Integer.MAX_VALUE, 256,               0,  false, false, true),
    TEXT(                  JDBCType.VARCHAR,   Integer.MAX_VALUE, Integer.MAX_VALUE, 0,  false, false, false),
    OBJECT(                JDBCType.STRUCT,    -1,                0,                 0,  false, false, false),
    NESTED(                JDBCType.STRUCT,    -1,                0,                 0,  false, false, false),
    BINARY(                JDBCType.VARBINARY, -1,                Integer.MAX_VALUE, 0,  false, false, false),
    // since ODBC and JDBC interpret precision for Date as display size,
    // the precision is 23 (number of chars in ISO8601 with millis) + Z (the UTC timezone)
    // see https://github.com/elastic/elasticsearch/issues/30386#issuecomment-386807288
    DATE(                  JDBCType.TIMESTAMP, Long.BYTES,        24,                24, false, false, true),
    //
    // specialized types
    //
    // IP can be v4 or v6. The latter has 2^128 addresses or 340,282,366,920,938,463,463,374,607,431,768,211,456
    // aka 39 chars
    IP(                    JDBCType.VARCHAR,   39,               39,                 0,  false, false, true),
    //
    // INTERVALS
    // the list is long as there are a lot of variations and that's what clients (ODBC) expect
    //                        jdbc type,                         size,            prec,disp, int,   rat,   docvals
    INTERVAL_YEAR(            ExtTypes.INTERVAL_YEAR,            Integer.BYTES,   7,    7,   false, false, false),
    INTERVAL_MONTH(           ExtTypes.INTERVAL_MONTH,           Integer.BYTES,   7,    7,   false, false, false),
    INTERVAL_DAY(             ExtTypes.INTERVAL_DAY,             Long.BYTES,      23,   23,  false, false, false),
    INTERVAL_HOUR(            ExtTypes.INTERVAL_HOUR,            Long.BYTES,      23,   23,  false, false, false),
    INTERVAL_MINUTE(          ExtTypes.INTERVAL_MINUTE,          Long.BYTES,      23,   23,  false, false, false),
    INTERVAL_SECOND(          ExtTypes.INTERVAL_SECOND,          Long.BYTES,      23,   23,  false, false, false),
    INTERVAL_YEAR_TO_MONTH(   ExtTypes.INTERVAL_YEAR_TO_MONTH,   Integer.BYTES,   7,    7,   false, false, false),
    INTERVAL_DAY_TO_HOUR(     ExtTypes.INTERVAL_DAY_TO_HOUR,     Long.BYTES,      23,   23,  false, false, false),
    INTERVAL_DAY_TO_MINUTE(   ExtTypes.INTERVAL_DAY_TO_MINUTE,   Long.BYTES,      23,   23,  false, false, false),
    INTERVAL_DAY_TO_SECOND(   ExtTypes.INTERVAL_DAY_TO_SECOND,   Long.BYTES,      23,   23,  false, false, false),
    INTERVAL_HOUR_TO_MINUTE(  ExtTypes.INTERVAL_HOUR_TO_MINUTE,  Long.BYTES,      23,   23,  false, false, false),
    INTERVAL_HOUR_TO_SECOND(  ExtTypes.INTERVAL_HOUR_TO_SECOND,  Long.BYTES,      23,   23,  false, false, false),
    INTERVAL_MINUTE_TO_SECOND(ExtTypes.INTERVAL_MINUTE_TO_SECOND,Long.BYTES,      23,   23,  false, false, false);
    // @formatter:on

    private static final Map<String, DataType> odbcToEs;

    static {
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

        // Intervals
        odbcToEs.put("SQL_INTERVAL_HOUR_TO_MINUTE", INTERVAL_HOUR_TO_MINUTE);
        odbcToEs.put("SQL_INTERVAL_HOUR_TO_SECOND", INTERVAL_HOUR_TO_SECOND);
        odbcToEs.put("SQL_INTERVAL_MINUTE_TO_SECOND", INTERVAL_MINUTE_TO_SECOND);
        odbcToEs.put("SQL_INTERVAL_MONTH", INTERVAL_MONTH);
        odbcToEs.put("SQL_INTERVAL_YEAR", INTERVAL_YEAR);
        odbcToEs.put("SQL_INTERVAL_YEAR_TO_MONTH", INTERVAL_YEAR_TO_MONTH);
        odbcToEs.put("SQL_INTERVAL_DAY", INTERVAL_DAY);
        odbcToEs.put("SQL_INTERVAL_HOUR", INTERVAL_HOUR);
        odbcToEs.put("SQL_INTERVAL_MINUTE", INTERVAL_MINUTE);
        odbcToEs.put("SQL_INTERVAL_SECOND", INTERVAL_SECOND);
        odbcToEs.put("SQL_INTERVAL_DAY_TO_HOUR", INTERVAL_DAY_TO_HOUR);
        odbcToEs.put("SQL_INTERVAL_DAY_TO_MINUTE", INTERVAL_DAY_TO_MINUTE);
        odbcToEs.put("SQL_INTERVAL_DAY_TO_SECOND", INTERVAL_DAY_TO_SECOND);
    }

    /**
     * Elasticsearch type name
     */
    public final String esType;

    /**
     * Compatible JDBC type
     */
    public final SQLType sqlType;

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
    private final boolean isInteger;

    /**
     * True if the type represents a rational number
     */
    private final boolean isRational;

    /**
     * True if the type supports doc values by default
     */
    public final boolean defaultDocValues;

    DataType(SQLType sqlType, int size, int defaultPrecision, int displaySize, boolean isInteger,
            boolean isRational, boolean defaultDocValues) {
        this.esType = name().toLowerCase(Locale.ROOT);
        this.sqlType = sqlType;
        this.size = size;
        this.defaultPrecision = defaultPrecision;
        this.displaySize = displaySize;
        this.isInteger = isInteger;
        this.isRational = isRational;
        this.defaultDocValues = defaultDocValues;
    }

    public String sqlName() {
        return sqlType.getName();
    }

    public boolean isInteger() {
        return isInteger;
    }

    public boolean isRational() {
        return isRational;
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
    
    public static DataType fromOdbcType(String odbcType) {
        return odbcToEs.get(odbcType);
    }
    
    /**
     * Creates returns DataType enum corresponding to the specified es type
     * <p>
     * For any dataType DataType.fromTypeName(dataType.esType) == dataType
     */
    public static DataType fromTypeName(String esType) {
        try {
            return DataType.valueOf(esType.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            return DataType.UNSUPPORTED;
        }
    }
}