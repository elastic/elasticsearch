/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;

import java.sql.JDBCType;
import java.sql.SQLType;
import java.sql.Types;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Elasticsearch SQL data types.
 * This class also implements JDBC {@link SQLType} for properly receiving and setting values.
 * Where possible, please use the built-in, JDBC {@link Types} and {@link JDBCType} to avoid coupling
 * to the API.
 */
public enum DataType {

    // @formatter:off
    //             esType            jdbc type,          size,              defPrecision,dispSize, int,   rat,   docvals
    NULL(          "null",           JDBCType.NULL,      0,                 0,                 0,  false, false, false),
    UNSUPPORTED(                     JDBCType.OTHER,     0,                 0,                 0,  false, false, false),
    BOOLEAN(       "boolean",        JDBCType.BOOLEAN,   1,                 1,                 1,  false, false, false),
    BYTE(          "byte",           JDBCType.TINYINT,   Byte.BYTES,        3,                 5,  true,  false, true),
    SHORT(         "short",          JDBCType.SMALLINT,  Short.BYTES,       5,                 6,  true,  false, true),
    INTEGER(       "integer",        JDBCType.INTEGER,   Integer.BYTES,     10,                11, true,  false, true),
    LONG(          "long",           JDBCType.BIGINT,    Long.BYTES,        19,                20, true,  false, true),
    // 53 bits defaultPrecision ~ 15(15.95) decimal digits (53log10(2)),
    DOUBLE(        "double",         JDBCType.DOUBLE,    Double.BYTES,      15,                25, false, true,  true),
    // 24 bits defaultPrecision - 24*log10(2) =~ 7 (7.22)
    FLOAT(         "float",          JDBCType.REAL,      Float.BYTES,       7,                 15, false, true,  true),
    HALF_FLOAT(    "half_float",     JDBCType.FLOAT,     Float.BYTES,       3,                 25, false, true,  true),
    // precision is based on long
    SCALED_FLOAT(  "scaled_float",   JDBCType.DOUBLE,    Long.BYTES,        15,                25, false, true,  true),
    KEYWORD(       "keyword",        JDBCType.VARCHAR,   Integer.MAX_VALUE, 32766,             32766, false, false, true),
    TEXT(          "text",           JDBCType.VARCHAR,   Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE,  false, false, false),
    OBJECT(        "object",         JDBCType.STRUCT,    -1,                0,                 0,  false, false, false),
    NESTED(        "nested",         JDBCType.STRUCT,    -1,                0,                 0,  false, false, false),
    BINARY(        "binary",         JDBCType.VARBINARY, -1,                Integer.MAX_VALUE, Integer.MAX_VALUE,  false, false, false),
    // since ODBC and JDBC interpret precision for Date as display size
    // the precision is 23 (number of chars in ISO8601 with millis) + 6 chars for the timezone (e.g.: +05:00)
    // see https://github.com/elastic/elasticsearch/issues/30386#issuecomment-386807288
    DATE(                            JDBCType.DATE,      Long.BYTES,        3,                 29, false, false, true),
    TIME(                            JDBCType.TIME,      Long.BYTES,        3,                 18, false, false, true),
    DATETIME(      "date",           JDBCType.TIMESTAMP, Long.BYTES,        3,                 29, false, false, true),
    //
    // specialized types
    //
    GEO_SHAPE(                       ExtTypes.GEOMETRY,  Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, false, false, false),
    //                                                                                 display size = 2 doubles + len("POINT( )")
    GEO_POINT(                       ExtTypes.GEOMETRY,  Double.BYTES*2,    Integer.MAX_VALUE, 25 * 2 + 8, false, false, false),
    // IP can be v4 or v6. The latter has 2^128 addresses or 340,282,366,920,938,463,463,374,607,431,768,211,456
    // aka 39 chars
    SHAPE(                           ExtTypes.GEOMETRY,  Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, false, false, false),
    //                                                                                 display size = 2 doubles + len("POINT( )")
    IP(            "ip",             JDBCType.VARCHAR,   39,               39,                 0,  false, false, true),
    //
    // INTERVALS
    // the list is long as there are a lot of variations and that's what clients (ODBC) expect
    //           esType:null  jdbc type,                         size,            prec,disp, int,   rat,   docvals
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

    private static final Map<String, DataType> ODBC_TO_ES = new HashMap<>(36);
    static {
        // Numeric
        ODBC_TO_ES.put("SQL_BIT", BOOLEAN);
        ODBC_TO_ES.put("SQL_TINYINT", BYTE);
        ODBC_TO_ES.put("SQL_SMALLINT", SHORT);
        ODBC_TO_ES.put("SQL_INTEGER", INTEGER);
        ODBC_TO_ES.put("SQL_BIGINT", LONG);
        ODBC_TO_ES.put("SQL_REAL", FLOAT);
        ODBC_TO_ES.put("SQL_FLOAT", DOUBLE);
        ODBC_TO_ES.put("SQL_DOUBLE", DOUBLE);
        ODBC_TO_ES.put("SQL_DECIMAL", DOUBLE);
        ODBC_TO_ES.put("SQL_NUMERIC", DOUBLE);

        // String
        ODBC_TO_ES.put("SQL_GUID", KEYWORD);
        ODBC_TO_ES.put("SQL_CHAR", KEYWORD);
        ODBC_TO_ES.put("SQL_WCHAR", KEYWORD);
        ODBC_TO_ES.put("SQL_VARCHAR", TEXT);
        ODBC_TO_ES.put("SQL_WVARCHAR", TEXT);
        ODBC_TO_ES.put("SQL_LONGVARCHAR", TEXT);
        ODBC_TO_ES.put("SQL_WLONGVARCHAR", TEXT);

        // Binary
        ODBC_TO_ES.put("SQL_BINARY", BINARY);
        ODBC_TO_ES.put("SQL_VARBINARY", BINARY);
        ODBC_TO_ES.put("SQL_LONGVARBINARY", BINARY);

        // Date
        ODBC_TO_ES.put("SQL_DATE", DATE);
        ODBC_TO_ES.put("SQL_TIME", TIME);
        ODBC_TO_ES.put("SQL_TIMESTAMP", DATETIME);

        // Intervals
        ODBC_TO_ES.put("SQL_INTERVAL_HOUR_TO_MINUTE", INTERVAL_HOUR_TO_MINUTE);
        ODBC_TO_ES.put("SQL_INTERVAL_HOUR_TO_SECOND", INTERVAL_HOUR_TO_SECOND);
        ODBC_TO_ES.put("SQL_INTERVAL_MINUTE_TO_SECOND", INTERVAL_MINUTE_TO_SECOND);
        ODBC_TO_ES.put("SQL_INTERVAL_MONTH", INTERVAL_MONTH);
        ODBC_TO_ES.put("SQL_INTERVAL_YEAR", INTERVAL_YEAR);
        ODBC_TO_ES.put("SQL_INTERVAL_YEAR_TO_MONTH", INTERVAL_YEAR_TO_MONTH);
        ODBC_TO_ES.put("SQL_INTERVAL_DAY", INTERVAL_DAY);
        ODBC_TO_ES.put("SQL_INTERVAL_HOUR", INTERVAL_HOUR);
        ODBC_TO_ES.put("SQL_INTERVAL_MINUTE", INTERVAL_MINUTE);
        ODBC_TO_ES.put("SQL_INTERVAL_SECOND", INTERVAL_SECOND);
        ODBC_TO_ES.put("SQL_INTERVAL_DAY_TO_HOUR", INTERVAL_DAY_TO_HOUR);
        ODBC_TO_ES.put("SQL_INTERVAL_DAY_TO_MINUTE", INTERVAL_DAY_TO_MINUTE);
        ODBC_TO_ES.put("SQL_INTERVAL_DAY_TO_SECOND", INTERVAL_DAY_TO_SECOND);
    }


    private static final Map<String, DataType> SQL_TO_ES = new HashMap<>(45);
    static {
        // first add ES types
        for (DataType type : DataType.values()) {
            if (type.isPrimitive()) {
                SQL_TO_ES.put(type.name(), type);
            }
        }

        // reuse the ODBC definition (without SQL_)
        // note that this will override existing types in particular FLOAT
        for (Entry<String, DataType> entry : ODBC_TO_ES.entrySet()) {
            SQL_TO_ES.put(entry.getKey().substring(4), entry.getValue());
        }

        // special ones
        SQL_TO_ES.put("BOOL", DataType.BOOLEAN);
        SQL_TO_ES.put("INT", DataType.INTEGER);
        SQL_TO_ES.put("STRING", DataType.KEYWORD);
    }


    private final String typeName;

    private final String esType;

    /**
     * Compatible JDBC type
     */
    private final SQLType sqlType;

    private final int size;

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
        this(null, sqlType, size, defaultPrecision, displaySize, isInteger, isRational, defaultDocValues);
    }

    DataType(String esType, SQLType sqlType, int size, int defaultPrecision, int displaySize, boolean isInteger,
             boolean isRational, boolean defaultDocValues) {
        this.typeName = name().toLowerCase(Locale.ROOT);
        this.esType = esType;
        this.sqlType = sqlType;
        this.size = size;
        this.defaultPrecision = defaultPrecision;
        this.displaySize = displaySize;
        this.isInteger = isInteger;
        this.isRational = isRational;
        this.defaultDocValues = defaultDocValues;
    }

    public String typeName() {
        return typeName;
    }

    public String esType() {
        return esType;
    }

    public SQLType sqlType() {
        return sqlType;
    }

    /**
     * Size of the type in bytes
     * <p>
     * -1 if the size can vary
     */
    public int size() {
        return size;
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

    public boolean isNull() {
        return this == NULL;
    }

    public boolean isNullOrNumeric() {
        return isNull() || isNumeric();
    }

    public boolean isNullOrInterval() {
        return isNull() || isInterval();
    }

    public boolean isString() {
        return this == KEYWORD || this == TEXT;
    }

    public boolean isPrimitive() {
        return this != OBJECT && this != NESTED && this != UNSUPPORTED;
    }

    public boolean isGeo() {
        return this == GEO_POINT || this == GEO_SHAPE || this == SHAPE;
    }

    public boolean isDateBased() {
        return this == DATE || this == DATETIME;
    }

    public boolean isTimeBased() {
        return this == TIME;
    }

    public boolean isDateOrTimeBased() {
        return isDateBased() || isTimeBased();
    }

    public boolean isInterval() {
        int ordinal = this.ordinal();
        return ordinal >= INTERVAL_YEAR.ordinal() && ordinal <= INTERVAL_MINUTE_TO_SECOND.ordinal();
    }

    public boolean isYearMonthInterval() {
        return this == INTERVAL_YEAR || this == INTERVAL_MONTH || this == INTERVAL_YEAR_TO_MONTH;
    }

    public boolean isDayTimeInterval() {
        int ordinal = this.ordinal();
        return (ordinal >= INTERVAL_DAY.ordinal() && ordinal <= INTERVAL_SECOND.ordinal())
                || (ordinal >= INTERVAL_DAY_TO_HOUR.ordinal() && ordinal <= INTERVAL_MINUTE_TO_SECOND.ordinal());
    }
    
    // data type extract-able from _source or from docvalue_fields
    public boolean isFromDocValuesOnly() {
        return this == KEYWORD  // because of ignore_above. Extracting this from _source wouldn't make sense if it wasn't indexed at all.
                || this == DATE         // because of date formats
                || this == DATETIME
                || this == SCALED_FLOAT // because of scaling_factor
                || this == GEO_POINT
                || this == GEO_SHAPE
                || this == SHAPE;
    }

    public static DataType fromOdbcType(String odbcType) {
        return ODBC_TO_ES.get(odbcType);
    }

    public static DataType fromSqlOrEsType(String typeName) {
        return SQL_TO_ES.get(typeName.toUpperCase(Locale.ROOT));
    }

    /**
     * Creates returns DataType enum corresponding to the specified es type
     */
    public static DataType fromTypeName(String esType) {
        String uppercase = esType.toUpperCase(Locale.ROOT);
        if (uppercase.equals("DATE")) {
            return DataType.DATETIME;
        }
        try {
            return DataType.valueOf(uppercase);
        } catch (IllegalArgumentException ex) {
            return DataType.UNSUPPORTED;
        }
    }

    public String format() {

        return isDateOrTimeBased() ? DateUtils.DATE_PARSE_FORMAT : null;
    }

    /**
     * Returns the appropriate NumberType enum corresponding to this es type
     */
    public NumberType numberType() {
        return NumberType.valueOf(esType.toUpperCase(Locale.ROOT));
    }
}
