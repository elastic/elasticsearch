/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.function.scalar.geo.GeoShape;
import org.elasticsearch.xpack.ql.expression.literal.Interval;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DefaultDataTypeRegistry;

import java.sql.JDBCType;
import java.sql.SQLType;
import java.time.OffsetTime;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import static org.elasticsearch.xpack.ql.type.DataTypes.BINARY;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.BYTE;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.HALF_FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.IP;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.NESTED;
import static org.elasticsearch.xpack.ql.type.DataTypes.NULL;
import static org.elasticsearch.xpack.ql.type.DataTypes.OBJECT;
import static org.elasticsearch.xpack.ql.type.DataTypes.SCALED_FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.SHORT;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSUPPORTED;
import static org.elasticsearch.xpack.ql.util.CollectionUtils.mapSize;

public class SqlDataTypes {

    // @formatter:off
    // date-only, time-only
    public static final DataType DATE = new DataType(Long.BYTES, false, false, true);
    public static final DataType TIME = new DataType(Long.BYTES, false, false, true);
    // interval
    public static final DataType INTERVAL_YEAR =             new DataType(Integer.BYTES, false, false, false);
    public static final DataType INTERVAL_MONTH =            new DataType(Integer.BYTES, false, false, false);
    public static final DataType INTERVAL_DAY =              new DataType(Long.BYTES,    false, false, false);
    public static final DataType INTERVAL_HOUR =             new DataType(Long.BYTES,    false, false, false);
    public static final DataType INTERVAL_MINUTE =           new DataType(Long.BYTES,    false, false, false);
    public static final DataType INTERVAL_SECOND =           new DataType(Long.BYTES,    false, false, false);
    public static final DataType INTERVAL_YEAR_TO_MONTH =    new DataType(Integer.BYTES, false, false, false);
    public static final DataType INTERVAL_DAY_TO_HOUR =      new DataType(Long.BYTES,    false, false, false);
    public static final DataType INTERVAL_DAY_TO_MINUTE =    new DataType(Long.BYTES,    false, false, false);
    public static final DataType INTERVAL_DAY_TO_SECOND =    new DataType(Long.BYTES,    false, false, false);
    public static final DataType INTERVAL_HOUR_TO_MINUTE =   new DataType(Long.BYTES,    false, false, false);
    public static final DataType INTERVAL_HOUR_TO_SECOND =   new DataType(Long.BYTES,    false, false, false);
    public static final DataType INTERVAL_MINUTE_TO_SECOND = new DataType(Long.BYTES,    false, false, false);
    // geo
    public static final DataType GEO_SHAPE = new DataType(Integer.MAX_VALUE, false, false, false);
    public static final DataType GEO_POINT = new DataType(Double.BYTES * 2,  false, false, false);
    public static final DataType SHAPE =     new DataType(Integer.MAX_VALUE, false, false, false);
    
    // @formatter:on

    private static final Map<String, DataType> ODBC_TO_ES = new HashMap<>(mapSize(38));

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

    private static final Map<String, DataType> SQL_TO_ES = new HashMap<>(mapSize(45));

    static {
        // first add ES types
        for (DataType type : DefaultDataTypeRegistry.INSTANCE.dataTypes()) {
            if (type != OBJECT && type != NESTED && type != UNSUPPORTED) {
                SQL_TO_ES.put(type.typeName().toUpperCase(Locale.ROOT), type);
            }
        }

        // reuse the ODBC definition (without SQL_)
        // note that this will override existing types in particular FLOAT
        for (Entry<String, DataType> entry : ODBC_TO_ES.entrySet()) {
            SQL_TO_ES.put(entry.getKey().substring(4), entry.getValue());
        }

        // special ones
        SQL_TO_ES.put("BOOL", BOOLEAN);
        SQL_TO_ES.put("INT", INTEGER);
        SQL_TO_ES.put("STRING", KEYWORD);
    }
    private SqlDataTypes() {}

    static DataType fromJava(Object value) {
        if (value instanceof OffsetTime) {
            return TIME;
        }
        if (value instanceof Interval) {
            return ((Interval<?>) value).dataType();
        }
        if (value instanceof GeoShape) {
            return GEO_SHAPE;
        }

        return null;
    }

    public static boolean isFromDocValuesOnly(DataType dataType) {
        return dataType == KEYWORD  // because of ignore_above. Extracting this from _source wouldn't make sense if it wasn't indexed at all.
                || dataType == DATE         // because of date formats
                || dataType == DATETIME
                || dataType == SCALED_FLOAT // because of scaling_factor
                || dataType == GEO_POINT
                || dataType == GEO_SHAPE
                || dataType == SHAPE;
    }

    public static DataType fromOdbcType(String odbcType) {
        return ODBC_TO_ES.get(odbcType);
    }

    public static DataType fromSqlOrEsType(String typeName) {
        return SQL_TO_ES.get(typeName.toUpperCase(Locale.ROOT));
    }

    public static SQLType sqlType(DataType dataType) {
        if (dataType == UNSUPPORTED) {
            return JDBCType.OTHER;
        }
        if (dataType == NULL) {
            return JDBCType.NULL;
        }
        if (dataType == BOOLEAN) {
            return JDBCType.BOOLEAN;
        }
        if (dataType == BYTE) {
            return JDBCType.TINYINT;
        }
        if (dataType == SHORT) {
            return JDBCType.SMALLINT;
        }
        if (dataType == INTEGER) {
            return JDBCType.INTEGER;
        }
        if (dataType == LONG) {
            return JDBCType.BIGINT;
        }
        if (dataType == DOUBLE) {
            return JDBCType.DOUBLE;
        }
        if (dataType == FLOAT) {
            return JDBCType.REAL;
        }
        if (dataType == HALF_FLOAT) {
            return JDBCType.FLOAT;
        }
        if (dataType == SCALED_FLOAT) {
            return JDBCType.DOUBLE;
        }
        if (dataType == KEYWORD) {
            return JDBCType.VARCHAR;
        }
        if (dataType == TEXT) {
            return JDBCType.VARCHAR;
        }
        if (dataType == DATETIME) {
            return JDBCType.TIMESTAMP;
        }
        if (dataType == IP) {
            return JDBCType.VARCHAR;
        }
        if (dataType == BINARY) {
            return JDBCType.BINARY;
        }
        if (dataType == OBJECT) {
            return JDBCType.STRUCT;
        }
        if (dataType == NESTED) {
            return JDBCType.STRUCT;
        }
        //
        // SQL specific
        //
        if (dataType == DATE) {
            return JDBCType.DATE;
        }
        if (dataType == TIME) {
            return JDBCType.TIME;
        }
        if (dataType == GEO_SHAPE) {
            return ExtTypes.GEOMETRY;
        }
        if (dataType == GEO_POINT) {
            return ExtTypes.GEOMETRY;
        }
        if (dataType == SHAPE) {
            return ExtTypes.GEOMETRY;
        }
        if (dataType == INTERVAL_YEAR) {
            return ExtTypes.INTERVAL_YEAR;
        }
        if (dataType == INTERVAL_MONTH) {
            return ExtTypes.INTERVAL_MONTH;
        }
        if (dataType == INTERVAL_DAY) {
            return ExtTypes.INTERVAL_DAY;
        }
        if (dataType == INTERVAL_HOUR) {
            return ExtTypes.INTERVAL_HOUR;
        }
        if (dataType == INTERVAL_MINUTE) {
            return ExtTypes.INTERVAL_MINUTE;
        }
        if (dataType == INTERVAL_SECOND) {
            return ExtTypes.INTERVAL_SECOND;
        }
        if (dataType == INTERVAL_YEAR_TO_MONTH) {
            return ExtTypes.INTERVAL_YEAR_TO_MONTH;
        }
        if (dataType == INTERVAL_DAY_TO_HOUR) {
            return ExtTypes.INTERVAL_DAY_TO_HOUR;
        }
        if (dataType == INTERVAL_DAY_TO_MINUTE) {
            return ExtTypes.INTERVAL_DAY_TO_MINUTE;
        }
        if (dataType == INTERVAL_DAY_TO_SECOND) {
            return ExtTypes.INTERVAL_DAY_TO_SECOND;
        }
        if (dataType == INTERVAL_HOUR_TO_MINUTE) {
            return ExtTypes.INTERVAL_HOUR_TO_MINUTE;
        }
        if (dataType == INTERVAL_HOUR_TO_SECOND) {
            return ExtTypes.INTERVAL_HOUR_TO_SECOND;
        }
        if (dataType == INTERVAL_MINUTE_TO_SECOND) {
            return ExtTypes.INTERVAL_MINUTE_TO_SECOND;
        }

        return null;
    }

    /**
     * Returns the precision of the field
     * <p>
     * Precision is the specified column size. For numeric data, this is the maximum precision. For character
     * data, this is the length in characters. For datetime datatypes, this is the length in characters of the
     * String representation (assuming the maximum allowed defaultPrecision of the fractional seconds component).
     */
    public static int defaultPrecision(DataType dataType) {
        if (dataType == UNSUPPORTED) {
            return 0;
        }
        if (dataType == NULL) {
            return 0;
        }
        if (dataType == BOOLEAN) {
            return 1;
        }
        if (dataType == BYTE) {
            return 3;
        }
        if (dataType == SHORT) {
            return 5;
        }
        if (dataType == INTEGER) {
            return 10;
        }
        if (dataType == LONG) {
            return 19;
        }
        if (dataType == DOUBLE) {
            return 15;
        }
        if (dataType == FLOAT) {
            return 7;
        }
        if (dataType == HALF_FLOAT) {
            return 3;
        }
        if (dataType == SCALED_FLOAT) {
            return 15;
        }
        if (dataType == KEYWORD) {
            return 15;
        }
        if (dataType == TEXT) {
            return Short.MAX_VALUE;
        }
        if (dataType == DATETIME) {
            return 3;
        }
        if (dataType == IP) {
            return 39;
        }
        if (dataType == BINARY) {
            return Integer.MAX_VALUE;
        }
        if (dataType == OBJECT) {
            return 0;
        }
        if (dataType == NESTED) {
            return 0;
        }
        //
        // SQL specific
        //
        // since ODBC and JDBC interpret precision for Date as display size
        // the precision is 23 (number of chars in ISO8601 with millis) + 6 chars for the timezone (e.g.: +05:00)
        // see https://github.com/elastic/elasticsearch/issues/30386#issuecomment-386807288
        if (dataType == DATE) {
            return 3;
        }
        if (dataType == TIME) {
            return 3;
        }

        if (dataType == GEO_SHAPE) {
            return Integer.MAX_VALUE;
        }
        if (dataType == GEO_POINT) {
            return Integer.MAX_VALUE;
        }
        if (dataType == SHAPE) {
            return Integer.MAX_VALUE;
        }
        if (dataType == INTERVAL_YEAR) {
            return 7;
        }
        if (dataType == INTERVAL_MONTH) {
            return 7;
        }
        if (dataType == INTERVAL_DAY) {
            return 23;
        }
        if (dataType == INTERVAL_HOUR) {
            return 23;
        }
        if (dataType == INTERVAL_MINUTE) {
            return 23;
        }
        if (dataType == INTERVAL_SECOND) {
            return 23;
        }
        if (dataType == INTERVAL_YEAR_TO_MONTH) {
            return 7;
        }
        if (dataType == INTERVAL_DAY_TO_HOUR) {
            return 23;
        }
        if (dataType == INTERVAL_DAY_TO_MINUTE) {
            return 23;
        }
        if (dataType == INTERVAL_DAY_TO_SECOND) {
            return 23;
        }
        if (dataType == INTERVAL_HOUR_TO_MINUTE) {
            return 23;
        }
        if (dataType == INTERVAL_HOUR_TO_SECOND) {
            return 23;
        }
        if (dataType == INTERVAL_MINUTE_TO_SECOND) {
            return 23;
        }

        return 0;
    }

    public static int displaySize(DataType dataType) {
        if (dataType == UNSUPPORTED) {
            return 0;
        }
        if (dataType == NULL) {
            return 0;
        }
        if (dataType == BOOLEAN) {
            return 1;
        }
        if (dataType == BYTE) {
            return 5;
        }
        if (dataType == SHORT) {
            return 6;
        }
        if (dataType == INTEGER) {
            return 11;
        }
        if (dataType == LONG) {
            return 20;
        }
        if (dataType == DOUBLE) {
            return 25;
        }
        if (dataType == FLOAT) {
            return 15;
        }
        if (dataType == HALF_FLOAT) {
            return 25;
        }
        if (dataType == SCALED_FLOAT) {
            return 25;
        }
        if (dataType == KEYWORD) {
            return Short.MAX_VALUE;
        }
        if (dataType == TEXT) {
            return Integer.MAX_VALUE;
        }
        if (dataType == DATETIME) {
            return 29;
        }
        if (dataType == IP) {
            return 0;
        }
        if (dataType == BINARY) {
            return Integer.MAX_VALUE;
        }
        if (dataType == OBJECT) {
            return 0;
        }
        if (dataType == NESTED) {
            return 0;
        }
        //
        // SQL specific
        //
        if (dataType == DATE) {
            return 29;
        }
        if (dataType == TIME) {
            return 18;
        }
        if (dataType == GEO_SHAPE) {
            return Integer.MAX_VALUE;
        }
        if (dataType == GEO_POINT) {
            //2 doubles + len("POINT( )")
            return 25 * 2 + 8;
        }
        if (dataType == SHAPE) {
            return Integer.MAX_VALUE;
        }
        if (dataType == INTERVAL_YEAR) {
            return 7;
        }
        if (dataType == INTERVAL_MONTH) {
            return 7;
        }
        if (dataType == INTERVAL_DAY) {
            return 23;
        }
        if (dataType == INTERVAL_HOUR) {
            return 23;
        }
        if (dataType == INTERVAL_MINUTE) {
            return 23;
        }
        if (dataType == INTERVAL_SECOND) {
            return 23;
        }
        if (dataType == INTERVAL_YEAR_TO_MONTH) {
            return 7;
        }
        if (dataType == INTERVAL_DAY_TO_HOUR) {
            return 23;
        }
        if (dataType == INTERVAL_DAY_TO_MINUTE) {
            return 23;
        }
        if (dataType == INTERVAL_DAY_TO_SECOND) {
            return 23;
        }
        if (dataType == INTERVAL_HOUR_TO_MINUTE) {
            return 23;
        }
        if (dataType == INTERVAL_HOUR_TO_SECOND) {
            return 23;
        }
        if (dataType == INTERVAL_MINUTE_TO_SECOND) {
            return 23;
        }

        return 0;
    }

    private static boolean isDateBased(DataType dataType) {
        return dataType == DATE || dataType == DATETIME;
    }
    public static boolean isInterval(DataType dataType) {
        int ordinal = this.ordinal();
        return ordinal >= INTERVAL_YEAR.ordinal() && ordinal <= INTERVAL_MINUTE_TO_SECOND.ordinal();
    }

    public static boolean isYearMonthInterval(DataType dataType) {
        return dataType == INTERVAL_YEAR || dataType == INTERVAL_MONTH || dataType == INTERVAL_YEAR_TO_MONTH;
    }

    public static boolean isDayTimeInterval(DataType dataType) {
        int ordinal = this.ordinal();
        return (ordinal >= INTERVAL_DAY.ordinal() && ordinal <= INTERVAL_SECOND.ordinal())
                || (ordinal >= INTERVAL_DAY_TO_HOUR.ordinal() && ordinal <= INTERVAL_MINUTE_TO_SECOND.ordinal());
    }

    // return the compatible interval between the two - it is assumed the types are intervals
    // YEAR and MONTH -> YEAR_TO_MONTH
    // DAY... SECOND -> DAY_TIME
    // YEAR_MONTH and DAY_SECOND are NOT compatible
    public static DataType compatibleInterval(DataType left, DataType right) {
        if (left == right) {
            return left;
        }
        if (isYearMonthInterval(left) && isYearMonthInterval(right)) {
            // no need to look at YEAR/YEAR or MONTH/MONTH as these are equal and already handled
            return INTERVAL_YEAR_TO_MONTH;
        }
        if (isDayTimeInterval(left) && isDayTimeInterval(right)) {
            // to avoid specifying the combinations, extract the leading and trailing unit from the name
            // D > H > S > M which is also the alphabetical order
            String lName = left.name().substring(9);
            String rName = right.name().substring(9);

            char leading = lName.charAt(0);
            if (rName.charAt(0) < leading) {
                leading = rName.charAt(0);
            }
            // look at the trailing unit
            if (lName.length() > 6) {
                int indexOf = lName.indexOf("_TO_");
                lName = lName.substring(indexOf + 4);
            }
            if (rName.length() > 6) {
                int indexOf = rName.indexOf("_TO_");
                rName = rName.substring(indexOf + 4);
            }
            char trailing = lName.charAt(0);
            if (rName.charAt(0) > trailing) {
                trailing = rName.charAt(0);
            }

            return fromTypeName("INTERVAL_" + intervalUnit(leading) + "_TO_" + intervalUnit(trailing));
        }
        return null;
    }

    private static String intervalUnit(char unitChar) {
        switch (unitChar) {
            case 'D':
                return "DAY";
            case 'H':
                return "HOUR";
            case 'M':
                return "MINUTE";
            case 'S':
                return "SECOND";
            default:
                throw new QlIllegalArgumentException("Unknown unit {}", unitChar);
        }
    }

    //
    // Metadata methods, mainly for ODBC.
    // As these are fairly obscure and limited in use, there is no point to promote them as a full type methods
    // hence why they appear here as utility methods.
    //

    // https://docs.microsoft.com/en-us/sql/relational-databases/native-client-odbc-date-time/metadata-catalog
    // https://github.com/elastic/elasticsearch/issues/30386
    public static Integer metaSqlDataType(DataType t) {
        if (t == DATETIME) {
            // ODBC SQL_DATETME
            return Integer.valueOf(9);
        }
        // this is safe since the vendor SQL types are short despite the return value
        return sqlType(t).getVendorTypeNumber();
    }

    // https://github.com/elastic/elasticsearch/issues/30386
    // https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgettypeinfo-function
    public static Integer metaSqlDateTimeSub(DataType t) {
        if (t == DATETIME) {
            // ODBC SQL_CODE_TIMESTAMP
            return Integer.valueOf(3);
        }
        // ODBC null
        return 0;
    }

    public static Short metaSqlMinimumScale(DataType t) {
        return metaSqlSameScale(t);
    }

    public static Short metaSqlMaximumScale(DataType t) {
        return metaSqlSameScale(t);
    }

    // https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/decimal-digits
    // https://github.com/elastic/elasticsearch/issues/40357
    // since the scale is fixed, minimum and maximum should return the same value
    // hence why this method exists
    private static Short metaSqlSameScale(DataType t) {
        // TODO: return info for SCALED_FLOATS (should be based on field not type)
        if (t.isInteger()) {
            return Short.valueOf((short) 0);
        }
        if (isDateBased(t) || t.isRational()) {
            return Short.valueOf((short) defaultPrecision(t));
        }
        return null;
    }

    // https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgettypeinfo-function
    public static Integer metaSqlRadix(DataType t) {
        // RADIX  - Determines how numbers returned by COLUMN_SIZE and DECIMAL_DIGITS should be interpreted.
        // 10 means they represent the number of decimal digits allowed for the column.
        // 2 means they represent the number of bits allowed for the column.
        // null means radix is not applicable for the given type.
        return t.isInteger() ? Integer.valueOf(10) : (t.isRational() ? Integer.valueOf(2) : null);
    }

    //https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgettypeinfo-function#comments
    //https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/column-size
    public static Integer precision(DataType t) {
        if (t.isNumeric()) {
            return defaultPrecision(t);
        }
        return displaySize(t);
    }
}