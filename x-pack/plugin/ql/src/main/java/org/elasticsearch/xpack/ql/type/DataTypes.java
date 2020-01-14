/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.function.scalar.geo.GeoShape;
import org.elasticsearch.xpack.ql.expression.literal.Interval;

import java.time.OffsetTime;
import java.time.ZonedDateTime;

public final class DataTypes {

    public static final DataType NULL = new DataType("null", 0, false, false, false);

    public static final DataType UNSUPPORTED = new DataType(null, 0, false, false, false);

    public static final DataType BOOLEAN = new DataType("boolean", 1, false, false, false);

    public static final DataType BYTE = new DataType("byte", Byte.BYTES, true, false, true);
    public static final DataType SHORT = new DataType("short", Short.BYTES, true, false, true);
    public static final DataType INTEGER = new DataType("integer", Integer.BYTES, true, false, true);
    public static final DataType LONG = new DataType("long", Long.BYTES, true, false, true);

    public static final DataType DOUBLE = new DataType("double", Double.BYTES, false, true, true);
    public static final DataType FLOAT = new DataType("float", Float.BYTES, false, true, true);
    public static final DataType HALF_FLOAT = new DataType("half_float", Float.BYTES, false, true, true);
    public static final DataType SCALED_FLOAT = new DataType("scaled_float", Long.BYTES, false, true, true);

    public static final DataType KEYWORD = new DataType("keyword", Short.MAX_VALUE, true, false, true);
    public static final DataType TEXT = new DataType("text", Integer.MAX_VALUE, true, false, true);

    public static final DataType DATETIME = new DataType("date", Long.BYTES, false, false, true);
    public static final DataType IP = new DataType("ip", 39, false, false, true);

    public static final DataType BINARY = new DataType("binary", Integer.MAX_VALUE, false, false, true);

    public static final DataType OBJECT = new DataType("object", 0, false, false, true);
    public static final DataType NESTED = new DataType("nested", 0, false, false, true);


    private DataTypes() {}

    public static boolean isUnsupported(DataType from) {
        return from == UNSUPPORTED;
    }

    public static DataType fromJava(Object value) {
        if (value == null) {
            return NULL;
        }
        if (value instanceof Integer) {
            return INTEGER;
        }
        if (value instanceof Long) {
            return LONG;
        }
        if (value instanceof Boolean) {
            return BOOLEAN;
        }
        if (value instanceof Double) {
            return DOUBLE;
        }
        if (value instanceof Float) {
            return FLOAT;
        }
        if (value instanceof Byte) {
            return BYTE;
        }
        if (value instanceof Short) {
            return SHORT;
        }
        if (value instanceof OffsetTime) {
            return TIME;
        }
        if (value instanceof ZonedDateTime) {
            return DATETIME;
        }
        if (value instanceof String || value instanceof Character) {
            return KEYWORD;
        }
        if (value instanceof Interval) {
            return ((Interval<?>) value).dataType();
        }
        if (value instanceof GeoShape) {
            return DataType.GEO_SHAPE;
        }

        throw new QlIllegalArgumentException("No idea what's the DataType for {}", value.getClass());
    }

    // return the compatible interval between the two - it is assumed the types are intervals
    // YEAR and MONTH -> YEAR_TO_MONTH
    // DAY... SECOND -> DAY_TIME
    // YEAR_MONTH and DAY_SECOND are NOT compatible
    public static DataType compatibleInterval(DataType left, DataType right) {
        if (left == right) {
            return left;
        }
        if (left.isYearMonthInterval() && right.isYearMonthInterval()) {
            // no need to look at YEAR/YEAR or MONTH/MONTH as these are equal and already handled
            return INTERVAL_YEAR_TO_MONTH;
        }
        if (left.isDayTimeInterval() && right.isDayTimeInterval()) {
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
        return t.sqlType().getVendorTypeNumber();
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
        if (t.isDateBased() || t.isRational()) {
            return Short.valueOf((short) t.defaultPrecision);
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
            return t.defaultPrecision;
        }
        return t.displaySize;
    }

    public static boolean areTypesCompatible(DataType left, DataType right) {
        if (left == right) {
            return true;
        } else {
            return
                (left == DataType.NULL || right == DataType.NULL)
                    || (left.isString() && right.isString())
                    || (left.isNumeric() && right.isNumeric())
                    || (left.isDateBased() && right.isDateBased())
                    || (left.isInterval() && right.isInterval() && compatibleInterval(left, right) != null);
        }
    }
}
