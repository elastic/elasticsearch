/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.GeoShape;
import org.elasticsearch.xpack.sql.expression.literal.Interval;

import java.time.OffsetTime;
import java.time.ZonedDateTime;

import static org.elasticsearch.xpack.sql.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.sql.type.DataType.BYTE;
import static org.elasticsearch.xpack.sql.type.DataType.DATETIME;
import static org.elasticsearch.xpack.sql.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.sql.type.DataType.FLOAT;
import static org.elasticsearch.xpack.sql.type.DataType.INTEGER;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_DAY;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_DAY_TO_HOUR;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_MINUTE_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_MONTH;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_SECOND;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_YEAR;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_YEAR_TO_MONTH;
import static org.elasticsearch.xpack.sql.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.sql.type.DataType.LONG;
import static org.elasticsearch.xpack.sql.type.DataType.NULL;
import static org.elasticsearch.xpack.sql.type.DataType.SHORT;
import static org.elasticsearch.xpack.sql.type.DataType.TIME;
import static org.elasticsearch.xpack.sql.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.sql.type.DataType.fromTypeName;

public final class DataTypes {

    private DataTypes() {}

    public static boolean isNull(DataType from) {
        return from == NULL;
    }

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
        throw new SqlIllegalArgumentException("No idea what's the DataType for {}", value.getClass());
    }


    //
    // Interval utilities
    //
    // some of the methods below could have used an EnumSet however isDayTime would have required a large initialization block
    // for this reason, these use the ordinal directly (and thus avoid the type check in EnumSet)

    public static boolean isInterval(DataType type) {
        int ordinal = type.ordinal();
        return ordinal >= INTERVAL_YEAR.ordinal() && ordinal <= INTERVAL_MINUTE_TO_SECOND.ordinal();
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

    private static boolean isYearMonthInterval(DataType type) {
        return type == INTERVAL_YEAR || type == INTERVAL_MONTH || type == INTERVAL_YEAR_TO_MONTH;
    }

    private static boolean isDayTimeInterval(DataType type) {
        int ordinal = type.ordinal();
        return (ordinal >= INTERVAL_DAY.ordinal() && ordinal <= INTERVAL_SECOND.ordinal())
                || (ordinal >= INTERVAL_DAY_TO_HOUR.ordinal() && ordinal <= INTERVAL_MINUTE_TO_SECOND.ordinal());
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
                throw new SqlIllegalArgumentException("Unknown unit {}", unitChar);
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
        return t.sqlType.getVendorTypeNumber();
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
                (left == DataType.NULL || right == DataType.NULL) ||
                    (left.isString() && right.isString()) ||
                    (left.isNumeric() && right.isNumeric());
        }
    }
}
