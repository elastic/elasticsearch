/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.xpack.sql.proto.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.ERA;
import static java.util.Calendar.HOUR_OF_DAY;
import static java.util.Calendar.MILLISECOND;
import static java.util.Calendar.MINUTE;
import static java.util.Calendar.MONTH;
import static java.util.Calendar.SECOND;
import static java.util.Calendar.YEAR;
import static org.elasticsearch.xpack.sql.jdbc.EsType.DATE;
import static org.elasticsearch.xpack.sql.jdbc.EsType.DATETIME;
import static org.elasticsearch.xpack.sql.jdbc.EsType.TIME;
import static org.elasticsearch.xpack.sql.jdbc.JdbcDateUtils.asDateTimeField;
import static org.elasticsearch.xpack.sql.jdbc.JdbcDateUtils.timeAsTime;

/**
 * Conversion utilities for conversion of JDBC types to Java type and back
 * <p>
 * Only the following JDBC types are supported as part of Elasticsearch response and parameters.
 * See org.elasticsearch.xpack.sql.type.DataType for details.
 * <p>
 * NULL, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DOUBLE, REAL, FLOAT, VARCHAR, VARBINARY and TIMESTAMP
 */
final class TypeConverter {

    private TypeConverter() {}

    /**
     * Converts millisecond after epoc to date
     */
    static Date convertDate(Long millis, Calendar cal) {
        return dateTimeConvert(millis, cal, c -> {
            c.set(HOUR_OF_DAY, 0);
            c.set(MINUTE, 0);
            c.set(SECOND, 0);
            c.set(MILLISECOND, 0);
            return new Date(c.getTimeInMillis());
        });
    }

    /**
     * Converts millisecond after epoc to time
     */
    static Time convertTime(Long millis, Calendar cal) {
        return dateTimeConvert(millis, cal, c -> {
            c.set(ERA, GregorianCalendar.AD);
            c.set(YEAR, 1970);
            c.set(MONTH, 0);
            c.set(DAY_OF_MONTH, 1);
            return new Time(c.getTimeInMillis());
        });
    }

    /**
     * Converts millisecond after epoch to timestamp
     */
    static Timestamp convertTimestamp(Long millis, int nanos, Calendar cal) {
        Timestamp ts = dateTimeConvert(millis, cal, c -> new Timestamp(c.getTimeInMillis()));
        if (ts != null) {
            ts.setNanos(nanos);
        }
        return ts;
    }

    private static <T> T dateTimeConvert(Long millis, Calendar c, Function<Calendar, T> creator) {
        if (millis == null) {
            return null;
        }
        long initial = c.getTimeInMillis();
        try {
            c.setTimeInMillis(millis);
            return creator.apply(c);
        } finally {
            c.setTimeInMillis(initial);
        }
    }

    static long convertFromCalendarToUTC(long value, Calendar cal) {
        if (cal == null) {
            return value;
        }
        Calendar c = (Calendar) cal.clone();
        c.setTimeInMillis(value);

        ZonedDateTime convertedDateTime = ZonedDateTime.ofInstant(c.toInstant(), c.getTimeZone().toZoneId())
            .withZoneSameLocal(ZoneOffset.UTC);

        return convertedDateTime.toInstant().toEpochMilli();
    }

    /**
     * Converts object val from columnType to type
     */
    @SuppressWarnings("unchecked")
    static <T> T convert(Object val, EsType columnType, Class<T> type, String typeString) throws SQLException {
        if (type == null) {
            return (T) convert(val, columnType, typeString);
        }

        // if the value type is the same as the target, no conversion is needed
        // make sure though to check the internal type against the desired one
        // since otherwise the internal object format can leak out
        // (for example dates when longs are requested or intervals for strings)
        if (type.isInstance(val) && TypeUtils.classOf(columnType) == type) {
            try {
                return type.cast(val);
            } catch (ClassCastException cce) {
                failConversion(val, columnType, typeString, type, cce);
            }
        }

        if (type == String.class) {
            return (T) asString(convert(val, columnType, typeString));
        }
        if (type == Boolean.class) {
            return (T) asBoolean(val, columnType, typeString);
        }
        if (type == Byte.class) {
            return (T) asByte(val, columnType, typeString);
        }
        if (type == Short.class) {
            return (T) asShort(val, columnType, typeString);
        }
        if (type == Integer.class) {
            return (T) asInteger(val, columnType, typeString);
        }
        if (type == Long.class) {
            return (T) asLong(val, columnType, typeString);
        }
        if (type == BigInteger.class) {
            return (T) asBigInteger(val, columnType, typeString);
        }
        if (type == Float.class) {
            return (T) asFloat(val, columnType, typeString);
        }
        if (type == Double.class) {
            return (T) asDouble(val, columnType, typeString);
        }
        if (type == Date.class) {
            return (T) asDate(val, columnType, typeString);
        }
        if (type == Time.class) {
            return (T) asTime(val, columnType, typeString);
        }
        if (type == Timestamp.class) {
            return (T) asTimestamp(val, columnType, typeString);
        }
        if (type == byte[].class) {
            return (T) asByteArray(val, columnType, typeString);
        }
        if (type == BigDecimal.class) {
            return (T) asBigDecimal(val, columnType, typeString);
        }
        //
        // JDK 8 types
        //
        if (type == LocalDate.class) {
            return (T) asLocalDate(val, columnType, typeString);
        }
        if (type == LocalTime.class) {
            return (T) asLocalTime(val, columnType, typeString);
        }
        if (type == LocalDateTime.class) {
            return (T) asLocalDateTime(val, columnType, typeString);
        }
        if (type == OffsetTime.class) {
            return (T) asOffsetTime(val, columnType, typeString);
        }
        if (type == OffsetDateTime.class) {
            return (T) asOffsetDateTime(val, columnType, typeString);
        }

        return failConversion(val, columnType, typeString, type);
    }

    /**
     * Converts the object from JSON representation to the specified JDBCType
     */
    static Object convert(Object v, EsType columnType, String typeString) throws SQLException {
        switch (columnType) {
            case NULL:
                return null;
            case BOOLEAN:
            case TEXT:
            case KEYWORD:
                return v; // These types are already represented correctly in JSON
            case BYTE:
                return ((Number) v).byteValue(); // Parser might return it as integer or long - need to update to the correct type
            case SHORT:
                return ((Number) v).shortValue(); // Parser might return it as integer or long - need to update to the correct type
            case INTEGER:
                return ((Number) v).intValue();
            case LONG:
                return ((Number) v).longValue();
            case UNSIGNED_LONG:
                return asBigInteger(v, columnType, typeString);
            case HALF_FLOAT:
            case SCALED_FLOAT:
            case DOUBLE:
                return doubleValue(v); // Double might be represented as string for infinity and NaN values
            case FLOAT:
                return floatValue(v); // Float might be represented as string for infinity and NaN values
            case DATE:
                return asDateTimeField(v, JdbcDateUtils::asDate, Date::new);
            case TIME:
                return timeAsTime(v.toString());
            case DATETIME:
                return asDateTimeField(v, JdbcDateUtils::asTimestamp, Timestamp::new);
            case INTERVAL_YEAR:
            case INTERVAL_MONTH:
            case INTERVAL_YEAR_TO_MONTH:
                return Period.parse(v.toString());
            case INTERVAL_DAY:
            case INTERVAL_HOUR:
            case INTERVAL_MINUTE:
            case INTERVAL_SECOND:
            case INTERVAL_DAY_TO_HOUR:
            case INTERVAL_DAY_TO_MINUTE:
            case INTERVAL_DAY_TO_SECOND:
            case INTERVAL_HOUR_TO_MINUTE:
            case INTERVAL_HOUR_TO_SECOND:
            case INTERVAL_MINUTE_TO_SECOND:
                return Duration.parse(v.toString());
            case GEO_POINT:
            case GEO_SHAPE:
            case SHAPE:
            case IP:
            case VERSION:
                return v.toString();
            default:
                throw new SQLException("Unexpected column type [" + typeString + "]");

        }
    }

    private static Double doubleValue(Object v) {
        if (v instanceof String) {
            switch ((String) v) {
                case "NaN":
                    return Double.NaN;
                case "Infinity":
                    return Double.POSITIVE_INFINITY;
                case "-Infinity":
                    return Double.NEGATIVE_INFINITY;
                default:
                    return Double.parseDouble((String) v);
            }
        }
        return ((Number) v).doubleValue();
    }

    private static Float floatValue(Object v) {
        if (v instanceof String) {
            switch ((String) v) {
                case "NaN":
                    return Float.NaN;
                case "Infinity":
                    return Float.POSITIVE_INFINITY;
                case "-Infinity":
                    return Float.NEGATIVE_INFINITY;
                default:
                    return Float.parseFloat((String) v);
            }
        }
        return ((Number) v).floatValue();
    }

    private static String asString(Object nativeValue) {
        return nativeValue == null ? null : StringUtils.toString(nativeValue);
    }

    private static <T> T failConversion(Object value, EsType columnType, String typeString, Class<T> target) throws SQLException {
        return failConversion(value, columnType, typeString, target, null);
    }

    private static <T> T failConversion(Object value, EsType columnType, String typeString, Class<T> target, Exception e)
        throws SQLException {
        String message = format(Locale.ROOT, "Unable to convert value [%.128s] of type [%s] to [%s]", value, columnType, typeString);
        throw e != null ? new SQLException(message, e) : new SQLException(message);
    }

    private static Boolean asBoolean(Object val, EsType columnType, String typeString) throws SQLException {
        switch (columnType) {
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
            case UNSIGNED_LONG:
            case FLOAT:
            case HALF_FLOAT:
            case SCALED_FLOAT:
            case DOUBLE:
                return Boolean.valueOf(((Number) val).doubleValue() != 0);
            case KEYWORD:
            case TEXT:
                return Boolean.valueOf((String) val);
            default:
                return failConversion(val, columnType, typeString, Boolean.class);
        }
    }

    private static Byte asByte(Object val, EsType columnType, String typeString) throws SQLException {
        switch (columnType) {
            case BOOLEAN:
                return Byte.valueOf(((Boolean) val).booleanValue() ? (byte) 1 : (byte) 0);
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                return safeToByte(((Number) val).longValue());
            case UNSIGNED_LONG:
                return safeToByte(asBigInteger(val, columnType, typeString));
            case FLOAT:
            case HALF_FLOAT:
            case SCALED_FLOAT:
            case DOUBLE:
                return safeToByte(safeToLong(((Number) val).doubleValue()));
            case KEYWORD:
            case TEXT:
                try {
                    return Byte.valueOf((String) val);
                } catch (NumberFormatException e) {
                    return failConversion(val, columnType, typeString, Byte.class, e);
                }
            default:
        }

        return failConversion(val, columnType, typeString, Byte.class);
    }

    private static Short asShort(Object val, EsType columnType, String typeString) throws SQLException {
        switch (columnType) {
            case BOOLEAN:
                return Short.valueOf(((Boolean) val).booleanValue() ? (short) 1 : (short) 0);
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                return safeToShort(((Number) val).longValue());
            case UNSIGNED_LONG:
                return safeToShort(asBigInteger(val, columnType, typeString));
            case FLOAT:
            case HALF_FLOAT:
            case SCALED_FLOAT:
            case DOUBLE:
                return safeToShort(safeToLong(((Number) val).doubleValue()));
            case KEYWORD:
            case TEXT:
                try {
                    return Short.valueOf((String) val);
                } catch (NumberFormatException e) {
                    return failConversion(val, columnType, typeString, Short.class, e);
                }
            default:
        }
        return failConversion(val, columnType, typeString, Short.class);
    }

    private static Integer asInteger(Object val, EsType columnType, String typeString) throws SQLException {
        switch (columnType) {
            case BOOLEAN:
                return Integer.valueOf(((Boolean) val).booleanValue() ? 1 : 0);
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                return safeToInt(((Number) val).longValue());
            case UNSIGNED_LONG:
                return safeToInt(asBigInteger(val, columnType, typeString));
            case FLOAT:
            case HALF_FLOAT:
            case SCALED_FLOAT:
            case DOUBLE:
                return safeToInt(safeToLong(((Number) val).doubleValue()));
            case KEYWORD:
            case TEXT:
                try {
                    return Integer.valueOf((String) val);
                } catch (NumberFormatException e) {
                    return failConversion(val, columnType, typeString, Integer.class, e);
                }
            default:
        }
        return failConversion(val, columnType, typeString, Integer.class);
    }

    private static Long asLong(Object val, EsType columnType, String typeString) throws SQLException {
        switch (columnType) {
            case BOOLEAN:
                return Long.valueOf(((Boolean) val).booleanValue() ? 1 : 0);
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                return Long.valueOf(((Number) val).longValue());
            case UNSIGNED_LONG:
                return safeToLong(asBigInteger(val, columnType, typeString));
            case FLOAT:
            case HALF_FLOAT:
            case SCALED_FLOAT:
            case DOUBLE:
                return safeToLong(((Number) val).doubleValue());
            // TODO: should we support conversion to TIMESTAMP?
            // The spec says that getLong() should support the following types conversions:
            // TINYINT, SMALLINT, INTEGER, BIGINT, REAL, FLOAT, DOUBLE, DECIMAL, NUMERIC, BIT, BOOLEAN, CHAR, VARCHAR, LONGVARCHAR
            // case TIMESTAMP:
            // return ((Number) val).longValue();
            case KEYWORD:
            case TEXT:
                try {
                    return Long.valueOf((String) val);
                } catch (NumberFormatException e) {
                    return failConversion(val, columnType, typeString, Long.class, e);
                }
            default:
        }

        return failConversion(val, columnType, typeString, Long.class);
    }

    private static Float asFloat(Object val, EsType columnType, String typeString) throws SQLException {
        switch (columnType) {
            case BOOLEAN:
                return Float.valueOf(((Boolean) val).booleanValue() ? 1 : 0);
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                return Float.valueOf(((Number) val).longValue());
            case UNSIGNED_LONG:
                return asBigInteger(val, columnType, typeString).floatValue();
            case FLOAT:
            case HALF_FLOAT:
            case SCALED_FLOAT:
            case DOUBLE:
                return Float.valueOf(((Number) val).floatValue());
            case KEYWORD:
            case TEXT:
                try {
                    return Float.valueOf((String) val);
                } catch (NumberFormatException e) {
                    return failConversion(val, columnType, typeString, Float.class, e);
                }
            default:
        }
        return failConversion(val, columnType, typeString, Float.class);
    }

    private static Double asDouble(Object val, EsType columnType, String typeString) throws SQLException {
        switch (columnType) {
            case BOOLEAN:
                return Double.valueOf(((Boolean) val).booleanValue() ? 1 : 0);
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                return Double.valueOf(((Number) val).longValue());
            case UNSIGNED_LONG:
                return asBigInteger(val, columnType, typeString).doubleValue();
            case FLOAT:
            case HALF_FLOAT:
            case SCALED_FLOAT:
            case DOUBLE:
                return Double.valueOf(((Number) val).doubleValue());
            case KEYWORD:
            case TEXT:
                try {
                    return Double.valueOf((String) val);
                } catch (NumberFormatException e) {
                    return failConversion(val, columnType, typeString, Double.class, e);
                }
            default:
        }
        return failConversion(val, columnType, typeString, Double.class);
    }

    private static Date asDate(Object val, EsType columnType, String typeString) throws SQLException {
        if (columnType == DATETIME || columnType == DATE) {
            return asDateTimeField(val, JdbcDateUtils::asDate, Date::new);
        }
        if (columnType == TIME) {
            return new Date(0L);
        }
        return failConversion(val, columnType, typeString, Date.class);
    }

    private static Time asTime(Object val, EsType columnType, String typeString) throws SQLException {
        if (columnType == DATETIME) {
            return asDateTimeField(val, JdbcDateUtils::asTime, Time::new);
        }
        if (columnType == TIME) {
            return asDateTimeField(val, JdbcDateUtils::timeAsTime, Time::new);
        }
        if (columnType == DATE) {
            return new Time(0L);
        }
        return failConversion(val, columnType, typeString, Time.class);
    }

    private static Timestamp asTimestamp(Object val, EsType columnType, String typeString) throws SQLException {
        if (columnType == DATETIME || columnType == DATE) {
            return asDateTimeField(val, JdbcDateUtils::asTimestamp, Timestamp::new);
        }
        if (columnType == TIME) {
            return asDateTimeField(val, JdbcDateUtils::timeAsTimestamp, Timestamp::new);
        }
        return failConversion(val, columnType, typeString, Timestamp.class);
    }

    private static byte[] asByteArray(Object val, EsType columnType, String typeString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private static BigInteger asBigInteger(Object val, EsType columnType, String typeString) throws SQLException {
        switch (columnType) {
            case BOOLEAN:
                return ((Boolean) val).booleanValue() ? BigInteger.ONE : BigInteger.ZERO;
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                return BigInteger.valueOf(((Number) val).longValue());
            case FLOAT:
            case HALF_FLOAT:
            case SCALED_FLOAT:
            case DOUBLE:
                return BigDecimal.valueOf(((Number) val).doubleValue()).toBigInteger();
            // Aggs can return floats dressed as UL types (bugUrl="https://github.com/elastic/elasticsearch/issues/65413")
            case UNSIGNED_LONG:
            case KEYWORD:
            case TEXT:
                try {
                    return new BigDecimal(val.toString()).toBigInteger();
                } catch (NumberFormatException e) {
                    return failConversion(val, columnType, typeString, BigInteger.class, e);
                }
            default:
        }
        return failConversion(val, columnType, typeString, BigInteger.class);
    }

    private static BigDecimal asBigDecimal(Object val, EsType columnType, String typeString) throws SQLException {
        switch (columnType) {
            case BOOLEAN:
                return (Boolean) val ? BigDecimal.ONE : BigDecimal.ZERO;
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                return BigDecimal.valueOf(((Number) val).longValue());
            case UNSIGNED_LONG:
                return new BigDecimal(asBigInteger(val, columnType, typeString));
            case FLOAT:
            case HALF_FLOAT:
                // floats are passed in as doubles here, so we need to dip into string to keep original float's (reduced) precision.
                return new BigDecimal(String.valueOf(((Number) val).floatValue()));
            case DOUBLE:
            case SCALED_FLOAT:
                return BigDecimal.valueOf(((Number) val).doubleValue());
            case KEYWORD:
            case TEXT:
                try {
                    return new BigDecimal((String) val);
                } catch (NumberFormatException nfe) {
                    return failConversion(val, columnType, typeString, BigDecimal.class, nfe);
                }
                // TODO: should we implement numeric - interval types conversions too; ever needed? ODBC does mandate it
                // https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/converting-data-from-c-to-sql-data-types
        }
        return failConversion(val, columnType, typeString, BigDecimal.class);
    }

    private static LocalDate asLocalDate(Object val, EsType columnType, String typeString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private static LocalTime asLocalTime(Object val, EsType columnType, String typeString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private static LocalDateTime asLocalDateTime(Object val, EsType columnType, String typeString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private static OffsetTime asOffsetTime(Object val, EsType columnType, String typeString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private static OffsetDateTime asOffsetDateTime(Object val, EsType columnType, String typeString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private static byte safeToByte(Number n) throws SQLException {
        if (n instanceof BigInteger) {
            try {
                return ((BigInteger) n).byteValueExact();
            } catch (ArithmeticException ae) {
                throw new SQLException(format(Locale.ROOT, "Numeric %s out of range", n));
            }
        }
        long x = n.longValue();
        if (x > Byte.MAX_VALUE || x < Byte.MIN_VALUE) {
            throw new SQLException(format(Locale.ROOT, "Numeric %s out of range", n));
        }
        return (byte) x;
    }

    private static short safeToShort(Number n) throws SQLException {
        if (n instanceof BigInteger) {
            try {
                return ((BigInteger) n).shortValueExact();
            } catch (ArithmeticException ae) {
                throw new SQLException(format(Locale.ROOT, "Numeric %s out of range", n));
            }
        }
        long x = n.longValue();
        if (x > Short.MAX_VALUE || x < Short.MIN_VALUE) {
            throw new SQLException(format(Locale.ROOT, "Numeric %s out of range", n));
        }
        return (short) x;
    }

    private static int safeToInt(Number n) throws SQLException {
        if (n instanceof BigInteger) {
            try {
                return ((BigInteger) n).intValueExact();
            } catch (ArithmeticException ae) {
                throw new SQLException(format(Locale.ROOT, "Numeric %s out of range", n));
            }
        }
        long x = n.longValue();
        if (x > Integer.MAX_VALUE || x < Integer.MIN_VALUE) {
            throw new SQLException(format(Locale.ROOT, "Numeric %s out of range", n));
        }
        return (int) x;
    }

    private static long safeToLong(Number n) throws SQLException {
        if (n instanceof BigInteger) {
            try {
                return ((BigInteger) n).longValueExact();
            } catch (ArithmeticException ae) {
                throw new SQLException(format(Locale.ROOT, "Numeric %s out of range", n));
            }
        }
        double x = n.doubleValue();
        if (x > Long.MAX_VALUE || x < Long.MIN_VALUE) {
            throw new SQLException(format(Locale.ROOT, "Numeric %s out of range", Double.toString(x)));
        }
        return Math.round(x);
    }
}
