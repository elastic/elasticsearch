/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.jdbc;

import java.sql.Date;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;
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

abstract class TypeConverter {

    static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC_CALENDAR"), Locale.ROOT);

    private static final long DAY_IN_MILLIS = 60 * 60 * 24;

    static Calendar defaultCalendar() {
        return (Calendar) UTC_CALENDAR.clone();
    }

    static Date convertDate(Long millis, Calendar cal) {
        return dateTimeConvert(millis, cal, c -> {
            c.set(HOUR_OF_DAY, 0);
            c.set(MINUTE, 0);
            c.set(SECOND, 0);
            c.set(MILLISECOND, 0);
            return new Date(c.getTimeInMillis());
        });
    }

    static Time convertTime(Long millis, Calendar cal) {
        return dateTimeConvert(millis, cal, c -> {
            c.set(ERA, GregorianCalendar.AD);
            c.set(YEAR, 1970);
            c.set(MONTH, 0);
            c.set(DAY_OF_MONTH, 1);
            return new Time(c.getTimeInMillis());
        });
    }

    static Timestamp convertTimestamp(Long millis, Calendar cal) {
        return dateTimeConvert(millis, cal, c -> {
            return new Timestamp(c.getTimeInMillis());
        });
    }

    private static <T> T dateTimeConvert(Long millis, Calendar c, Function<Calendar, T> creator) {
        if (millis == null) {
            return null;
        }
        long initial = c.getTimeInMillis();
        try {
            c.setTimeInMillis(millis.longValue());
            return creator.apply(c);
        } finally {
            c.setTimeInMillis(initial);
        }
    }

    @SuppressWarnings("unchecked")
    static <T> T convert(Object val, JDBCType columnType, Class<T> type) throws SQLException {
        if (type == null) {
            return (T) asNative(val, columnType);
        }
        if (type == String.class) {
            return (T) asString(asNative(val, columnType));
        }
        if (type == Boolean.class) {
            return (T) asBoolean(val, columnType);
        }
        if (type == Byte.class) {
            return (T) asByte(val, columnType);
        }
        if (type == Short.class) {
            return (T) asShort(val, columnType);
        }
        if (type == Integer.class) {
            return (T) asInteger(val, columnType);
        }
        if (type == Long.class) {
            return (T) asLong(val, columnType);
        }
        if (type == Float.class) {
            return (T) asFloat(val, columnType);
        }
        if (type == Double.class) {
            return (T) asDouble(val, columnType);
        }
        if (type == Date.class) {
            return (T) asDate(val, columnType);
        }
        if (type == Time.class) {
            return (T) asTime(val, columnType);
        }
        if (type == Timestamp.class) {
            return (T) asTimestamp(val, columnType);
        }
        if (type == byte[].class) {
            return (T) asByteArray(val, columnType);
        }
        //
        // JDK 8 types
        //
        if (type == LocalDate.class) {
            return (T) asLocalDate(val, columnType);
        }
        if (type == LocalTime.class) {
            return (T) asLocalTime(val, columnType);
        }
        if (type == LocalDateTime.class) {
            return (T) asLocalDateTime(val, columnType);
        }
        if (type == OffsetTime.class) {
            return (T) asOffsetTime(val, columnType);
        }
        if (type == OffsetDateTime.class) {
            return (T) asOffsetDateTime(val, columnType);
        }
        return null;
    }

    // keep in check with JdbcUtils#columnType
    private static Object asNative(Object v, JDBCType columnType) {
        Object result = null;
        switch (columnType) {
            case BIT:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case REAL:
            case FLOAT:
            case DOUBLE:
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
            case TIMESTAMP:
                result = v;
                break;
            // since the date is already in UTC_CALENDAR just do calendar math
            case DATE:
                result = new Date(utcMillisRemoveTime(((Long) v).longValue()));
                break;
            case TIME:
                result = new Time(utcMillisRemoveDate(((Long) v).longValue()));
                break;
            default:
        }
        return result;
    }

    private static String asString(Object nativeValue) {
        return nativeValue == null ? null : String.valueOf(nativeValue);
    }

    private static Boolean asBoolean(Object val, JDBCType columnType) {
        switch (columnType) {
            case BIT:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case REAL:
            case FLOAT:
            case DOUBLE:
                return Boolean.valueOf(Integer.signum(((Number) val).intValue()) == 0);
             default:
                 return null;
        }
    }

    private static Byte asByte(Object val, JDBCType columnType) throws SQLException {
        switch (columnType) {
            case BIT:
            case BOOLEAN:
                return Byte.valueOf(((Boolean) val).booleanValue() ? (byte) 1 : (byte) 0);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return safeToByte(((Number) val).longValue());
            case REAL:
            case FLOAT:
            case DOUBLE:
                return safeToByte(safeToLong(((Number) val).doubleValue()));
            default:
        }

        return null;
    }

    private static Short asShort(Object val, JDBCType columnType) throws SQLException {
        switch (columnType) {
            case BIT:
            case BOOLEAN:
                return Short.valueOf(((Boolean) val).booleanValue() ? (short) 1 : (short) 0);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return safeToShort(((Number) val).longValue());
            case REAL:
            case FLOAT:
            case DOUBLE:
                return safeToShort(safeToLong(((Number) val).doubleValue()));
            default:
        }

        return null;
    }

    private static Integer asInteger(Object val, JDBCType columnType) throws SQLException {
        switch (columnType) {
            case BIT:
            case BOOLEAN:
                return Integer.valueOf(((Boolean) val).booleanValue() ? 1 : 0);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return safeToInt(((Number) val).longValue());
            case REAL:
            case FLOAT:
            case DOUBLE:
                return safeToInt(safeToLong(((Number) val).doubleValue()));
            default:
        }

        return null;
    }

    private static Long asLong(Object val, JDBCType columnType) throws SQLException {
        switch (columnType) {
            case BIT:
            case BOOLEAN:
                return Long.valueOf(((Boolean) val).booleanValue() ? 1 : 0);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return Long.valueOf(((Number) val).longValue());
            case REAL:
            case FLOAT:
            case DOUBLE:
                return safeToLong(((Number) val).doubleValue());
            case DATE:
                return utcMillisRemoveTime(((Number) val).longValue());
            case TIME:
                return utcMillisRemoveDate(((Number) val).longValue());
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIMEZONE:
                return ((Number) val).longValue();
            default:
        }

        return null;
    }

    private static Float asFloat(Object val, JDBCType columnType) throws SQLException {
        switch (columnType) {
            case BIT:
            case BOOLEAN:
                return Float.valueOf(((Boolean) val).booleanValue() ? 1 : 0);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return Float.valueOf((float) ((Number) val).longValue());
            case REAL:
            case FLOAT:
            case DOUBLE:
                return new Float(((Number) val).doubleValue());
            default:
        }

        return null;
    }

    private static Double asDouble(Object val, JDBCType columnType) throws SQLException {
        switch (columnType) {
            case BIT:
            case BOOLEAN:
                return Double.valueOf(((Boolean) val).booleanValue() ? 1 : 0);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return Double.valueOf((double) ((Number) val).longValue());
            case REAL:
            case FLOAT:
            case DOUBLE:
                return new Double(((Number) val).doubleValue());
            default:
        }

        return null;
    }

    private static Date asDate(Object val, JDBCType columnType) throws SQLException {
        switch (columnType) {
            case TIME:
                // time has no date component
                return new Date(0);
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIMEZONE:
                return new Date(utcMillisRemoveTime(((Number) val).longValue()));
            default:
        }

        return null;
    }

    private static Time asTime(Object val, JDBCType columnType) throws SQLException {
        switch (columnType) {
            case DATE:
                // date has no time component
                return new Time(0);
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIMEZONE:
                return new Time(utcMillisRemoveDate(((Number) val).longValue()));
            default:
        }

        return null;
    }

    private static Timestamp asTimestamp(Object val, JDBCType columnType) throws SQLException {
        switch (columnType) {
            case DATE:
                return new Timestamp(utcMillisRemoveTime(((Number) val).longValue()));
            case TIME:
                return new Timestamp(utcMillisRemoveDate(((Number) val).longValue()));
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIMEZONE:
                return new Timestamp(((Number) val).longValue());
            default:
        }

        return null;
    }

    private static byte[] asByteArray(Object val, JDBCType columnType) {
        throw new UnsupportedOperationException();
    }
    private static LocalDate asLocalDate(Object val, JDBCType columnType) {
        throw new UnsupportedOperationException();
    }

    private static LocalTime asLocalTime(Object val, JDBCType columnType) {
        throw new UnsupportedOperationException();
    }

    private static LocalDateTime asLocalDateTime(Object val, JDBCType columnType) {
        throw new UnsupportedOperationException();
    }

    private static OffsetTime asOffsetTime(Object val, JDBCType columnType) {
        throw new UnsupportedOperationException();
    }

    private static OffsetDateTime asOffsetDateTime(Object val, JDBCType columnType) {
        throw new UnsupportedOperationException();
    }


    private static long utcMillisRemoveTime(long l) {
        return l - (l % DAY_IN_MILLIS);
    }

    private static long utcMillisRemoveDate(long l) {
        return l % DAY_IN_MILLIS;
    }

    private static byte safeToByte(long x) throws SQLException {
        if (x > Byte.MAX_VALUE || x < Byte.MIN_VALUE) {
            throw new SQLException(format(Locale.ROOT, "Numeric %d out of range", Long.toString(x)));
        }
        return (byte) x;
    }

    private static short safeToShort(long x) throws SQLException {
        if (x > Short.MAX_VALUE || x < Short.MIN_VALUE) {
            throw new SQLException(format(Locale.ROOT, "Numeric %d out of range", Long.toString(x)));
        }
        return (short) x;
    }

    private static int safeToInt(long x) throws SQLException {
        if (x > Integer.MAX_VALUE || x < Integer.MIN_VALUE) {
            throw new SQLException(format(Locale.ROOT, "Numeric %d out of range", Long.toString(x)));
        }
        return (int) x;
    }

    private static long safeToLong(double x) throws SQLException {
        if (x > Long.MAX_VALUE || x < Long.MIN_VALUE) {
            throw new SQLException(format(Locale.ROOT, "Numeric %d out of range", Double.toString(x)));
        }
        return Math.round(x);
    }
}