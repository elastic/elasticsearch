/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.jdbc;

import org.elasticsearch.xpack.sql.jdbc.JdbcSQLException;
import org.elasticsearch.xpack.sql.type.DataType;

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

/**
 * Conversion utilities for conversion of JDBC types to Java type and back
 * <p>
 * Only the following JDBC types are supported as part of Elasticsearch response and parameters.
 * See org.elasticsearch.xpack.sql.type.DataType for details.
 * <p>
 * NULL, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DOUBLE, REAL, FLOAT, VARCHAR, VARBINARY and TIMESTAMP
 */
final class TypeConverter {

    private TypeConverter() {

    }

    private static final long DAY_IN_MILLIS = 60 * 60 * 24;

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
     * Converts millisecond after epoc to timestamp
     */
    static Timestamp convertTimestamp(Long millis, Calendar cal) {
        return dateTimeConvert(millis, cal, c -> new Timestamp(c.getTimeInMillis()));
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

    /**
     * Converts object val from columnType to type
     */
    @SuppressWarnings("unchecked")
    static <T> T convert(Object val, JDBCType columnType, Class<T> type) throws SQLException {
        if (type == null) {
            return (T) convert(val, columnType);
        }
        if (type == String.class) {
            return (T) asString(convert(val, columnType));
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
        throw new SQLException("Conversion from type [" + columnType + "] to [" + type.getName() + "] not supported");
    }

    /**
     * Translates numeric JDBC type into corresponding Java class
     * <p>
     * See {@link javax.sql.rowset.RowSetMetaDataImpl#getColumnClassName} and
     * https://db.apache.org/derby/docs/10.5/ref/rrefjdbc20377.html
     */
    public static String classNameOf(JDBCType jdbcType) throws JdbcSQLException {
        final DataType dataType;
        try {
            dataType = DataType.fromJdbcType(jdbcType);
        } catch (IllegalArgumentException ex) {
            // Convert unsupported exception to JdbcSQLException
            throw new JdbcSQLException(ex, ex.getMessage());
        }
        if (dataType.javaName == null) {
            throw new JdbcSQLException("Unsupported JDBC type [" + jdbcType + "]");
        }
        return dataType.javaName;
    }

    /**
     * Converts the object from JSON representation to the specified JDBCType
     * <p>
     * The returned types needs to correspond to ES-portion of classes returned by {@link TypeConverter#classNameOf}
     */
    static Object convert(Object v, JDBCType columnType) throws SQLException {
        switch (columnType) {
            case NULL:
                return null;
            case BOOLEAN:
            case VARCHAR:
                return v;  // These types are already represented correctly in JSON
            case TINYINT:
                return ((Number) v).byteValue();  // Parser might return it as integer or long - need to update to the correct type
            case SMALLINT:
                return ((Number) v).shortValue(); // Parser might return it as integer or long - need to update to the correct type
            case INTEGER:
                return ((Number) v).intValue();
            case BIGINT:
                return ((Number) v).longValue();
            case FLOAT:
            case DOUBLE:
                return doubleValue(v); // Double might be represented as string for infinity and NaN values
            case REAL:
                return floatValue(v);  // Float might be represented as string for infinity and NaN values
            case TIMESTAMP:
                return ((Number) v).longValue();
            default:
                throw new SQLException("Unexpected column type [" + columnType.getName() + "]");

        }
    }

    /**
     * Returns true if the type represents a signed number, false otherwise
     * <p>
     * It needs to support both params and column types
     */
    static boolean isSigned(JDBCType jdbcType) throws SQLException {
        final DataType dataType;
        try {
            dataType = DataType.fromJdbcType(jdbcType);
        } catch (IllegalArgumentException ex) {
            // Convert unsupported exception to JdbcSQLException
            throw new JdbcSQLException(ex, ex.getMessage());
        }
        return dataType.isSigned();
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
        return nativeValue == null ? null : String.valueOf(nativeValue);
    }

    private static Boolean asBoolean(Object val, JDBCType columnType) throws SQLException {
        switch (columnType) {
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
                throw new SQLException("Conversion from type [" + columnType + "] to [Boolean] not supported");

        }
    }

    private static Byte asByte(Object val, JDBCType columnType) throws SQLException {
        switch (columnType) {
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

        throw new SQLException("Conversion from type [" + columnType + "] to [Byte] not supported");
    }

    private static Short asShort(Object val, JDBCType columnType) throws SQLException {
        switch (columnType) {
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

        throw new SQLException("Conversion from type [" + columnType + "] to [Short] not supported");
    }

    private static Integer asInteger(Object val, JDBCType columnType) throws SQLException {
        switch (columnType) {
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

        throw new SQLException("Conversion from type [" + columnType + "] to [Integer] not supported");
    }

    private static Long asLong(Object val, JDBCType columnType) throws SQLException {
        switch (columnType) {
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
            case TIMESTAMP:
                return ((Number) val).longValue();
            default:
        }

        throw new SQLException("Conversion from type [" + columnType + "] to [Long] not supported");
    }

    private static Float asFloat(Object val, JDBCType columnType) throws SQLException {
        switch (columnType) {
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

        throw new SQLException("Conversion from type [" + columnType + "] to [Float] not supported");
    }

    private static Double asDouble(Object val, JDBCType columnType) throws SQLException {
        switch (columnType) {
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

        throw new SQLException("Conversion from type [" + columnType + "] to [Double] not supported");
    }

    private static Date asDate(Object val, JDBCType columnType) throws SQLException {
        if (columnType == JDBCType.TIMESTAMP) {
            return new Date(utcMillisRemoveTime(((Number) val).longValue()));
        }
        throw new SQLException("Conversion from type [" + columnType + "] to [Date] not supported");
    }

    private static Time asTime(Object val, JDBCType columnType) throws SQLException {
        if (columnType == JDBCType.TIMESTAMP) {
            return new Time(utcMillisRemoveDate(((Number) val).longValue()));
        }
        throw new SQLException("Conversion from type [" + columnType + "] to [Time] not supported");
    }

    private static Timestamp asTimestamp(Object val, JDBCType columnType) throws SQLException {
        if (columnType == JDBCType.TIMESTAMP) {
            return new Timestamp(((Number) val).longValue());
        }
        throw new SQLException("Conversion from type [" + columnType + "] to [Timestamp] not supported");
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