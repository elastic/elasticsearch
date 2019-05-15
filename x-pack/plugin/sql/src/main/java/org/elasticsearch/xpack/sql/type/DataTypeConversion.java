/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.function.DoubleFunction;
import java.util.function.Function;
import java.util.function.LongFunction;

import static org.elasticsearch.xpack.sql.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.sql.type.DataType.DATE;
import static org.elasticsearch.xpack.sql.type.DataType.DATETIME;
import static org.elasticsearch.xpack.sql.type.DataType.LONG;
import static org.elasticsearch.xpack.sql.type.DataType.NULL;
import static org.elasticsearch.xpack.sql.type.DataType.TIME;

/**
 * Conversions from one Elasticsearch data type to another Elasticsearch data types.
 * <p>
 * This class throws {@link SqlIllegalArgumentException} to differentiate between validation
 * errors inside SQL as oppose to the rest of ES.
 */
public abstract class DataTypeConversion {

    /**
     * Returns the type compatible with both left and right types
     * <p>
     * If one of the types is null - returns another type
     * If both types are numeric - returns type with the highest precision int &lt; long &lt; float &lt; double
     * If one of the types is string and another numeric - returns numeric
     */
    public static DataType commonType(DataType left, DataType right) {
        if (left == right) {
            return left;
        }
        if (DataTypes.isNull(left)) {
            return right;
        }
        if (DataTypes.isNull(right)) {
            return left;
        }
        if (left.isNumeric() && right.isNumeric()) {
            // if one is int
            if (left.isInteger()) {
                // promote the highest int
                if (right.isInteger()) {
                    return left.size > right.size ? left : right;
                }
                // promote the rational
                return right;
            }
            // try the other side
            if (right.isInteger()) {
                return left;
            }
            // promote the highest rational
            return left.size > right.size ? left : right;
        }
        if (left.isString()) {
            if (right.isNumeric()) {
                return right;
            }
        }
        if (right.isString()) {
            if (left.isNumeric()) {
                return left;
            }
        }

        // interval and dates
        if (left == DATE) {
            if (DataTypes.isInterval(right)) {
                return left;
            }
        }
        if (right == DATE) {
            if (DataTypes.isInterval(left)) {
                return right;
            }
        }
        if (left == TIME) {
            if (right == DATE) {
                return DATETIME;
            }
            if (DataTypes.isInterval(right)) {
                return left;
            }
        }
        if (right == TIME) {
            if (left == DATE) {
                return DATETIME;
            }
            if (DataTypes.isInterval(left)) {
                return right;
            }
        }
        if (left == DATETIME) {
            if (right == DATE || right == TIME) {
                return left;
            }
            if (DataTypes.isInterval(right)) {
                return left;
            }
        }
        if (right == DATETIME) {
            if (left == DATE || left == TIME) {
                return right;
            }
            if (DataTypes.isInterval(left)) {
                return right;
            }
        }
        // Interval * integer is a valid operation
        if (DataTypes.isInterval(left)) {
            if (right.isInteger()) {
                return left;
            }
        }
        if (DataTypes.isInterval(right)) {
            if (left.isInteger()) {
                return right;
            }
        }
        if (DataTypes.isInterval(left)) {
            // intervals widening
            if (DataTypes.isInterval(right)) {
                // null returned for incompatible intervals
                return DataTypes.compatibleInterval(left, right);
            }
        }

        // none found
        return null;
    }

    /**
     * Returns true if the from type can be converted to the to type, false - otherwise
     */
    public static boolean canConvert(DataType from, DataType to) {
        // Special handling for nulls and if conversion is not requires
        if (from == to || from == NULL) {
            return true;
        }
        // only primitives are supported so far
        return from.isPrimitive() && to.isPrimitive() && conversion(from, to) != null;
    }

    /**
     * Get the conversion from one type to another.
     */
    public static Conversion conversionFor(DataType from, DataType to) {
        // Special handling for nulls and if conversion is not requires
        if (from == to) {
            return Conversion.IDENTITY;
        }
        if (to == NULL || from == NULL) {
            return Conversion.NULL;
        }
        if (from == NULL) {
            return Conversion.NULL;
        }
        
        Conversion conversion = conversion(from, to);
        if (conversion == null) {
            throw new SqlIllegalArgumentException("cannot convert from [" + from.typeName + "] to [" + to.typeName + "]");
        }
        return conversion;
    }

    private static Conversion conversion(DataType from, DataType to) {
        switch (to) {
            case KEYWORD:
            case TEXT:
                return conversionToString(from);
            case IP:
                return conversionToIp(from);
            case LONG:
                return conversionToLong(from);
            case INTEGER:
                return conversionToInt(from);
            case SHORT:
                return conversionToShort(from);
            case BYTE:
                return conversionToByte(from);
            case FLOAT:
                return conversionToFloat(from);
            case DOUBLE:
                return conversionToDouble(from);
            case DATE:
                return conversionToDate(from);
            case TIME:
                return conversionToTime(from);
            case DATETIME:
                return conversionToDateTime(from);
            case BOOLEAN:
                return conversionToBoolean(from);
            default:
                return null;
        }

    }

    private static Conversion conversionToString(DataType from) {
        if (from == DATE) {
            return Conversion.DATE_TO_STRING;
        }
        if (from == TIME) {
            return Conversion.TIME_TO_STRING;
        }
        if (from == DATETIME) {
            return Conversion.DATETIME_TO_STRING;
        }
        return Conversion.OTHER_TO_STRING;
    }

    private static Conversion conversionToIp(DataType from) {
        if (from.isString()) {
            return Conversion.STRING_TO_IP;
        }
        return null;
    }

    private static Conversion conversionToLong(DataType from) {
        if (from.isRational()) {
            return Conversion.RATIONAL_TO_LONG;
        }
        if (from.isInteger()) {
            return Conversion.INTEGER_TO_LONG;
        }
        if (from == BOOLEAN) {
            return Conversion.BOOL_TO_LONG;
        }
        if (from.isString()) {
            return Conversion.STRING_TO_LONG;
        }
        if (from == DATE) {
            return Conversion.DATE_TO_LONG;
        }
        if (from == TIME) {
            return Conversion.TIME_TO_LONG;
        }
        if (from == DATETIME) {
            return Conversion.DATETIME_TO_LONG;
        }
        return null;
    }

    private static Conversion conversionToInt(DataType from) {
        if (from.isRational()) {
            return Conversion.RATIONAL_TO_INT;
        }
        if (from.isInteger()) {
            return Conversion.INTEGER_TO_INT;
        }
        if (from == BOOLEAN) {
            return Conversion.BOOL_TO_INT;
        }
        if (from.isString()) {
            return Conversion.STRING_TO_INT;
        }
        if (from == DATE) {
            return Conversion.DATE_TO_INT;
        }
        if (from == TIME) {
            return Conversion.TIME_TO_INT;
        }
        if (from == DATETIME) {
            return Conversion.DATETIME_TO_INT;
        }
        return null;
    }

    private static Conversion conversionToShort(DataType from) {
        if (from.isRational()) {
            return Conversion.RATIONAL_TO_SHORT;
        }
        if (from.isInteger()) {
            return Conversion.INTEGER_TO_SHORT;
        }
        if (from == BOOLEAN) {
            return Conversion.BOOL_TO_SHORT;
        }
        if (from.isString()) {
            return Conversion.STRING_TO_SHORT;
        }
        if (from == DATE) {
            return Conversion.DATE_TO_SHORT;
        }
        if (from == TIME) {
            return Conversion.TIME_TO_SHORT;
        }
        if (from == DATETIME) {
            return Conversion.DATETIME_TO_SHORT;
        }
        return null;
    }

    private static Conversion conversionToByte(DataType from) {
        if (from.isRational()) {
            return Conversion.RATIONAL_TO_BYTE;
        }
        if (from.isInteger()) {
            return Conversion.INTEGER_TO_BYTE;
        }
        if (from == BOOLEAN) {
            return Conversion.BOOL_TO_BYTE;
        }
        if (from.isString()) {
            return Conversion.STRING_TO_BYTE;
        }
        if (from == DATE) {
            return Conversion.DATE_TO_BYTE;
        }
        if (from == TIME) {
            return Conversion.TIME_TO_BYTE;
        }
        if (from == DATETIME) {
            return Conversion.DATETIME_TO_BYTE;
        }
        return null;
    }

    private static Conversion conversionToFloat(DataType from) {
        if (from.isRational()) {
            return Conversion.RATIONAL_TO_FLOAT;
        }
        if (from.isInteger()) {
            return Conversion.INTEGER_TO_FLOAT;
        }
        if (from == BOOLEAN) {
            return Conversion.BOOL_TO_FLOAT;
        }
        if (from.isString()) {
            return Conversion.STRING_TO_FLOAT;
        }
        if (from == DATE) {
            return Conversion.DATE_TO_FLOAT;
        }
        if (from == TIME) {
            return Conversion.TIME_TO_FLOAT;
        }
        if (from == DATETIME) {
            return Conversion.DATETIME_TO_FLOAT;
        }
        return null;
    }

    private static Conversion conversionToDouble(DataType from) {
        if (from.isRational()) {
            return Conversion.RATIONAL_TO_DOUBLE;
        }
        if (from.isInteger()) {
            return Conversion.INTEGER_TO_DOUBLE;
        }
        if (from == BOOLEAN) {
            return Conversion.BOOL_TO_DOUBLE;
        }
        if (from.isString()) {
            return Conversion.STRING_TO_DOUBLE;
        }
        if (from == DATE) {
            return Conversion.DATE_TO_DOUBLE;
        }
        if (from == TIME) {
            return Conversion.TIME_TO_DOUBLE;
        }
        if (from == DATETIME) {
            return Conversion.DATETIME_TO_DOUBLE;
        }
        return null;
    }

    private static Conversion conversionToDate(DataType from) {
        if (from.isRational()) {
            return Conversion.RATIONAL_TO_DATE;
        }
        if (from.isInteger()) {
            return Conversion.INTEGER_TO_DATE;
        }
        if (from == BOOLEAN) {
            return Conversion.BOOL_TO_DATE; // We emit an int here which is ok because of Java's casting rules
        }
        if (from.isString()) {
            return Conversion.STRING_TO_DATE;
        }
        if (from == DATETIME) {
            return Conversion.DATETIME_TO_DATE;
        }
        return null;
    }

    private static Conversion conversionToTime(DataType from) {
        if (from.isRational()) {
            return Conversion.RATIONAL_TO_TIME;
        }
        if (from.isInteger()) {
            return Conversion.INTEGER_TO_TIME;
        }
        if (from == BOOLEAN) {
            return Conversion.BOOL_TO_TIME; // We emit an int here which is ok because of Java's casting rules
        }
        if (from.isString()) {
            return Conversion.STRING_TO_TIME;
        }
        if (from == DATE) {
            return Conversion.DATE_TO_TIME;
        }
        if (from == DATETIME) {
            return Conversion.DATETIME_TO_TIME;
        }
        return null;
    }

    private static Conversion conversionToDateTime(DataType from) {
        if (from.isRational()) {
            return Conversion.RATIONAL_TO_DATETIME;
        }
        if (from.isInteger()) {
            return Conversion.INTEGER_TO_DATETIME;
        }
        if (from == BOOLEAN) {
            return Conversion.BOOL_TO_DATETIME; // We emit an int here which is ok because of Java's casting rules
        }
        if (from.isString()) {
            return Conversion.STRING_TO_DATETIME;
        }
        if (from == DATE) {
            return Conversion.DATE_TO_DATETIME;
        }
        return null;
    }

    private static Conversion conversionToBoolean(DataType from) {
        if (from.isNumeric()) {
            return Conversion.NUMERIC_TO_BOOLEAN;
        }
        if (from.isString()) {
            return Conversion.STRING_TO_BOOLEAN;
        }
        if (from == DATE) {
            return Conversion.DATE_TO_BOOLEAN;
        }
        if (from == TIME) {
            return Conversion.TIME_TO_BOOLEAN;
        }
        if (from == DATETIME) {
            return Conversion.DATETIME_TO_BOOLEAN;
        }
        return null;
    }

    public static byte safeToByte(long x) {
        if (x > Byte.MAX_VALUE || x < Byte.MIN_VALUE) {
            throw new SqlIllegalArgumentException("[" + x + "] out of [byte] range");
        }
        return (byte) x;
    }

    public static short safeToShort(long x) {
        if (x > Short.MAX_VALUE || x < Short.MIN_VALUE) {
            throw new SqlIllegalArgumentException("[" + x + "] out of [short] range");
        }
        return (short) x;
    }

    public static int safeToInt(long x) {
        if (x > Integer.MAX_VALUE || x < Integer.MIN_VALUE) {
            throw new SqlIllegalArgumentException("[" + x + "] out of [integer] range");
        }
        return (int) x;
    }

    public static long safeToLong(double x) {
        if (x > Long.MAX_VALUE || x < Long.MIN_VALUE) {
            throw new SqlIllegalArgumentException("[" + x + "] out of [long] range");
        }
        return Math.round(x);
    }

    public static Number toInteger(double x, DataType dataType) {
        long l = safeToLong(x);

        switch (dataType) {
            case BYTE:
                return safeToByte(l);
            case SHORT:
                return safeToShort(l);
            case INTEGER:
                return safeToInt(l);
            default:
                return l;
        }
    }

    public static boolean convertToBoolean(String val) {
        String lowVal = val.toLowerCase(Locale.ROOT);
        if (Booleans.isBoolean(lowVal) == false) {
            throw new SqlIllegalArgumentException("cannot cast [" + val + "] to [boolean]");
        }
        return Booleans.parseBoolean(lowVal);
    }

    /**
     * Converts arbitrary object to the desired data type.
     * <p>
     * Throws SqlIllegalArgumentException if such conversion is not possible
     */
    public static Object convert(Object value, DataType dataType) {
        DataType detectedType = DataTypes.fromJava(value);
        if (detectedType == dataType || value == null) {
            return value;
        }
        return conversionFor(detectedType, dataType).convert(value);
    }

    /**
     * Reference to a data type conversion that can be serialized. Note that the position in the enum
     * is important because it is used for serialization.
     */
    public enum Conversion {
        IDENTITY(Function.identity()),
        NULL(value -> null),
        
        DATE_TO_STRING(o -> DateUtils.toDateString((ZonedDateTime) o)),
        TIME_TO_STRING(o -> DateUtils.toTimeString((OffsetTime) o)),
        DATETIME_TO_STRING(o -> DateUtils.toString((ZonedDateTime) o)),
        OTHER_TO_STRING(String::valueOf),

        RATIONAL_TO_LONG(fromDouble(DataTypeConversion::safeToLong)),
        INTEGER_TO_LONG(fromLong(value -> value)),
        STRING_TO_LONG(fromString(Long::valueOf, "long")),
        DATE_TO_LONG(fromDateTime(value -> value)),
        TIME_TO_LONG(fromTime(value -> value)),
        DATETIME_TO_LONG(fromDateTime(value -> value)),

        RATIONAL_TO_INT(fromDouble(value -> safeToInt(safeToLong(value)))),
        INTEGER_TO_INT(fromLong(DataTypeConversion::safeToInt)),
        BOOL_TO_INT(fromBool(value -> value ? 1 : 0)),
        STRING_TO_INT(fromString(Integer::valueOf, "integer")),
        DATE_TO_INT(fromDateTime(DataTypeConversion::safeToInt)),
        TIME_TO_INT(fromTime(DataTypeConversion::safeToInt)),
        DATETIME_TO_INT(fromDateTime(DataTypeConversion::safeToInt)),

        RATIONAL_TO_SHORT(fromDouble(value -> safeToShort(safeToLong(value)))),
        INTEGER_TO_SHORT(fromLong(DataTypeConversion::safeToShort)),
        BOOL_TO_SHORT(fromBool(value -> value ? (short) 1 : (short) 0)),
        STRING_TO_SHORT(fromString(Short::valueOf, "short")),
        DATE_TO_SHORT(fromDateTime(DataTypeConversion::safeToShort)),
        TIME_TO_SHORT(fromTime(DataTypeConversion::safeToShort)),
        DATETIME_TO_SHORT(fromDateTime(DataTypeConversion::safeToShort)),

        RATIONAL_TO_BYTE(fromDouble(value -> safeToByte(safeToLong(value)))),
        INTEGER_TO_BYTE(fromLong(DataTypeConversion::safeToByte)),
        BOOL_TO_BYTE(fromBool(value -> value ? (byte) 1 : (byte) 0)),
        STRING_TO_BYTE(fromString(Byte::valueOf, "byte")),
        DATE_TO_BYTE(fromDateTime(DataTypeConversion::safeToByte)),
        TIME_TO_BYTE(fromTime(DataTypeConversion::safeToByte)),
        DATETIME_TO_BYTE(fromDateTime(DataTypeConversion::safeToByte)),

        // TODO floating point conversions are lossy but conversions to integer conversions are not. Are we ok with that?
        RATIONAL_TO_FLOAT(fromDouble(value -> (float) value)),
        INTEGER_TO_FLOAT(fromLong(value -> (float) value)),
        BOOL_TO_FLOAT(fromBool(value -> value ? 1f : 0f)),
        STRING_TO_FLOAT(fromString(Float::valueOf, "float")),
        DATE_TO_FLOAT(fromDateTime(value -> (float) value)),
        TIME_TO_FLOAT(fromTime(value -> (float) value)),
        DATETIME_TO_FLOAT(fromDateTime(value -> (float) value)),

        RATIONAL_TO_DOUBLE(fromDouble(Double::valueOf)),
        INTEGER_TO_DOUBLE(fromLong(Double::valueOf)),
        BOOL_TO_DOUBLE(fromBool(value -> value ? 1d : 0d)),
        STRING_TO_DOUBLE(fromString(Double::valueOf, "double")),
        DATE_TO_DOUBLE(fromDateTime(Double::valueOf)),
        TIME_TO_DOUBLE(fromTime(Double::valueOf)),
        DATETIME_TO_DOUBLE(fromDateTime(Double::valueOf)),

        RATIONAL_TO_DATE(toDate(RATIONAL_TO_LONG)),
        INTEGER_TO_DATE(toDate(INTEGER_TO_LONG)),
        BOOL_TO_DATE(toDate(BOOL_TO_INT)),
        STRING_TO_DATE(fromString(DateUtils::asDateOnly, "date")),
        DATETIME_TO_DATE(fromDatetimeToDate()),

        RATIONAL_TO_TIME(toTime(RATIONAL_TO_LONG)),
        INTEGER_TO_TIME(toTime(INTEGER_TO_LONG)),
        BOOL_TO_TIME(toTime(BOOL_TO_INT)),
        STRING_TO_TIME(fromString(DateUtils::asTimeOnly, "time")),
        DATE_TO_TIME(fromDatetimeToTime()),
        DATETIME_TO_TIME(fromDatetimeToTime()),

        RATIONAL_TO_DATETIME(toDateTime(RATIONAL_TO_LONG)),
        INTEGER_TO_DATETIME(toDateTime(INTEGER_TO_LONG)),
        BOOL_TO_DATETIME(toDateTime(BOOL_TO_INT)),
        STRING_TO_DATETIME(fromString(DateUtils::asDateTime, "datetime")),
        DATE_TO_DATETIME(value -> value),

        NUMERIC_TO_BOOLEAN(fromLong(value -> value != 0)),
        STRING_TO_BOOLEAN(fromString(DataTypeConversion::convertToBoolean, "boolean")),
        DATE_TO_BOOLEAN(fromDateTime(value -> value != 0)),
        TIME_TO_BOOLEAN(fromTime(value -> value != 0)),
        DATETIME_TO_BOOLEAN(fromDateTime(value -> value != 0)),

        BOOL_TO_LONG(fromBool(value -> value ? 1L : 0L)),

        STRING_TO_IP(o -> {
            if (!InetAddresses.isInetAddress(o.toString())) {
                throw new SqlIllegalArgumentException( "[" + o + "] is not a valid IPv4 or IPv6 address");
            }
            return o;
        });

        private final Function<Object, Object> converter;

        Conversion(Function<Object, Object> converter) {
            this.converter = converter;
        }

        private static Function<Object, Object> fromDouble(DoubleFunction<Object> converter) {
            return (Object l) -> converter.apply(((Number) l).doubleValue());
        }

        private static Function<Object, Object> fromLong(LongFunction<Object> converter) {
            return (Object l) -> converter.apply(((Number) l).longValue());
        }
        
        private static Function<Object, Object> fromString(Function<String, Object> converter, String to) {
            return (Object value) -> {
                try {
                    return converter.apply(value.toString());
                } catch (NumberFormatException e) {
                    throw new SqlIllegalArgumentException(e, "cannot cast [{}] to [{}]", value, to);
                } catch (DateTimeParseException | IllegalArgumentException e) {
                    throw new SqlIllegalArgumentException(e, "cannot cast [{}] to [{}]: {}", value, to, e.getMessage());
                }
            };
        }

        private static Function<Object, Object> fromBool(Function<Boolean, Object> converter) {
            return (Object l) -> converter.apply(((Boolean) l));
        }

        private static Function<Object, Object> fromTime(Function<Long, Object> converter) {
            return l -> converter.apply(((OffsetTime) l).atDate(DateUtils.EPOCH).toInstant().toEpochMilli());
        }

        private static Function<Object, Object> fromDateTime(Function<Long, Object> converter) {
            return l -> converter.apply(((ZonedDateTime) l).toInstant().toEpochMilli());
        }

        private static Function<Object, Object> toDate(Conversion conversion) {
            return l -> DateUtils.asDateOnly(((Number) conversion.convert(l)).longValue());
        }

        private static Function<Object, Object> toTime(Conversion conversion) {
            return l -> DateUtils.asTimeOnly(((Number) conversion.convert(l)).longValue());
        }

        private static Function<Object, Object> toDateTime(Conversion conversion) {
            return l -> DateUtils.asDateTime(((Number) conversion.convert(l)).longValue());
        }

        private static Function<Object, Object> fromDatetimeToDate() {
            return l -> DateUtils.asDateOnly((ZonedDateTime) l);
        }

        private static Function<Object, Object> fromDatetimeToTime() {
            return l -> ((ZonedDateTime) l).toOffsetDateTime().toOffsetTime();
        }

        public Object convert(Object l) {
            if (l == null) {
                return null;
            }
            return converter.apply(l);
        }
    }

    public static DataType asInteger(DataType dataType) {
        if (!dataType.isNumeric()) {
            return dataType;
        }

        return dataType.isInteger() ? dataType : LONG;
    }
}
