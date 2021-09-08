/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.function.DoubleFunction;
import java.util.function.Function;
import java.util.function.LongFunction;

import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.BYTE;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.IP;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.NULL;
import static org.elasticsearch.xpack.ql.type.DataTypes.SHORT;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.isDateTime;
import static org.elasticsearch.xpack.ql.type.DataTypes.isPrimitive;
import static org.elasticsearch.xpack.ql.type.DataTypes.isString;

/**
 * Conversion utility from one Elasticsearch data type to another Elasticsearch data types.
 */
public final class DataTypeConverter {

    private DataTypeConverter() {}

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
        if (left == NULL) {
            return right;
        }
        if (right == NULL) {
            return left;
        }
        if (isString(left) && isString(right)) {
            if (left == TEXT || right == TEXT) {
                return TEXT;
            }
            if (left == KEYWORD) {
                return KEYWORD;
            }
            return right;
        }
        if (left.isNumeric() && right.isNumeric()) {
            // if one is int
            if (left.isInteger()) {
                // promote the highest int
                if (right.isInteger()) {
                    return left.size() > right.size() ? left : right;
                }
                // promote the rational
                return right;
            }
            // try the other side
            if (right.isInteger()) {
                return left;
            }
            // promote the highest rational
            return left.size() > right.size() ? left : right;
        }
        if (isString(left)) {
            if (right.isNumeric()) {
                return right;
            }
        }
        if (isString(right)) {
            if (left.isNumeric()) {
                return left;
            }
        }

        if (isDateTime(left) && isDateTime(right)) {
            return DATETIME;
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
        return isPrimitive(from) && isPrimitive(to) && converterFor(from, to) != null;
    }

    /**
     * Get the conversion from one type to another.
     */
    public static Converter converterFor(DataType from, DataType to) {
        // Special handling for nulls and if conversion is not requires
        if (from == to || (isDateTime(from) && isDateTime(to))) {
            return DefaultConverter.IDENTITY;
        }
        if (to == NULL || from == NULL) {
            return DefaultConverter.TO_NULL;
        }
        // proper converters
        if (to == KEYWORD || to == TEXT) {
            return conversionToString(from);
        }
        if (to == LONG) {
            return conversionToLong(from);
        }
        if (to == INTEGER) {
            return conversionToInt(from);
        }
        if (to == SHORT) {
            return conversionToShort(from);
        }
        if (to == BYTE) {
            return conversionToByte(from);
        }
        if (to == FLOAT) {
            return conversionToFloat(from);
        }
        if (to == DOUBLE) {
            return conversionToDouble(from);
        }
        if (isDateTime(to)) {
            return conversionToDateTime(from);
        }
        if (to == BOOLEAN) {
            return conversionToBoolean(from);
        }
        if (to == IP) {
            return conversionToIp(from);
        }
        return null;
    }

    private static Converter conversionToString(DataType from) {
        if (isDateTime(from)) {
            return DefaultConverter.DATETIME_TO_STRING;
        }
        return DefaultConverter.OTHER_TO_STRING;
    }

    private static Converter conversionToIp(DataType from) {
        if (isString(from)) {
            return DefaultConverter.STRING_TO_IP;
        }
        return null;
    }

    private static Converter conversionToLong(DataType from) {
        if (from.isRational()) {
            return DefaultConverter.RATIONAL_TO_LONG;
        }
        if (from.isInteger()) {
            return DefaultConverter.INTEGER_TO_LONG;
        }
        if (from == BOOLEAN) {
            return DefaultConverter.BOOL_TO_LONG;
        }
        if (isString(from)) {
            return DefaultConverter.STRING_TO_LONG;
        }
        if (isDateTime(from)) {
            return DefaultConverter.DATETIME_TO_LONG;
        }
        return null;
    }

    private static Converter conversionToInt(DataType from) {
        if (from.isRational()) {
            return DefaultConverter.RATIONAL_TO_INT;
        }
        if (from.isInteger()) {
            return DefaultConverter.INTEGER_TO_INT;
        }
        if (from == BOOLEAN) {
            return DefaultConverter.BOOL_TO_INT;
        }
        if (isString(from)) {
            return DefaultConverter.STRING_TO_INT;
        }
        if (isDateTime(from)) {
            return DefaultConverter.DATETIME_TO_INT;
        }
        return null;
    }

    private static Converter conversionToShort(DataType from) {
        if (from.isRational()) {
            return DefaultConverter.RATIONAL_TO_SHORT;
        }
        if (from.isInteger()) {
            return DefaultConverter.INTEGER_TO_SHORT;
        }
        if (from == BOOLEAN) {
            return DefaultConverter.BOOL_TO_SHORT;
        }
        if (isString(from)) {
            return DefaultConverter.STRING_TO_SHORT;
        }
        if (isDateTime(from)) {
            return DefaultConverter.DATETIME_TO_SHORT;
        }
        return null;
    }

    private static Converter conversionToByte(DataType from) {
        if (from.isRational()) {
            return DefaultConverter.RATIONAL_TO_BYTE;
        }
        if (from.isInteger()) {
            return DefaultConverter.INTEGER_TO_BYTE;
        }
        if (from == BOOLEAN) {
            return DefaultConverter.BOOL_TO_BYTE;
        }
        if (isString(from)) {
            return DefaultConverter.STRING_TO_BYTE;
        }
        if (isDateTime(from)) {
            return DefaultConverter.DATETIME_TO_BYTE;
        }
        return null;
    }

    private static DefaultConverter conversionToFloat(DataType from) {
        if (from.isRational()) {
            return DefaultConverter.RATIONAL_TO_FLOAT;
        }
        if (from.isInteger()) {
            return DefaultConverter.INTEGER_TO_FLOAT;
        }
        if (from == BOOLEAN) {
            return DefaultConverter.BOOL_TO_FLOAT;
        }
        if (isString(from)) {
            return DefaultConverter.STRING_TO_FLOAT;
        }
        if (isDateTime(from)) {
            return DefaultConverter.DATETIME_TO_FLOAT;
        }
        return null;
    }

    private static DefaultConverter conversionToDouble(DataType from) {
        if (from.isRational()) {
            return DefaultConverter.RATIONAL_TO_DOUBLE;
        }
        if (from.isInteger()) {
            return DefaultConverter.INTEGER_TO_DOUBLE;
        }
        if (from == BOOLEAN) {
            return DefaultConverter.BOOL_TO_DOUBLE;
        }
        if (isString(from)) {
            return DefaultConverter.STRING_TO_DOUBLE;
        }
        if (isDateTime(from)) {
            return DefaultConverter.DATETIME_TO_DOUBLE;
        }
        return null;
    }

    private static DefaultConverter conversionToDateTime(DataType from) {
        if (from.isRational()) {
            return DefaultConverter.RATIONAL_TO_DATETIME;
        }
        if (from.isInteger()) {
            return DefaultConverter.INTEGER_TO_DATETIME;
        }
        if (from == BOOLEAN) {
            return DefaultConverter.BOOL_TO_DATETIME; // We emit an int here which is ok because of Java's casting rules
        }
        if (isString(from)) {
            return DefaultConverter.STRING_TO_DATETIME;
        }
        return null;
    }

    private static DefaultConverter conversionToBoolean(DataType from) {
        if (from.isNumeric()) {
            return DefaultConverter.NUMERIC_TO_BOOLEAN;
        }
        if (isString(from)) {
            return DefaultConverter.STRING_TO_BOOLEAN;
        }
        if (isDateTime(from)) {
            return DefaultConverter.DATETIME_TO_BOOLEAN;
        }
        return null;
    }

    public static byte safeToByte(long x) {
        if (x > Byte.MAX_VALUE || x < Byte.MIN_VALUE) {
            throw new QlIllegalArgumentException("[" + x + "] out of [byte] range");
        }
        return (byte) x;
    }

    public static short safeToShort(long x) {
        if (x > Short.MAX_VALUE || x < Short.MIN_VALUE) {
            throw new QlIllegalArgumentException("[" + x + "] out of [short] range");
        }
        return (short) x;
    }

    public static int safeToInt(long x) {
        if (x > Integer.MAX_VALUE || x < Integer.MIN_VALUE) {
            throw new QlIllegalArgumentException("[" + x + "] out of [integer] range");
        }
        return (int) x;
    }

    public static long safeToLong(double x) {
        if (x > Long.MAX_VALUE || x < Long.MIN_VALUE) {
            throw new QlIllegalArgumentException("[" + x + "] out of [long] range");
        }
        return Math.round(x);
    }

    public static Number toInteger(double x, DataType dataType) {
        long l = safeToLong(x);

        if (dataType == BYTE) {
            return safeToByte(l);
        }
        if (dataType == SHORT) {
            return safeToShort(l);
        }
        if (dataType == INTEGER) {
            return safeToInt(l);
        }
        return l;
    }

    public static boolean convertToBoolean(String val) {
        String lowVal = val.toLowerCase(Locale.ROOT);
        if (Booleans.isBoolean(lowVal) == false) {
            throw new QlIllegalArgumentException("cannot cast [" + val + "] to [boolean]");
        }
        return Booleans.parseBoolean(lowVal);
    }

    /**
     * Converts arbitrary object to the desired data type.
     * <p>
     * Throws QlIllegalArgumentException if such conversion is not possible
     */
    public static Object convert(Object value, DataType dataType) {
        DataType detectedType = DataTypes.fromJava(value);
        if (detectedType == dataType || value == null) {
            return value;
        }
        Converter converter = converterFor(detectedType, dataType);

        if (converter == null) {
            throw new QlIllegalArgumentException("cannot convert from [{}], type [{}] to [{}]", value, detectedType.typeName(),
                    dataType.typeName());
        }

        return converter.convert(value);
    }

    /**
     * Reference to a data type conversion that can be serialized. Note that the position in the enum
     * is important because it is used for serialization.
     */
    public enum DefaultConverter implements Converter {
        IDENTITY(Function.identity()),
        TO_NULL(value -> null),

        DATETIME_TO_STRING(o -> DateUtils.toString((ZonedDateTime) o)),
        OTHER_TO_STRING(String::valueOf),

        RATIONAL_TO_LONG(fromDouble(DataTypeConverter::safeToLong)),
        INTEGER_TO_LONG(fromLong(value -> value)),
        STRING_TO_LONG(fromString(Long::valueOf, "long")),
        DATETIME_TO_LONG(fromDateTime(value -> value)),

        RATIONAL_TO_INT(fromDouble(value -> safeToInt(safeToLong(value)))),
        INTEGER_TO_INT(fromLong(DataTypeConverter::safeToInt)),
        BOOL_TO_INT(fromBool(value -> value ? 1 : 0)),
        STRING_TO_INT(fromString(Integer::valueOf, "integer")),
        DATETIME_TO_INT(fromDateTime(DataTypeConverter::safeToInt)),

        RATIONAL_TO_SHORT(fromDouble(value -> safeToShort(safeToLong(value)))),
        INTEGER_TO_SHORT(fromLong(DataTypeConverter::safeToShort)),
        BOOL_TO_SHORT(fromBool(value -> value ? (short) 1 : (short) 0)),
        STRING_TO_SHORT(fromString(Short::valueOf, "short")),
        DATETIME_TO_SHORT(fromDateTime(DataTypeConverter::safeToShort)),

        RATIONAL_TO_BYTE(fromDouble(value -> safeToByte(safeToLong(value)))),
        INTEGER_TO_BYTE(fromLong(DataTypeConverter::safeToByte)),
        BOOL_TO_BYTE(fromBool(value -> value ? (byte) 1 : (byte) 0)),
        STRING_TO_BYTE(fromString(Byte::valueOf, "byte")),
        DATETIME_TO_BYTE(fromDateTime(DataTypeConverter::safeToByte)),

        // TODO floating point conversions are lossy but conversions to integer conversions are not. Are we ok with that?
        RATIONAL_TO_FLOAT(fromDouble(value -> (float) value)),
        INTEGER_TO_FLOAT(fromLong(value -> (float) value)),
        BOOL_TO_FLOAT(fromBool(value -> value ? 1f : 0f)),
        STRING_TO_FLOAT(fromString(Float::valueOf, "float")),
        DATETIME_TO_FLOAT(fromDateTime(value -> (float) value)),

        RATIONAL_TO_DOUBLE(fromDouble(Double::valueOf)),
        INTEGER_TO_DOUBLE(fromLong(Double::valueOf)),
        BOOL_TO_DOUBLE(fromBool(value -> value ? 1d : 0d)),
        STRING_TO_DOUBLE(fromString(Double::valueOf, "double")),
        DATETIME_TO_DOUBLE(fromDateTime(Double::valueOf)),

        RATIONAL_TO_DATETIME(toDateTime(RATIONAL_TO_LONG)),
        INTEGER_TO_DATETIME(toDateTime(INTEGER_TO_LONG)),
        BOOL_TO_DATETIME(toDateTime(BOOL_TO_INT)),
        STRING_TO_DATETIME(fromString(DateUtils::asDateTime, "datetime")),

        NUMERIC_TO_BOOLEAN(fromLong(value -> value != 0)),
        STRING_TO_BOOLEAN(fromString(DataTypeConverter::convertToBoolean, "boolean")),
        DATETIME_TO_BOOLEAN(fromDateTime(value -> value != 0)),

        BOOL_TO_LONG(fromBool(value -> value ? 1L : 0L)),

        STRING_TO_IP(o -> {
            if (InetAddresses.isInetAddress(o.toString()) == false) {
                throw new QlIllegalArgumentException("[" + o + "] is not a valid IPv4 or IPv6 address");
            }
            return o;
        });

        public static final String NAME = "dtc-def";

        private final Function<Object, Object> converter;

        DefaultConverter(Function<Object, Object> converter) {
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
                    throw new QlIllegalArgumentException(e, "cannot cast [{}] to [{}]", value, to);
                } catch (DateTimeParseException | IllegalArgumentException e) {
                    throw new QlIllegalArgumentException(e, "cannot cast [{}] to [{}]: {}", value, to, e.getMessage());
                }
            };
        }

        private static Function<Object, Object> fromBool(Function<Boolean, Object> converter) {
            return (Object l) -> converter.apply(((Boolean) l));
        }

        private static Function<Object, Object> fromDateTime(Function<Long, Object> converter) {
            return l -> converter.apply(((ZonedDateTime) l).toInstant().toEpochMilli());
        }

        private static Function<Object, Object> toDateTime(Converter conversion) {
            return l -> DateUtils.asDateTime(((Number) conversion.convert(l)).longValue());
        }

        @Override
        public Object convert(Object l) {
            if (l == null) {
                return null;
            }
            return converter.apply(l);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        public static Converter read(StreamInput in) throws IOException {
            return in.readEnum(DefaultConverter.class);
        }
    }

    public static DataType asInteger(DataType dataType) {
        if (dataType.isNumeric() == false) {
            return dataType;
        }

        return dataType.isInteger() ? dataType : LONG;
    }
}
