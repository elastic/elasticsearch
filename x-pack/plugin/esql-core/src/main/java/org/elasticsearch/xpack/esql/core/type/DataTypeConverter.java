/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.util.DateUtils;
import org.elasticsearch.xpack.versionfield.Version;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.function.DoubleFunction;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.BYTE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.SHORT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.core.type.DataType.isDateTime;
import static org.elasticsearch.xpack.esql.core.type.DataType.isPrimitiveAndSupported;
import static org.elasticsearch.xpack.esql.core.type.DataType.isString;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.UNSIGNED_LONG_MAX;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.inUnsignedLongRange;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.isUnsignedLong;

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
            int lsize = left.estimatedSize().orElseThrow();
            int rsize = right.estimatedSize().orElseThrow();
            // if one is int
            if (left.isWholeNumber()) {
                // promote the highest int
                if (right.isWholeNumber()) {
                    if (left == UNSIGNED_LONG || right == UNSIGNED_LONG) {
                        return UNSIGNED_LONG;
                    }
                    return lsize > rsize ? left : right;
                }
                // promote the rational
                return right;
            }
            // try the other side
            if (right.isWholeNumber()) {
                return left;
            }
            // promote the highest rational
            return lsize > rsize ? left : right;
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
        return isPrimitiveAndSupported(from) && isPrimitiveAndSupported(to) && converterFor(from, to) != null;
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
        if (to == UNSIGNED_LONG) {
            return conversionToUnsignedLong(from);
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
        if (to == VERSION) {
            return conversionToVersion(from);
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

    private static Converter conversionToVersion(DataType from) {
        if (isString(from)) {
            return DefaultConverter.STRING_TO_VERSION;
        }
        return null;
    }

    private static Converter conversionToUnsignedLong(DataType from) {
        if (from.isRationalNumber()) {
            return DefaultConverter.RATIONAL_TO_UNSIGNED_LONG;
        }
        if (from.isWholeNumber()) {
            return DefaultConverter.INTEGER_TO_UNSIGNED_LONG;
        }
        if (from == BOOLEAN) {
            return DefaultConverter.BOOL_TO_UNSIGNED_LONG;
        }
        if (isString(from)) {
            return DefaultConverter.STRING_TO_UNSIGNED_LONG;
        }
        if (from == DATETIME) {
            return DefaultConverter.DATETIME_TO_UNSIGNED_LONG;
        }
        return null;
    }

    private static Converter conversionToLong(DataType from) {
        if (from.isRationalNumber()) {
            return DefaultConverter.RATIONAL_TO_LONG;
        }
        if (from.isWholeNumber()) {
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
        if (from.isRationalNumber()) {
            return DefaultConverter.RATIONAL_TO_INT;
        }
        if (from.isWholeNumber()) {
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
        if (from.isRationalNumber()) {
            return DefaultConverter.RATIONAL_TO_SHORT;
        }
        if (from.isWholeNumber()) {
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
        if (from.isRationalNumber()) {
            return DefaultConverter.RATIONAL_TO_BYTE;
        }
        if (from.isWholeNumber()) {
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
        if (from.isRationalNumber()) {
            return DefaultConverter.RATIONAL_TO_FLOAT;
        }
        if (from.isWholeNumber()) {
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
        if (from.isRationalNumber()) {
            return DefaultConverter.RATIONAL_TO_DOUBLE;
        }
        if (from.isWholeNumber()) {
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
        if (from.isRationalNumber()) {
            return DefaultConverter.RATIONAL_TO_DATETIME;
        }
        if (from.isWholeNumber()) {
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
            throw new InvalidArgumentException("[{}] out of [byte] range", x);
        }
        return (byte) x;
    }

    public static short safeToShort(long x) {
        if (x > Short.MAX_VALUE || x < Short.MIN_VALUE) {
            throw new InvalidArgumentException("[{}] out of [short] range", x);
        }
        return (short) x;
    }

    public static int safeToInt(long x) {
        if (x > Integer.MAX_VALUE || x < Integer.MIN_VALUE) {
            throw new InvalidArgumentException("[{}] out of [integer] range", x);
        }
        return (int) x;
    }

    public static int safeToInt(double x) {
        if (x > Integer.MAX_VALUE || x < Integer.MIN_VALUE) {
            throw new InvalidArgumentException("[{}] out of [integer] range", x);
        }
        // cast is safe, double can represent all of int's range
        return (int) Math.round(x);
    }

    public static long safeDoubleToLong(double x) {
        if (x > Long.MAX_VALUE || x < Long.MIN_VALUE) {
            throw new InvalidArgumentException("[{}] out of [long] range", x);
        }
        return Math.round(x);
    }

    public static Long safeToLong(Number x) {
        try {
            if (x instanceof BigInteger) {
                return ((BigInteger) x).longValueExact();
            }
            // integer converters are also provided double values (aggs generated on integer fields)
            if (x instanceof Double || x instanceof Float) {
                return safeDoubleToLong(x.doubleValue());
            }
            return x.longValue();
        } catch (ArithmeticException ae) {
            throw new InvalidArgumentException(ae, "[{}] out of [long] range", x);
        }
    }

    public static BigInteger safeToUnsignedLong(Double x) {
        if (inUnsignedLongRange(x) == false) {
            throw new InvalidArgumentException("[{}] out of [unsigned_long] range", x);
        }
        return BigDecimal.valueOf(x).toBigInteger();
    }

    public static BigInteger safeToUnsignedLong(Long x) {
        if (x < 0) {
            throw new InvalidArgumentException("[{}] out of [unsigned_long] range", x);
        }
        return BigInteger.valueOf(x);
    }

    public static BigInteger safeToUnsignedLong(String x) {
        BigInteger bi = new BigDecimal(x).toBigInteger();
        if (isUnsignedLong(bi) == false) {
            throw new InvalidArgumentException("[{}] out of [unsigned_long] range", x);
        }
        return bi;
    }

    // "unsafe" value conversion to unsigned long (vs. "safe", type-only conversion of safeToUnsignedLong());
    // -1L -> 18446744073709551615 (=UNSIGNED_LONG_MAX)
    public static BigInteger toUnsignedLong(Number number) {
        BigInteger bi = BigInteger.valueOf(number.longValue());
        return bi.signum() < 0 ? bi.and(UNSIGNED_LONG_MAX) : bi;
    }

    public static Number toInteger(double x, DataType dataType) {
        long l = safeDoubleToLong(x);

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
            throw new InvalidArgumentException("cannot cast [{}] to [boolean]", val);
        }
        return Booleans.parseBoolean(lowVal);
    }

    /**
     * Converts arbitrary object to the desired data type.
     * <p>
     * Throws InvalidArgumentException if such conversion is not possible
     */
    public static Object convert(Object value, DataType dataType) {
        DataType detectedType = DataType.fromJava(value);
        if (detectedType == dataType || value == null) {
            return value;
        }
        Converter converter = converterFor(detectedType, dataType);

        if (converter == null) {
            throw new InvalidArgumentException(
                "cannot convert from [{}], type [{}] to [{}]",
                value,
                detectedType.typeName(),
                dataType.typeName()
            );
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

        RATIONAL_TO_UNSIGNED_LONG(fromDouble(DataTypeConverter::safeToUnsignedLong)),
        INTEGER_TO_UNSIGNED_LONG(fromNumber(value -> DataTypeConverter.safeToUnsignedLong(value.longValue()))),
        STRING_TO_UNSIGNED_LONG(fromString(DataTypeConverter::safeToUnsignedLong, "unsigned_long")),
        DATETIME_TO_UNSIGNED_LONG(fromDateTime(DataTypeConverter::safeToUnsignedLong)),

        RATIONAL_TO_LONG(fromDouble(DataTypeConverter::safeDoubleToLong)),
        INTEGER_TO_LONG(fromNumber(DataTypeConverter::safeToLong)),
        STRING_TO_LONG(fromString(Long::valueOf, "long")),
        DATETIME_TO_LONG(fromDateTime(value -> value)),

        RATIONAL_TO_INT(fromDouble(value -> safeToInt(safeDoubleToLong(value)))),
        INTEGER_TO_INT(fromNumber(value -> safeToInt(safeToLong(value)))),
        BOOL_TO_INT(fromBool(value -> value ? 1 : 0)),
        STRING_TO_INT(fromString(Integer::valueOf, "integer")),
        DATETIME_TO_INT(fromDateTime(DataTypeConverter::safeToInt)),

        RATIONAL_TO_SHORT(fromDouble(value -> safeToShort(safeDoubleToLong(value)))),
        INTEGER_TO_SHORT(fromNumber(value -> safeToShort(safeToLong(value)))),
        BOOL_TO_SHORT(fromBool(value -> value ? (short) 1 : (short) 0)),
        STRING_TO_SHORT(fromString(Short::valueOf, "short")),
        DATETIME_TO_SHORT(fromDateTime(DataTypeConverter::safeToShort)),

        RATIONAL_TO_BYTE(fromDouble(value -> safeToByte(safeDoubleToLong(value)))),
        INTEGER_TO_BYTE(fromNumber(value -> safeToByte(safeToLong(value)))),
        BOOL_TO_BYTE(fromBool(value -> value ? (byte) 1 : (byte) 0)),
        STRING_TO_BYTE(fromString(Byte::valueOf, "byte")),
        DATETIME_TO_BYTE(fromDateTime(DataTypeConverter::safeToByte)),

        // TODO floating point conversions are lossy but conversions to integer are not. Are we ok with that?
        RATIONAL_TO_FLOAT(fromDouble(value -> (float) value)),
        INTEGER_TO_FLOAT(fromNumber(Number::floatValue)),
        BOOL_TO_FLOAT(fromBool(value -> value ? 1f : 0f)),
        STRING_TO_FLOAT(fromString(Float::valueOf, "float")),
        DATETIME_TO_FLOAT(fromDateTime(value -> (float) value)),

        RATIONAL_TO_DOUBLE(fromDouble(Double::valueOf)),
        INTEGER_TO_DOUBLE(fromNumber(Number::doubleValue)),
        BOOL_TO_DOUBLE(fromBool(value -> value ? 1d : 0d)),
        STRING_TO_DOUBLE(fromString(Double::valueOf, "double")),
        DATETIME_TO_DOUBLE(fromDateTime(Double::valueOf)),

        RATIONAL_TO_DATETIME(toDateTime(RATIONAL_TO_LONG)),
        INTEGER_TO_DATETIME(toDateTime(INTEGER_TO_LONG)),
        BOOL_TO_DATETIME(toDateTime(BOOL_TO_INT)),
        STRING_TO_DATETIME(fromString(DateUtils::asDateTime, "datetime")),

        NUMERIC_TO_BOOLEAN(fromDouble(value -> value != 0)),
        STRING_TO_BOOLEAN(fromString(DataTypeConverter::convertToBoolean, "boolean")),
        DATETIME_TO_BOOLEAN(fromDateTime(value -> value != 0)),

        BOOL_TO_UNSIGNED_LONG(fromBool(value -> value ? BigInteger.ONE : BigInteger.ZERO)),
        BOOL_TO_LONG(fromBool(value -> value ? 1L : 0L)),

        STRING_TO_IP(o -> {
            if (InetAddresses.isInetAddress(o.toString()) == false) {
                throw new InvalidArgumentException("[{}] is not a valid IPv4 or IPv6 address", o);
            }
            return o;
        }),
        STRING_TO_VERSION(o -> new Version(o.toString()));

        public static final String NAME = "dtc-def";

        private final Function<Object, Object> converter;

        DefaultConverter(Function<Object, Object> converter) {
            this.converter = converter;
        }

        private static Function<Object, Object> fromDouble(DoubleFunction<Object> converter) {
            return (Object l) -> converter.apply(((Number) l).doubleValue());
        }

        private static Function<Object, Object> fromNumber(Function<Number, Object> converter) {
            return l -> converter.apply((Number) l);
        }

        public static Function<Object, Object> fromString(Function<String, Object> converter, String to) {
            return (Object value) -> {
                try {
                    return converter.apply(value.toString());
                } catch (NumberFormatException e) {
                    throw new InvalidArgumentException(e, "cannot cast [{}] to [{}]", value, to);
                } catch (DateTimeParseException | IllegalArgumentException e) {
                    throw new InvalidArgumentException(e, "cannot cast [{}] to [{}]: {}", value, to, e.getMessage());
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

        return dataType.isWholeNumber() ? dataType : LONG;
    }
}
