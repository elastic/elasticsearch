/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.function.DoubleFunction;
import java.util.function.Function;
import java.util.function.LongFunction;

/**
 * Conversions from one data type to another.
 */
public abstract class DataTypeConversion {

    private static final DateTimeFormatter UTC_DATE_FORMATTER = ISODateTimeFormat.dateTimeNoMillis().withZoneUTC();

    public static DataType commonType(DataType left, DataType right) {
        if (left.same(right)) {
            return left;
        }
        if (nullable(left)) {
            return right;
        }
        if (nullable(right)) {
            return left;
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
        if (left instanceof StringType) {
            if (right.isNumeric()) {
                return right;
            }
        }
        if (right instanceof StringType) {
            if (left.isNumeric()) {
                return left;
            }
        }
        // none found
        return null;
    }

    public static boolean nullable(DataType from) {
        return from instanceof NullType;
    }

    public static boolean canConvert(DataType from, DataType to) { // TODO it'd be cleaner and more right to fetch the conversion
        // only primitives are supported so far
        if (from.isComplex() || to.isComplex()) {
            return false;
        }
        
        if (from.getClass() == to.getClass()) {
            return true;
        }
        if (from instanceof NullType) {
            return true;
        }
        
        // anything can be converted to String
        if (to instanceof StringType) {
            return true;
        }
        // also anything can be converted into a bool
        if (to instanceof BooleanType) {
            return true;
        }

        // numeric conversion
        if ((from instanceof StringType || from instanceof BooleanType || from instanceof DateType || from.isNumeric()) && to.isNumeric()) {
            return true;
        }
        // date conversion
        if ((from instanceof DateType || from instanceof StringType || from.isNumeric()) && to instanceof DateType) {
            return true;
        }

        return false;
    }

    /**
     * Get the conversion from one type to another.
     */
    public static Conversion conversionFor(DataType from, DataType to) {
        if (to instanceof StringType) {
            return conversionToString(from);
        }
        if (to instanceof LongType) {
            return conversionToLong(from);
        }
        if (to instanceof IntegerType) {
            return conversionToInt(from);
        }
        if (to instanceof ShortType) {
            return conversionToShort(from);
        }
        if (to instanceof ByteType) {
            return conversionToByte(from);
        }
        if (to instanceof FloatType) {
            return conversionToFloat(from);
        }
        if (to instanceof DoubleType) {
            return conversionToDouble(from);
        }
        if (to instanceof DateType) {
            return conversionToDate(from);
        }
        if (to instanceof BooleanType) {
            return conversionToBoolean(from);
        }
        throw new SqlIllegalArgumentException("cannot convert from [" + from + "] to [" + to + "]");
    }

    private static Conversion conversionToString(DataType from) {
        if (from instanceof DateType) {
            return Conversion.DATE_TO_STRING;
        }
        return Conversion.OTHER_TO_STRING;
    }

    private static Conversion conversionToLong(DataType from) {
        if (from.isRational()) {
            return Conversion.RATIONAL_TO_LONG;
        }
        if (from.isInteger()) {
            return Conversion.INTEGER_TO_LONG;
        }
        if (from instanceof BooleanType) {
            return Conversion.BOOL_TO_INT; // We emit an int here which is ok because of Java's casting rules
        }
        if (from instanceof StringType) {
            return Conversion.STRING_TO_LONG;
        }
        throw new SqlIllegalArgumentException("cannot convert from [" + from + "] to [Long]");
    }

    private static Conversion conversionToInt(DataType from) {
        if (from.isRational()) {
            return Conversion.RATIONAL_TO_INT;
        }
        if (from.isInteger()) {
            return Conversion.INTEGER_TO_INT;
        }
        if (from instanceof BooleanType) {
            return Conversion.BOOL_TO_INT;
        }
        if (from instanceof StringType) {
            return Conversion.STRING_TO_INT;
        }
        throw new SqlIllegalArgumentException("cannot convert from [" + from + "] to [Integer]");
    }

    private static Conversion conversionToShort(DataType from) {
        if (from.isRational()) {
            return Conversion.RATIONAL_TO_SHORT;
        }
        if (from.isInteger()) {
            return Conversion.INTEGER_TO_SHORT;
        }
        if (from instanceof BooleanType) {
            return Conversion.BOOL_TO_SHORT;
        }
        if (from instanceof StringType) {
            return Conversion.STRING_TO_SHORT;
        }
        throw new SqlIllegalArgumentException("cannot convert [" + from + "] to [Short]");
    }

    private static Conversion conversionToByte(DataType from) {
        if (from.isRational()) {
            return Conversion.RATIONAL_TO_BYTE;
        }
        if (from.isInteger()) {
            return Conversion.INTEGER_TO_BYTE;
        }
        if (from instanceof BooleanType) {
            return Conversion.BOOL_TO_BYTE;
        }
        if (from instanceof StringType) {
            return Conversion.STRING_TO_BYTE;
        }
        throw new SqlIllegalArgumentException("cannot convert [" + from + "] to [Byte]");
    }

    private static Conversion conversionToFloat(DataType from) {
        if (from.isRational()) {
            return Conversion.RATIONAL_TO_FLOAT;
        }
        if (from.isInteger()) {
            return Conversion.INTEGER_TO_FLOAT;
        }
        if (from instanceof BooleanType) {
            return Conversion.BOOL_TO_FLOAT;
        }
        if (from instanceof StringType) {
            return Conversion.STRING_TO_FLOAT;
        }
        throw new SqlIllegalArgumentException("cannot convert [" + from + "] to [Float]");
    }

    private static Conversion conversionToDouble(DataType from) {
        if (from.isRational()) {
            return Conversion.RATIONAL_TO_DOUBLE;
        }
        if (from.isInteger()) {
            return Conversion.INTEGER_TO_DOUBLE;
        }
        if (from instanceof BooleanType) {
            return Conversion.BOOL_TO_DOUBLE;
        }
        if (from instanceof StringType) {
            return Conversion.STRING_TO_DOUBLE;
        }
        throw new SqlIllegalArgumentException("cannot convert [" + from + "] to [Double]");
    }

    private static Conversion conversionToDate(DataType from) {
        if (from.isRational()) {
            return Conversion.RATIONAL_TO_LONG;
        }
        if (from.isInteger()) {
            return Conversion.INTEGER_TO_LONG;
        }
        if (from instanceof BooleanType) {
            return Conversion.BOOL_TO_INT; // We emit an int here which is ok because of Java's casting rules
        }
        if (from instanceof StringType) {
            return Conversion.STRING_TO_DATE;
        }
        throw new SqlIllegalArgumentException("cannot convert [" + from + "] to [Date]");
    }

    private static Conversion conversionToBoolean(DataType from) {
        if (from.isNumeric()) {
            return Conversion.NUMERIC_TO_BOOLEAN;
        }
        if (from instanceof StringType) {
            return Conversion.STRING_TO_BOOLEAN;
        }
        throw new SqlIllegalArgumentException("cannot convert [" + from + "] to [Boolean]");
    }

    public static byte safeToByte(long x) {
        if (x > Byte.MAX_VALUE || x < Byte.MIN_VALUE) {
            throw new SqlIllegalArgumentException("Numeric %d out of byte range", Long.toString(x));
        }
        return (byte) x;
    }

    public static short safeToShort(long x) {
        if (x > Short.MAX_VALUE || x < Short.MIN_VALUE) {
            throw new SqlIllegalArgumentException("Numeric %d out of short range", Long.toString(x));
        }
        return (short) x;
    }

    public static int safeToInt(long x) {
        if (x > Integer.MAX_VALUE || x < Integer.MIN_VALUE) {
            // NOCOMMIT should these instead be regular IllegalArgumentExceptions so we throw a 400 error? Or something else?
            throw new SqlIllegalArgumentException("numeric %d out of int range", Long.toString(x));
        }
        return (int) x;
    }

    public static long safeToLong(double x) {
        if (x > Long.MAX_VALUE || x < Long.MIN_VALUE) {
            throw new SqlIllegalArgumentException("[" + x + "] out of [Long] range");
        }
        return Math.round(x);
    }

    public static Object convert(Object value, DataType dataType) {
        DataType detectedType = DataTypes.fromJava(value);
        if (detectedType.equals(dataType)) {
            return value;
        }
        return conversionFor(detectedType, dataType).convert(value);
    }

    /**
     * Reference to a data type conversion that can be serialized. Note that the position in the enum
     * is important because it is used for serialization.
     */
    public enum Conversion {
        DATE_TO_STRING(fromLong(UTC_DATE_FORMATTER::print)),
        OTHER_TO_STRING(String::valueOf),
        RATIONAL_TO_LONG(fromDouble(DataTypeConversion::safeToLong)),
        INTEGER_TO_LONG(fromLong(value -> value)),
        STRING_TO_LONG(fromString(Long::valueOf, "Long")),
        RATIONAL_TO_INT(fromDouble(value -> safeToInt(safeToLong(value)))),
        INTEGER_TO_INT(fromLong(DataTypeConversion::safeToInt)),
        BOOL_TO_INT(fromBool(value -> value ? 1 : 0)),
        STRING_TO_INT(fromString(Integer::valueOf, "Int")),
        RATIONAL_TO_SHORT(fromDouble(value -> safeToShort(safeToLong(value)))),
        INTEGER_TO_SHORT(fromLong(DataTypeConversion::safeToShort)),
        BOOL_TO_SHORT(fromBool(value -> value ? (short) 1 : (short) 0)),
        STRING_TO_SHORT(fromString(Short::valueOf, "Short")),
        RATIONAL_TO_BYTE(fromDouble(value -> safeToByte(safeToLong(value)))),
        INTEGER_TO_BYTE(fromLong(DataTypeConversion::safeToByte)),
        BOOL_TO_BYTE(fromBool(value -> value ? (byte) 1 : (byte) 0)),
        STRING_TO_BYTE(fromString(Byte::valueOf, "Byte")),
        // TODO floating point conversions are lossy but conversions to integer conversions are not. Are we ok with that?
        RATIONAL_TO_FLOAT(fromDouble(value -> (float) value)),
        INTEGER_TO_FLOAT(fromLong(value -> (float) value)),
        BOOL_TO_FLOAT(fromBool(value -> value ? 1f : 0f)),
        STRING_TO_FLOAT(fromString(Float::valueOf, "Float")),
        RATIONAL_TO_DOUBLE(fromDouble(value -> value)),
        INTEGER_TO_DOUBLE(fromLong(Double::valueOf)),
        BOOL_TO_DOUBLE(fromBool(value -> value ? 1d: 0d)),
        STRING_TO_DOUBLE(fromString(Double::valueOf, "Double")),
        STRING_TO_DATE(fromString(UTC_DATE_FORMATTER::parseMillis, "Date")),
        NUMERIC_TO_BOOLEAN(fromLong(value -> value != 0)),
        STRING_TO_BOOLEAN(fromString(Booleans::isBoolean, "Boolean")), // NOCOMMIT probably wrong
        ;

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
                    throw new SqlIllegalArgumentException("cannot cast [%s] to [%s]", value, to, e);
                } catch (IllegalArgumentException e) {
                    throw new SqlIllegalArgumentException("cannot cast [%s] to [%s]:%s", value, to, e.getMessage(), e);
                }
            };
        }

        private static Function<Object, Object> fromBool(Function<Boolean, Object> converter) {
            return (Object l) -> converter.apply(((Boolean) l));
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

        return dataType.isInteger() ? dataType : DataTypes.LONG;
    }
}