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

// utilities around DataTypes and SQL
public abstract class DataTypeConvertion {

    private static DateTimeFormatter UTC_DATE_FORMATTER = ISODateTimeFormat.dateTimeNoMillis();//.withZoneUTC();

    public static boolean canConvert(DataType from, DataType to) {
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

    @SuppressWarnings("unchecked")
    public static <T> T convert(Object value, DataType from, DataType to) {
        if (value == null) {
            return null;
        }
        Object result = null;
        if (to instanceof StringType) {
            result = toString(value, from);
        }
        else if (to instanceof LongType) {
            result = toLong(value, from);
        }
        else if (to instanceof IntegerType) {
            result = toInt(value, from);
        }
        else if (to instanceof ShortType) {
            result = toShort(value, from);
        }
        else if (to instanceof ByteType) {
            result = toByte(value, from);
        }
        else if (to instanceof FloatType) {
            result = toFloat(value, from);
        }
        else if (to instanceof DoubleType) {
            result = toDouble(value, from);
        }
        else if (to instanceof DateType) {
            result = toDate(value, from);
        }
        else if (to instanceof BooleanType) {
            result = toBoolean(value, from);
        }

        return (T) result;
    }

    private static Boolean toBoolean(Object value, DataType from) {
        if (value instanceof Number) {
            return ((Number) value).longValue() != 0;
        }
        if (value instanceof String) {
            return Booleans.isBoolean(value.toString());
        }

        throw new SqlIllegalArgumentException("Does not know how to convert object %s to Boolean", value);
    }

    private static String toString(Object value, DataType from) {
        if (from instanceof DateType) {
            // TODO: maybe detect the cast and return the source instead
            return UTC_DATE_FORMATTER.print((Long) value);
        }
        
        return String.valueOf(value);
    }

    private static Byte toByte(Object value, DataType from) {
        if (from.isRational()) {
            return safeToByte(safeToLong(((Number) value).doubleValue()));
        }
        if (from.isInteger()) {
            return safeToByte(((Number) value).longValue());
        }
        if (from instanceof BooleanType) {
            return Byte.valueOf(((Boolean) value).booleanValue() ? (byte) 1 : (byte) 0);
        }
        if (from instanceof StringType) {
            try {
                return Byte.parseByte(String.valueOf(value));
            } catch (NumberFormatException nfe) {
                throw new SqlIllegalArgumentException("Cannot cast %s to Byte", value);
            }
        }
        throw new SqlIllegalArgumentException("Does not know how to convert object %s to Byte", value);
    }

    private static Short toShort(Object value, DataType from) {
        if (from.isRational()) {
            return safeToShort(safeToLong(((Number) value).doubleValue()));
        }
        if (from.isInteger()) {
            return safeToShort(((Number) value).longValue());
        }
        if (from instanceof BooleanType) {
            return Short.valueOf(((Boolean) value).booleanValue() ? (short) 1 : (short) 0);
        }
        if (from instanceof StringType) {
            try {
                return Short.parseShort(String.valueOf(value));
            } catch (NumberFormatException nfe) {
                throw new SqlIllegalArgumentException("Cannot cast %s to Short", value);
            }
        }
        throw new SqlIllegalArgumentException("Does not know how to convert object %s to Short", value);
    }

    private static Integer toInt(Object value, DataType from) {
        if (from.isRational()) {
            return safeToInt(safeToLong(((Number) value).doubleValue()));
        }
        if (from.isInteger()) {
            return safeToInt(((Number) value).longValue());
        }
        if (from instanceof BooleanType) {
            return Integer.valueOf(((Boolean) value).booleanValue() ? 1 : 0);
        }
        if (from instanceof StringType) {
            try {
                return Integer.parseInt(String.valueOf(value));
            } catch (NumberFormatException nfe) {
                throw new SqlIllegalArgumentException("Cannot cast %s to Integer", value);
            }
        }
        throw new SqlIllegalArgumentException("Does not know how to convert object %s to Integer", value);
    }

    private static Long toLong(Object value, DataType from) {
        if (from.isRational()) {
            return safeToLong(((Number) value).doubleValue());
        }
        if (from.isInteger()) {
            return Long.valueOf(((Number) value).longValue());
        }
        if (from instanceof BooleanType) {
            return Long.valueOf(((Boolean) value).booleanValue() ? 1 : 0);
        }
        if (from instanceof StringType) {
            try {
                return Long.parseLong(String.valueOf(value));
            } catch (NumberFormatException nfe) {
                throw new SqlIllegalArgumentException("Cannot cast %s to Long", value);
            }
        }
        throw new SqlIllegalArgumentException("Does not know how to convert object %s to Long", value);
    }

    private static Float toFloat(Object value, DataType from) {
        if (from.isRational()) {
            return new Float(((Number) value).doubleValue());
        }
        if (from.isInteger()) {
            return Float.valueOf(((Number) value).longValue());
        }
        if (from instanceof BooleanType) {
            return Float.valueOf(((Boolean) value).booleanValue() ? 1 : 0);
        }
        if (from instanceof StringType) {
            try {
                return Float.parseFloat(String.valueOf(value));
            } catch (NumberFormatException nfe) {
                throw new SqlIllegalArgumentException("Cannot cast %s to Float", value);
            }
        }
        throw new SqlIllegalArgumentException("Does not know how to convert object %s to Float", value);
    }

    private static Double toDouble(Object value, DataType from) {
        if (from.isRational()) {
            return new Double(((Number) value).doubleValue());
        }
        if (from.isInteger()) {
            return Double.valueOf(((Number) value).longValue());
        }
        if (from instanceof BooleanType) {
            return Double.valueOf(((Boolean) value).booleanValue() ? 1 : 0);
        }
        if (from instanceof StringType) {
            try {
                return Double.parseDouble(String.valueOf(value));
            } catch (NumberFormatException nfe) {
                throw new SqlIllegalArgumentException("Cannot cast %s to Double", value);
            }
        }
        throw new SqlIllegalArgumentException("Does not know how to convert object %s to Double", value);
    }

    // dates are returned in long format (for UTC)
    private static Long toDate(Object value, DataType from) {
        if (from.isRational()) {
            return safeToLong(((Number) value).doubleValue());
        }
        if (from.isInteger()) {
            return Long.valueOf(((Number) value).longValue());
        }
        if (from instanceof BooleanType) {
            return Long.valueOf(((Boolean) value).booleanValue() ? 1 : 0);
        }
        if (from instanceof StringType) {
            try {
                return UTC_DATE_FORMATTER.parseMillis(String.valueOf(value));
            } catch (IllegalArgumentException iae) {
                throw new SqlIllegalArgumentException("Cannot cast %s to Date", value);
            }
        }
        throw new SqlIllegalArgumentException("Does not know how to convert object %s to Double", value);
    }

    private static byte safeToByte(long x) {
        if (x > Byte.MAX_VALUE || x < Byte.MIN_VALUE) {
            throw new SqlIllegalArgumentException("Numeric %d out of byte range", Long.toString(x));
        }
        return (byte) x;
    }

    private static short safeToShort(long x) {
        if (x > Short.MAX_VALUE || x < Short.MIN_VALUE) {
            throw new SqlIllegalArgumentException("Numeric %d out of short range", Long.toString(x));
        }
        return (short) x;
    }

    private static int safeToInt(long x) {
        if (x > Integer.MAX_VALUE || x < Integer.MIN_VALUE) {
            throw new SqlIllegalArgumentException("Numeric %d out of int range", Long.toString(x));
        }
        return (int) x;
    }

    private static long safeToLong(double x) {
        if (x > Long.MAX_VALUE || x < Long.MIN_VALUE) {
            throw new SqlIllegalArgumentException("Numeric %d out of long range", Double.toString(x));
        }
        return Math.round(x);
    }

    public static boolean nullable(DataType from, DataType to) {
        if (from instanceof NullType) {
            return true;
        }

        return false;
    }
}