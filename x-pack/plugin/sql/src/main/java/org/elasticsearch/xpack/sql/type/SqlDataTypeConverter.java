/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.type.Converter;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypeConverter;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.literal.interval.Intervals;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.io.IOException;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.type.DataTypeConverter.DefaultConverter.BOOL_TO_INT;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.DefaultConverter.DATETIME_TO_BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.DefaultConverter.DATETIME_TO_BYTE;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.DefaultConverter.DATETIME_TO_DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.DefaultConverter.DATETIME_TO_FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.DefaultConverter.DATETIME_TO_INT;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.DefaultConverter.DATETIME_TO_LONG;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.DefaultConverter.DATETIME_TO_SHORT;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.DefaultConverter.IDENTITY;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.DefaultConverter.INTEGER_TO_LONG;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.DefaultConverter.RATIONAL_TO_LONG;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.DefaultConverter.TO_NULL;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.BYTE;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.NULL;
import static org.elasticsearch.xpack.ql.type.DataTypes.SHORT;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.isPrimitive;
import static org.elasticsearch.xpack.ql.type.DataTypes.isString;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.DATE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.TIME;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.isInterval;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.isYearMonthInterval;


public final class SqlDataTypeConverter {

    private SqlDataTypeConverter() {}

    public static DataType commonType(DataType left, DataType right) {
        DataType common = DataTypeConverter.commonType(left, right);

        if (common != null) {
            return common;
        }

        // interval and dates
        if (left == DATE) {
            if (isYearMonthInterval(right)) {
                return left;
            }
            // promote
            return DATETIME;
        }
        if (right == DATE) {
            if (isYearMonthInterval(left)) {
                return right;
            }
            // promote
            return DATETIME;
        }
        if (left == TIME) {
            if (right == DATE) {
                return DATETIME;
            }
            if (isInterval(right)) {
                return left;
            }
        }
        if (right == TIME) {
            if (left == DATE) {
                return DATETIME;
            }
            if (isInterval(left)) {
                return right;
            }
        }
        if (left == DATETIME) {
            if (right == DATE || right == TIME) {
                return left;
            }
            if (isInterval(right)) {
                return left;
            }
        }
        if (right == DATETIME) {
            if (left == DATE || left == TIME) {
                return right;
            }
            if (isInterval(left)) {
                return right;
            }
        }
        // Interval * integer is a valid operation
        if (isInterval(left)) {
            if (right.isInteger()) {
                return left;
            }
        }
        if (isInterval(right)) {
            if (left.isInteger()) {
                return right;
            }
        }
        if (isInterval(left)) {
            // intervals widening
            if (isInterval(right)) {
                // null returned for incompatible intervals
                return Intervals.compatibleInterval(left, right);
            }
        }

        return null;
    }

    public static boolean canConvert(DataType from, DataType to) {
        // Special handling for nulls and if conversion is not requires
        if (from == to || from == NULL) {
            return true;
        }
        // only primitives are supported so far
        return isPrimitive(from) && isPrimitive(to) && converterFor(from, to) != null;
    }

    public static Converter converterFor(DataType from, DataType to) {
        // Special handling for nulls and if conversion is not requires
        if (from == to) {
            return IDENTITY;
        }
        if (to == NULL || from == NULL) {
            return TO_NULL;
        }
        // proper converters
        if (to == DATE) {
            return conversionToDate(from);
        }
        if (to == TIME) {
            return conversionToTime(from);
        }
        // extend the default converter with DATE and TIME
        if (from == DATE || from == TIME) {
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
            if (to == DATETIME) {
                return conversionToDateTime(from);
            }
            if (to == BOOLEAN) {
                return conversionToBoolean(from);
            }
        }
        // fallback to default
        return DataTypeConverter.converterFor(from, to);
    }
    
    private static Converter conversionToString(DataType from) {
        if (from == DATE) {
            return SqlConverter.DATE_TO_STRING;
        }
        if (from == TIME) {
            return SqlConverter.TIME_TO_STRING;
        }
        return null;
    }
    
    private static Converter conversionToLong(DataType from) {
        if (from == DATE) {
            return SqlConverter.DATE_TO_LONG;
        }
        if (from == TIME) {
            return SqlConverter.TIME_TO_LONG;
        }
        return null;
    }

    private static Converter conversionToInt(DataType from) {
        if (from == DATE) {
            return SqlConverter.DATE_TO_INT;
        }
        if (from == TIME) {
            return SqlConverter.TIME_TO_INT;
        }
        return null;
    }

    private static Converter conversionToShort(DataType from) {
        if (from == DATE) {
            return SqlConverter.DATE_TO_SHORT;
        }
        if (from == TIME) {
            return SqlConverter.TIME_TO_SHORT;
        }
        return null;
    }

    private static Converter conversionToByte(DataType from) {
        if (from == DATE) {
            return SqlConverter.DATE_TO_BYTE;
        }
        if (from == TIME) {
            return SqlConverter.TIME_TO_BYTE;
        }
        return null;
    }

    private static Converter conversionToFloat(DataType from) {
        if (from == DATE) {
            return SqlConverter.DATE_TO_FLOAT;
        }
        if (from == TIME) {
            return SqlConverter.TIME_TO_FLOAT;
        }
        return null;
    }

    private static Converter conversionToDouble(DataType from) {
        if (from == DATE) {
            return SqlConverter.DATE_TO_DOUBLE;
        }
        if (from == TIME) {
            return SqlConverter.TIME_TO_DOUBLE;
        }
        return null;
    }

    private static Converter conversionToDateTime(DataType from) {
        if (from == DATE) {
            return SqlConverter.DATE_TO_DATETIME;
        }

        return DataTypeConverter.converterFor(from, DATETIME);
    }

    private static Converter conversionToBoolean(DataType from) {
        if (from == DATE) {
            return SqlConverter.DATE_TO_BOOLEAN;
        }
        if (from == TIME) {
            return SqlConverter.TIME_TO_BOOLEAN;
        }
        return null;
    }

    private static Converter conversionToDate(DataType from) {
        if (from.isRational()) {
            return SqlConverter.RATIONAL_TO_DATE;
        }
        if (from.isInteger()) {
            return SqlConverter.INTEGER_TO_DATE;
        }
        if (from == BOOLEAN) {
            return SqlConverter.BOOL_TO_DATE; // We emit an int here which is ok because of Java's casting rules
        }
        if (isString(from)) {
            return SqlConverter.STRING_TO_DATE;
        }
        if (from == DATETIME) {
            return SqlConverter.DATETIME_TO_DATE;
        }
        return null;
    }

    private static Converter conversionToTime(DataType from) {
        if (from.isRational()) {
            return SqlConverter.RATIONAL_TO_TIME;
        }
        if (from.isInteger()) {
            return SqlConverter.INTEGER_TO_TIME;
        }
        if (from == BOOLEAN) {
            return SqlConverter.BOOL_TO_TIME; // We emit an int here which is ok because of Java's casting rules
        }
        if (isString(from)) {
            return SqlConverter.STRING_TO_TIME;
        }
        if (from == DATE) {
            return SqlConverter.DATE_TO_TIME;
        }
        if (from == DATETIME) {
            return SqlConverter.DATETIME_TO_TIME;
        }
        return null;
    }

    public static Object convert(Object value, DataType dataType) {
        DataType detectedType = SqlDataTypes.fromJava(value);

        if (detectedType == null) {
            throw new SqlIllegalArgumentException("cannot detect datatype for [{}]", value);
        }

        if (detectedType == dataType || value == null) {
            return value;
        }

        Converter converter = converterFor(detectedType, dataType);

        if (converter == null) {
            throw new SqlIllegalArgumentException("cannot convert from [{}] to [{}]", value, dataType.typeName());
        }

        return converter.convert(value);
    }

    /**
     * Reference to a data type conversion that can be serialized. Note that the position in the enum
     * is important because it is used for serialization.
     */
    public enum SqlConverter implements Converter {
        DATE_TO_STRING(o -> DateUtils.toDateString((ZonedDateTime) o)),
        TIME_TO_STRING(o -> DateUtils.toTimeString((OffsetTime) o)),
        
        DATE_TO_LONG(delegate(DATETIME_TO_LONG)),
        TIME_TO_LONG(fromTime(value -> value)),

        DATE_TO_INT(delegate(DATETIME_TO_INT)),
        TIME_TO_INT(fromTime(DataTypeConverter::safeToInt)),

        DATE_TO_SHORT(delegate(DATETIME_TO_SHORT)),
        TIME_TO_SHORT(fromTime(DataTypeConverter::safeToShort)),

        DATE_TO_BYTE(delegate(DATETIME_TO_BYTE)),
        TIME_TO_BYTE(fromTime(DataTypeConverter::safeToByte)),

        DATE_TO_FLOAT(delegate(DATETIME_TO_FLOAT)),
        TIME_TO_FLOAT(fromTime(value -> (float) value)),

        DATE_TO_DOUBLE(delegate(DATETIME_TO_DOUBLE)),
        TIME_TO_DOUBLE(fromTime(Double::valueOf)),

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

        DATE_TO_DATETIME(value -> value),

        DATE_TO_BOOLEAN(delegate(DATETIME_TO_BOOLEAN)),
        TIME_TO_BOOLEAN(fromTime(value -> value != 0));

        public static final String NAME = "dtc-sql";
        
        private final Function<Object, Object> converter;

        SqlConverter(Function<Object, Object> converter) {
            this.converter = converter;
        }

        private static Function<Object, Object> fromTime(Function<Long, Object> converter) {
            return l -> converter.apply(((OffsetTime) l).atDate(DateUtils.EPOCH).toInstant().toEpochMilli());
        }

        private static Function<Object, Object> toDate(Converter conversion) {
            return l -> DateUtils.asDateOnly(((Number) conversion.convert(l)).longValue());
        }

        private static Function<Object, Object> toTime(Converter conversion) {
            return l -> DateUtils.asTimeOnly(((Number) conversion.convert(l)).longValue());
        }

        private static Function<Object, Object> fromDatetimeToDate() {
            return l -> DateUtils.asDateOnly((ZonedDateTime) l);
        }

        private static Function<Object, Object> fromDatetimeToTime() {
            return l -> ((ZonedDateTime) l).toOffsetDateTime().toOffsetTime();
        }

        private static Function<Object, Object> delegate(Converter converter) {
            return converter::convert;
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
            return in.readEnum(SqlConverter.class);
        }
    }
}