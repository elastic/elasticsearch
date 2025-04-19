/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder.Metric;
import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.Converter;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToBoolean;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCartesianPoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCartesianShape;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDateNanos;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatePeriod;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeoPoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeoShape;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIpLeadingZerosRejected;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToTimeDuration;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToUnsignedLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToVersion;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.versionfield.Version;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Map.entry;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.TIME_DURATION;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.core.type.DataType.isDateTime;
import static org.elasticsearch.xpack.esql.core.type.DataType.isDateTimeOrNanosOrTemporal;
import static org.elasticsearch.xpack.esql.core.type.DataType.isNullOrDatePeriod;
import static org.elasticsearch.xpack.esql.core.type.DataType.isNullOrTemporalAmount;
import static org.elasticsearch.xpack.esql.core.type.DataType.isNullOrTimeDuration;
import static org.elasticsearch.xpack.esql.core.type.DataType.isString;
import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeDoubleToLong;
import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeToInt;
import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeToLong;
import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeToUnsignedLong;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.ONE_AS_UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.ZERO_AS_UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.asLongUnsigned;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.asUnsignedLong;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongAsNumber;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

public class EsqlDataTypeConverter {

    public static final DateFormatter DEFAULT_DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_optional_time");
    public static final DateFormatter DEFAULT_DATE_NANOS_FORMATTER = DateFormatter.forPattern("strict_date_optional_time_nanos");

    public static final DateFormatter HOUR_MINUTE_SECOND = DateFormatter.forPattern("strict_hour_minute_second_fraction");

    private static final Map<DataType, BiFunction<Source, Expression, AbstractConvertFunction>> TYPE_TO_CONVERTER_FUNCTION = Map.ofEntries(
        entry(BOOLEAN, ToBoolean::new),
        entry(CARTESIAN_POINT, ToCartesianPoint::new),
        entry(CARTESIAN_SHAPE, ToCartesianShape::new),
        entry(DATETIME, ToDatetime::new),
        entry(DATE_NANOS, ToDateNanos::new),
        // ToDegrees, typeless
        entry(DOUBLE, ToDouble::new),
        entry(GEO_POINT, ToGeoPoint::new),
        entry(GEO_SHAPE, ToGeoShape::new),
        entry(INTEGER, ToInteger::new),
        entry(IP, ToIpLeadingZerosRejected::new),
        entry(LONG, ToLong::new),
        // ToRadians, typeless
        entry(KEYWORD, ToString::new),
        entry(UNSIGNED_LONG, ToUnsignedLong::new),
        entry(VERSION, ToVersion::new),
        entry(DATE_PERIOD, ToDatePeriod::new),
        entry(TIME_DURATION, ToTimeDuration::new)
    );

    public enum INTERVALS {
        // TIME_DURATION,
        MILLISECOND,
        MILLISECONDS,
        MS,
        SECOND,
        SECONDS,
        SEC,
        S,
        MINUTE,
        MINUTES,
        MIN,
        HOUR,
        HOURS,
        H,
        // DATE_PERIOD
        DAY,
        DAYS,
        D,
        WEEK,
        WEEKS,
        W,
        MONTH,
        MONTHS,
        MO,
        QUARTER,
        QUARTERS,
        Q,
        YEAR,
        YEARS,
        YR,
        Y;
    }

    public static List<INTERVALS> TIME_DURATIONS = List.of(
        INTERVALS.MILLISECOND,
        INTERVALS.MILLISECONDS,
        INTERVALS.MS,
        INTERVALS.SECOND,
        INTERVALS.SECONDS,
        INTERVALS.SEC,
        INTERVALS.S,
        INTERVALS.MINUTE,
        INTERVALS.MINUTES,
        INTERVALS.MIN,
        INTERVALS.HOUR,
        INTERVALS.HOURS,
        INTERVALS.H
    );

    public static List<INTERVALS> DATE_PERIODS = List.of(
        INTERVALS.DAY,
        INTERVALS.DAYS,
        INTERVALS.D,
        INTERVALS.WEEK,
        INTERVALS.WEEKS,
        INTERVALS.W,
        INTERVALS.MONTH,
        INTERVALS.MONTHS,
        INTERVALS.MO,
        INTERVALS.QUARTER,
        INTERVALS.QUARTERS,
        INTERVALS.Q,
        INTERVALS.YEAR,
        INTERVALS.YEARS,
        INTERVALS.YR,
        INTERVALS.Y
    );

    public static final String INVALID_INTERVAL_ERROR =
        "Invalid interval value in [{}], expected integer followed by one of {} but got [{}]";

    public static Converter converterFor(DataType from, DataType to) {
        // TODO move EXPRESSION_TO_LONG here if there is no regression
        if (isString(from)) {
            if (to == DataType.DATETIME) {
                return EsqlConverter.STRING_TO_DATETIME;
            }
            if (to == DATE_NANOS) {
                return EsqlConverter.STRING_TO_DATE_NANOS;
            }
            if (to == DataType.IP) {
                return EsqlConverter.STRING_TO_IP;
            }
            if (to == DataType.VERSION) {
                return EsqlConverter.STRING_TO_VERSION;
            }
            if (to == DataType.DOUBLE) {
                return EsqlConverter.STRING_TO_DOUBLE;
            }
            if (to == DataType.LONG) {
                return EsqlConverter.STRING_TO_LONG;
            }
            if (to == DataType.INTEGER) {
                return EsqlConverter.STRING_TO_INT;
            }
            if (to == DataType.BOOLEAN) {
                return EsqlConverter.STRING_TO_BOOLEAN;
            }
            if (DataType.isSpatial(to)) {
                return EsqlConverter.STRING_TO_SPATIAL;
            }
            if (to == DataType.TIME_DURATION) {
                return EsqlConverter.STRING_TO_TIME_DURATION;
            }
            if (to == DataType.DATE_PERIOD) {
                return EsqlConverter.STRING_TO_DATE_PERIOD;
            }
        }
        Converter converter = DataTypeConverter.converterFor(from, to);
        if (converter != null) {
            return converter;
        }
        return null;
    }

    public static TemporalAmount foldToTemporalAmount(FoldContext ctx, Expression field, String sourceText, DataType expectedType) {
        if (field.foldable()) {
            Object v = field.fold(ctx);
            if (v instanceof BytesRef b) {
                try {
                    return EsqlDataTypeConverter.parseTemporalAmount(b.utf8ToString(), expectedType);
                } catch (ParsingException e) {
                    throw new IllegalArgumentException(
                        LoggerMessageFormat.format(
                            null,
                            INVALID_INTERVAL_ERROR,
                            sourceText,
                            expectedType == DATE_PERIOD ? DATE_PERIODS : TIME_DURATIONS,
                            b.utf8ToString()
                        )
                    );
                }
            } else if (v instanceof TemporalAmount t) {
                return t;
            }
        }

        throw new IllegalArgumentException(
            LoggerMessageFormat.format(
                null,
                "argument of [{}] must be a constant, received [{}]",
                field.sourceText(),
                Expressions.name(field)
            )
        );
    }

    public static TemporalAmount parseTemporalAmount(Object val, DataType expectedType) {
        String errorMessage = "Cannot parse [{}] to {}";
        String str = String.valueOf(val);
        if (str == null) {
            return null;
        }
        StringBuilder value = new StringBuilder();
        StringBuilder temporalUnit = new StringBuilder();
        separateValueAndTemporalUnitForTemporalAmount(str.strip(), value, temporalUnit, errorMessage, expectedType.toString());
        if ((value.isEmpty() || temporalUnit.isEmpty()) == false) {
            try {
                TemporalAmount result = parseTemporalAmount(Integer.parseInt(value.toString()), temporalUnit.toString(), Source.EMPTY);
                if (DataType.DATE_PERIOD == expectedType && result instanceof Period
                    || DataType.TIME_DURATION == expectedType && result instanceof Duration) {
                    return result;
                }
                if (result instanceof Period && expectedType == DataType.TIME_DURATION) {
                    errorMessage += ", did you mean " + DataType.DATE_PERIOD + "?";
                }
                if (result instanceof Duration && expectedType == DataType.DATE_PERIOD) {
                    errorMessage += ", did you mean " + DataType.TIME_DURATION + "?";
                }
            } catch (NumberFormatException ex) {
                // wrong pattern
            }
        }
        throw new ParsingException(Source.EMPTY, errorMessage, val, expectedType);
    }

    public static TemporalAmount maybeParseTemporalAmount(String str) {
        // The string literal can be either Date_Period or Time_Duration, derive the data type from its temporal unit
        String errorMessage = "Cannot parse [{}] to {}";
        String expectedTypes = DATE_PERIOD + " or " + TIME_DURATION;
        StringBuilder value = new StringBuilder();
        StringBuilder temporalUnit = new StringBuilder();
        separateValueAndTemporalUnitForTemporalAmount(str, value, temporalUnit, errorMessage, expectedTypes);
        if ((value.isEmpty() || temporalUnit.isEmpty()) == false) {
            try {
                return parseTemporalAmount(Integer.parseInt(value.toString()), temporalUnit.toString(), Source.EMPTY);
            } catch (NumberFormatException ex) {
                throw new ParsingException(Source.EMPTY, errorMessage, str, expectedTypes);
            }
        }
        return null;
    }

    private static void separateValueAndTemporalUnitForTemporalAmount(
        String temporalAmount,
        StringBuilder value,
        StringBuilder temporalUnit,
        String errorMessage,
        String expectedType
    ) {
        StringBuilder nextBuffer = value;
        boolean lastWasSpace = false;
        for (char c : temporalAmount.toCharArray()) {
            if (c == ' ') {
                if (lastWasSpace == false) {
                    nextBuffer = nextBuffer == value ? temporalUnit : null;
                }
                lastWasSpace = true;
                continue;
            }
            if (nextBuffer == null) {
                throw new ParsingException(Source.EMPTY, errorMessage, temporalAmount, expectedType);
            }
            nextBuffer.append(c);
            lastWasSpace = false;
        }
    }

    /**
     * Converts arbitrary object to the desired data type.
     * <p>
     * Throws QlIllegalArgumentException if such conversion is not possible
     */
    public static Object convert(Object value, DataType dataType) {
        DataType detectedType = DataType.fromJava(value);
        if (detectedType == dataType || value == null) {
            return value;
        }
        Converter converter = converterFor(detectedType, dataType);

        if (converter == null) {
            throw new QlIllegalArgumentException(
                "cannot convert from [{}], type [{}] to [{}]",
                value,
                detectedType.typeName(),
                dataType.typeName()
            );
        }

        return converter.convert(value);
    }

    /**
     * Returns the type compatible with both left and right types
     * <p>
     * If one of the types is null - returns another type
     * If both types are numeric - returns type with the highest precision int &lt; long &lt; float &lt; double
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
        if (isDateTimeOrNanosOrTemporal(left) || isDateTimeOrNanosOrTemporal(right)) {
            if ((isDateTime(left) && isNullOrTemporalAmount(right)) || (isNullOrTemporalAmount(left) && isDateTime(right))) {
                return DATETIME;
            }
            if ((left == DATE_NANOS && isNullOrTemporalAmount(right)) || (isNullOrTemporalAmount(left) && right == DATE_NANOS)) {
                return DATE_NANOS;
            }
            if (isNullOrTimeDuration(left) && isNullOrTimeDuration(right)) {
                return TIME_DURATION;
            }
            if (isNullOrDatePeriod(left) && isNullOrDatePeriod(right)) {
                return DATE_PERIOD;
            }
            if ((isDateTime(left) && right == DATE_NANOS) || (left == DATE_NANOS && isDateTime(right))) {
                return DATE_NANOS;
            }
        }
        if (isString(left) && isString(right)) {
            // Both TEXT and SEMANTIC_TEXT are processed as KEYWORD
            return KEYWORD;
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
        // none found
        return null;
    }

    // generally supporting abbreviations from https://en.wikipedia.org/wiki/Unit_of_time
    public static TemporalAmount parseTemporalAmount(Number value, String temporalUnit, Source source) throws InvalidArgumentException,
        ArithmeticException, ParsingException {
        try {
            return switch (INTERVALS.valueOf(temporalUnit.toUpperCase(Locale.ROOT))) {
                case MILLISECOND, MILLISECONDS, MS -> Duration.ofMillis(safeToLong(value));
                case SECOND, SECONDS, SEC, S -> Duration.ofSeconds(safeToLong(value));
                case MINUTE, MINUTES, MIN -> Duration.ofMinutes(safeToLong(value));
                case HOUR, HOURS, H -> Duration.ofHours(safeToLong(value));

                case DAY, DAYS, D -> Period.ofDays(safeToInt(safeToLong(value)));
                case WEEK, WEEKS, W -> Period.ofWeeks(safeToInt(safeToLong(value)));
                case MONTH, MONTHS, MO -> Period.ofMonths(safeToInt(safeToLong(value)));
                case QUARTER, QUARTERS, Q -> Period.ofMonths(safeToInt(Math.multiplyExact(3L, safeToLong(value))));
                case YEAR, YEARS, YR, Y -> Period.ofYears(safeToInt(safeToLong(value)));
            };
        } catch (IllegalArgumentException e) {
            throw new ParsingException(source, "Unexpected temporal unit: '{}'", temporalUnit);
        }
    }

    /**
     * The following conversions are used by DateExtract.
     */
    private static ChronoField stringToChrono(Object field) {
        ChronoField chronoField = null;
        try {
            BytesRef br = BytesRefs.toBytesRef(field);
            chronoField = ChronoField.valueOf(br.utf8ToString().toUpperCase(Locale.ROOT));
        } catch (Exception e) {
            return null;
        }
        return chronoField;
    }

    public static long chronoToLong(long dateTime, BytesRef chronoField, ZoneId zone) {
        ChronoField chrono = ChronoField.valueOf(chronoField.utf8ToString().toUpperCase(Locale.ROOT));
        return chronoToLong(dateTime, chrono, zone);
    }

    public static long chronoToLong(long dateTime, ChronoField chronoField, ZoneId zone) {
        return Instant.ofEpochMilli(dateTime).atZone(zone).getLong(chronoField);
    }

    /**
     * Extract the given {@link ChronoField} value from a date specified as a long number of nanoseconds since epoch
     * @param dateNanos - long nanoseconds since epoch
     * @param chronoField - The field to extract
     * @param zone - Timezone for the given date
     * @return - long representing the given ChronoField value
     */
    public static long chronoToLongNanos(long dateNanos, BytesRef chronoField, ZoneId zone) {
        ChronoField chrono = ChronoField.valueOf(chronoField.utf8ToString().toUpperCase(Locale.ROOT));
        return chronoToLongNanos(dateNanos, chrono, zone);
    }

    /**
     * Extract the given {@link ChronoField} value from a date specified as a long number of nanoseconds since epoch
     * @param dateNanos - long nanoseconds since epoch
     * @param chronoField - The field to extract
     * @param zone - Timezone for the given date
     * @return - long representing the given ChronoField value
     */
    public static long chronoToLongNanos(long dateNanos, ChronoField chronoField, ZoneId zone) {
        return DateUtils.toInstant(dateNanos).atZone(zone).getLong(chronoField);
    }

    /**
     * The following conversions are between String and other data types.
     */
    public static BytesRef stringToIP(BytesRef field) {
        return StringUtils.parseIP(field.utf8ToString());
    }

    public static BytesRef stringToIP(String field) {
        return StringUtils.parseIP(field);
    }

    public static String ipToString(BytesRef field) {
        return DocValueFormat.IP.format(field);
    }

    public static BytesRef stringToVersion(BytesRef field) {
        return new Version(field.utf8ToString()).toBytesRef();
    }

    public static BytesRef stringToVersion(String field) {
        return new Version(field).toBytesRef();
    }

    public static String versionToString(BytesRef field) {
        return new Version(field).toString();
    }

    public static String versionToString(Version field) {
        return field.toString();
    }

    public static String spatialToString(BytesRef field) {
        return UNSPECIFIED.wkbToWkt(field);
    }

    public static BytesRef stringToSpatial(String field) {
        return UNSPECIFIED.wktToWkb(field);
    }

    public static long dateTimeToLong(String dateTime) {
        return DEFAULT_DATE_TIME_FORMATTER.parseMillis(dateTime);
    }

    public static long dateTimeToLong(String dateTime, DateFormatter formatter) {
        return formatter == null ? dateTimeToLong(dateTime) : formatter.parseMillis(dateTime);
    }

    public static long dateNanosToLong(String dateNano) {
        return dateNanosToLong(dateNano, DEFAULT_DATE_NANOS_FORMATTER);
    }

    public static long dateNanosToLong(String dateNano, DateFormatter formatter) {
        Instant parsed = DateFormatters.from(formatter.parse(dateNano)).toInstant();
        return DateUtils.toLong(parsed);
    }

    public static String dateWithTypeToString(long dateTime, DataType type) {
        if (type == DATETIME) {
            return dateTimeToString(dateTime);
        }
        if (type == DATE_NANOS) {
            return nanoTimeToString(dateTime);
        }
        throw new IllegalArgumentException("Unsupported data type [" + type + "]");
    }

    public static String dateTimeToString(long dateTime) {
        return DEFAULT_DATE_TIME_FORMATTER.formatMillis(dateTime);
    }

    public static String nanoTimeToString(long dateTime) {
        return DEFAULT_DATE_NANOS_FORMATTER.formatNanos(dateTime);
    }

    public static String dateTimeToString(long dateTime, DateFormatter formatter) {
        return formatter == null ? dateTimeToString(dateTime) : formatter.formatMillis(dateTime);
    }

    public static String nanoTimeToString(long dateTime, DateFormatter formatter) {
        return formatter == null ? nanoTimeToString(dateTime) : formatter.formatNanos(dateTime);
    }

    public static BytesRef numericBooleanToString(Object field) {
        return new BytesRef(String.valueOf(field));
    }

    public static boolean stringToBoolean(String field) {
        return Boolean.parseBoolean(field);
    }

    public static int stringToInt(String field) {
        try {
            return Integer.parseInt(field);
        } catch (NumberFormatException nfe) {
            try {
                return safeToInt(stringToDouble(field));
            } catch (Exception e) {
                throw new InvalidArgumentException(nfe, "Cannot parse number [{}]", field);
            }
        }
    }

    public static long stringToLong(String field) {
        try {
            return StringUtils.parseLong(field);
        } catch (InvalidArgumentException iae) {
            try {
                return safeDoubleToLong(stringToDouble(field));
            } catch (Exception e) {
                throw new InvalidArgumentException(iae, "Cannot parse number [{}]", field);
            }
        }
    }

    public static double stringToDouble(String field) {
        return StringUtils.parseDouble(field);
    }

    public static BytesRef unsignedLongToString(long number) {
        return new BytesRef(unsignedLongAsNumber(number).toString());
    }

    public static long stringToUnsignedLong(String field) {
        return asLongUnsigned(safeToUnsignedLong(field));
    }

    public static Number stringToIntegral(String field) {
        return StringUtils.parseIntegral(field);
    }

    /**
     * The following conversion are between unsignedLong and other numeric data types.
     */
    public static double unsignedLongToDouble(long number) {
        return NumericUtils.unsignedLongAsNumber(number).doubleValue();
    }

    public static long doubleToUnsignedLong(double number) {
        return NumericUtils.asLongUnsigned(safeToUnsignedLong(number));
    }

    public static int unsignedLongToInt(long number) {
        Number n = NumericUtils.unsignedLongAsNumber(number);
        int i = n.intValue();
        if (i != n.longValue()) {
            throw new InvalidArgumentException("[{}] out of [integer] range", n);
        }
        return i;
    }

    public static long intToUnsignedLong(int number) {
        return longToUnsignedLong(number, false);
    }

    public static long unsignedLongToLong(long number) {
        return DataTypeConverter.safeToLong(unsignedLongAsNumber(number));
    }

    public static long longToUnsignedLong(long number, boolean allowNegative) {
        return allowNegative == false ? NumericUtils.asLongUnsigned(safeToUnsignedLong(number)) : NumericUtils.asLongUnsigned(number);
    }

    public static long bigIntegerToUnsignedLong(BigInteger field) {
        BigInteger unsignedLong = asUnsignedLong(field);
        return NumericUtils.asLongUnsigned(unsignedLong);
    }

    public static BigInteger unsignedLongToBigInteger(long number) {
        return NumericUtils.unsignedLongAsBigInteger(number);
    }

    public static boolean unsignedLongToBoolean(long number) {
        Number n = NumericUtils.unsignedLongAsNumber(number);
        return n instanceof BigInteger || n.longValue() != 0;
    }

    public static long booleanToUnsignedLong(boolean number) {
        return number ? ONE_AS_UNSIGNED_LONG : ZERO_AS_UNSIGNED_LONG;
    }

    public static String aggregateMetricDoubleBlockToString(CompositeBlock compositeBlock, int index) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            for (Metric metric : List.of(Metric.MIN, Metric.MAX, Metric.SUM)) {
                var block = compositeBlock.getBlock(metric.getIndex());
                if (block.isNull(index) == false) {
                    builder.field(metric.getLabel(), ((DoubleBlock) block).getDouble(index));
                }
            }
            var countBlock = compositeBlock.getBlock(Metric.COUNT.getIndex());
            if (countBlock.isNull(index) == false) {
                builder.field(Metric.COUNT.getLabel(), ((IntBlock) countBlock).getInt(index));
            }
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new IllegalStateException("error rendering aggregate metric double", e);
        }
    }

    public static String aggregateMetricDoubleLiteralToString(AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral aggMetric) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            if (aggMetric.min() != null) {
                builder.field(Metric.MIN.getLabel(), aggMetric.min());
            }
            if (aggMetric.max() != null) {
                builder.field(Metric.MAX.getLabel(), aggMetric.max());
            }
            if (aggMetric.sum() != null) {
                builder.field(Metric.SUM.getLabel(), aggMetric.sum());
            }
            if (aggMetric.count() != null) {
                builder.field(Metric.COUNT.getLabel(), aggMetric.count());
            }
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new IllegalStateException("error rendering aggregate metric double", e);
        }
    }

    public static AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral stringToAggregateMetricDoubleLiteral(String s) {
        Double min = null;
        Double max = null;
        Double sum = null;
        Integer count = null;
        String[] values = s.substring(1, s.length() - 1).split(",");
        for (String v : values) {
            var pair = v.split(":");
            String type = pair[0];
            String number = pair[1];
            switch (type) {
                case "min":
                    min = Double.parseDouble(number);
                    break;
                case "max":
                    max = Double.parseDouble(number);
                    break;
                case "sum":
                    sum = Double.parseDouble(number);
                    break;
                case "value_count":
                    count = Integer.parseInt(number);
                    break;
                default:
                    throw new IllegalArgumentException(
                        "Received a metric that wasn't min, max, sum, or value_count: " + type + " with value: " + number
                    );
            }
        }
        return new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral(min, max, sum, count);
    }

    public enum EsqlConverter implements Converter {

        STRING_TO_DATE_PERIOD(x -> EsqlDataTypeConverter.parseTemporalAmount(x, DataType.DATE_PERIOD)),
        STRING_TO_TIME_DURATION(x -> EsqlDataTypeConverter.parseTemporalAmount(x, DataType.TIME_DURATION)),
        STRING_TO_CHRONO_FIELD(EsqlDataTypeConverter::stringToChrono),
        STRING_TO_DATETIME(x -> EsqlDataTypeConverter.dateTimeToLong((String) x)),
        STRING_TO_DATE_NANOS(x -> EsqlDataTypeConverter.dateNanosToLong((String) x)),
        STRING_TO_IP(x -> EsqlDataTypeConverter.stringToIP((String) x)),
        STRING_TO_VERSION(x -> EsqlDataTypeConverter.stringToVersion((String) x)),
        STRING_TO_DOUBLE(x -> EsqlDataTypeConverter.stringToDouble((String) x)),
        STRING_TO_LONG(x -> EsqlDataTypeConverter.stringToLong((String) x)),
        STRING_TO_INT(x -> EsqlDataTypeConverter.stringToInt((String) x)),
        STRING_TO_BOOLEAN(x -> EsqlDataTypeConverter.stringToBoolean((String) x)),
        STRING_TO_SPATIAL(x -> EsqlDataTypeConverter.stringToSpatial((String) x));

        private static final String NAME = "esql-converter";
        private final Function<Object, Object> converter;

        EsqlConverter(Function<Object, Object> converter) {
            this.converter = converter;
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
            return in.readEnum(EsqlConverter.class);
        }

        @Override
        public Object convert(Object l) {
            if (l == null) {
                return null;
            }
            return converter.apply(l);
        }
    }

    public static BiFunction<Source, Expression, AbstractConvertFunction> converterFunctionFactory(DataType toType) {
        return TYPE_TO_CONVERTER_FUNCTION.get(toType);
    }
}
