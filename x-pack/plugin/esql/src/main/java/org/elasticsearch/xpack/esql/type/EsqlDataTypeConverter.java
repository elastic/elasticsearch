/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.ql.InvalidArgumentException;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Foldables;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.Converter;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypeConverter;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAmount;
import java.util.Locale;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.type.DataTypeConverter.safeToInt;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.safeToLong;
import static org.elasticsearch.xpack.ql.type.DataTypes.NULL;
import static org.elasticsearch.xpack.ql.type.DataTypes.isPrimitive;
import static org.elasticsearch.xpack.ql.type.DataTypes.isString;
import static org.elasticsearch.xpack.ql.util.StringUtils.parseIP;

public class EsqlDataTypeConverter {

    public static final DateFormatter ESQL_DEFAULT_DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_optional_time");

    public static final DateFormatter ESQL_HOUR_MINUTE_SECOND = DateFormatter.forPattern("strict_hour_minute_second_fraction");

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

    public static Converter converterFor(DataType from, DataType to) {
        // TODO move EXPRESSION_TO_LONG here if there is no regression
        Converter converter = DataTypeConverter.converterFor(from, to);
        if (converter != null) {
            return converter;
        }
        if (isString(from) && to == EsqlDataTypes.TIME_DURATION) {
            return EsqlConverter.STRING_TO_TIME_DURATION;
        }
        if (isString(from) && to == EsqlDataTypes.DATE_PERIOD) {
            return EsqlConverter.STRING_TO_DATE_PERIOD;
        }
        return null;
    }

    public static TemporalAmount parseTemporalAmount(Object val, DataType expectedType) {
        String errorMessage = "Cannot parse [{}] to {}";
        String str = String.valueOf(val);
        if (str == null) {
            return null;
        }
        StringBuilder value = new StringBuilder();
        StringBuilder qualifier = new StringBuilder();
        StringBuilder nextBuffer = value;
        boolean lastWasSpace = false;
        for (char c : str.trim().toCharArray()) {
            if (c == ' ') {
                if (lastWasSpace == false) {
                    nextBuffer = nextBuffer == value ? qualifier : null;
                }
                lastWasSpace = true;
                continue;
            }
            if (nextBuffer == null) {
                throw new ParsingException(Source.EMPTY, errorMessage, val, expectedType);
            }
            nextBuffer.append(c);
            lastWasSpace = false;
        }

        if ((value.isEmpty() || qualifier.isEmpty()) == false) {
            try {
                TemporalAmount result = parseTemporalAmout(Integer.parseInt(value.toString()), qualifier.toString(), Source.EMPTY);
                if (EsqlDataTypes.DATE_PERIOD == expectedType && result instanceof Period
                    || EsqlDataTypes.TIME_DURATION == expectedType && result instanceof Duration) {
                    return result;
                }
                if (result instanceof Period && expectedType == EsqlDataTypes.TIME_DURATION) {
                    errorMessage += ", did you mean " + EsqlDataTypes.DATE_PERIOD + "?";
                }
                if (result instanceof Duration && expectedType == EsqlDataTypes.DATE_PERIOD) {
                    errorMessage += ", did you mean " + EsqlDataTypes.TIME_DURATION + "?";
                }
            } catch (NumberFormatException ex) {
                // wrong pattern
            }
        }

        throw new ParsingException(Source.EMPTY, errorMessage, val, expectedType);
    }

    /**
     * Converts arbitrary object to the desired data type.
     * <p>
     * Throws QlIllegalArgumentException if such conversion is not possible
     */
    public static Object convert(Object value, DataType dataType) {
        DataType detectedType = EsqlDataTypes.fromJava(value);
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

    public static DataType commonType(DataType left, DataType right) {
        return DataTypeConverter.commonType(left, right);
    }

    public static TemporalAmount parseTemporalAmout(Number value, String qualifier, Source source) throws InvalidArgumentException,
        ArithmeticException, ParsingException {
        return switch (qualifier) {
            case "millisecond", "milliseconds" -> Duration.ofMillis(safeToLong(value));
            case "second", "seconds" -> Duration.ofSeconds(safeToLong(value));
            case "minute", "minutes" -> Duration.ofMinutes(safeToLong(value));
            case "hour", "hours" -> Duration.ofHours(safeToLong(value));

            case "day", "days" -> Period.ofDays(safeToInt(safeToLong(value)));
            case "week", "weeks" -> Period.ofWeeks(safeToInt(safeToLong(value)));
            case "month", "months" -> Period.ofMonths(safeToInt(safeToLong(value)));
            case "year", "years" -> Period.ofYears(safeToInt(safeToLong(value)));

            default -> throw new ParsingException(source, "Unexpected time interval qualifier: '{}'", qualifier);
        };
    }

    /**
     * Converts a string or date expression to long.
     */
    private static Long foldExpressionToLong(Expression exp) {
        Object value = Foldables.valueOf(exp);
        if (DataTypes.isDateTime(exp.dataType())) {
            return ((Number) value).longValue();
        } else if (DataTypes.isString(exp.dataType())) {
            return convertDatetimeStringToLong(((BytesRef) value).utf8ToString(), ESQL_DEFAULT_DATE_TIME_FORMATTER);
        }
        throw new IllegalArgumentException("unsupported type [" + exp.dataType() + "]");
    }

    /**
     * Converts a string expression to ChronoField.
     */
    private static ChronoField foldExpressionToChronoField(Expression exp) {
        Object value = Foldables.valueOf(exp);
        ChronoField chronoField = null;
        if (exp.dataType() == DataTypes.KEYWORD) {
            try {
                BytesRef br = BytesRefs.toBytesRef(value);
                chronoField = ChronoField.valueOf(br.utf8ToString().toUpperCase(Locale.ROOT));
            } catch (Exception e) {
                return null;
            }
        }
        return chronoField;
    }

    private static BytesRef convertStringToIP(BytesRef value) {
        return parseIP(value.utf8ToString());
    }

    /**
     * Returns Datetime in long according to a ChronoField provided.
     */
    public static long extractLongChronoField(long value, Object chronoField, ZoneId zone) {
        ChronoField chrono = null;
        if (chronoField instanceof BytesRef cf) {
            chrono = ChronoField.valueOf(cf.utf8ToString().toUpperCase(Locale.ROOT));
        } else if (chronoField instanceof ChronoField cf) {
            chrono = cf;
        } else {
            throw new IllegalArgumentException("unsupported type [" + chrono.getClass() + "]");
        }
        return Instant.ofEpochMilli(value).atZone(zone).getLong(chrono);
    }

    public static long convertDatetimeStringToLong(String value, DateFormatter formatter) {
        formatter = formatter == null ? ESQL_DEFAULT_DATE_TIME_FORMATTER : formatter;
        return formatter.parseMillis(value);
    }

    public static String convertDatetimeLongToString(long value, DateFormatter formatter) {
        return formatter.formatMillis(value);
    }

    public enum EsqlConverter implements Converter {

        STRING_TO_DATE_PERIOD(x -> EsqlDataTypeConverter.parseTemporalAmount(x, EsqlDataTypes.DATE_PERIOD)),
        STRING_TO_TIME_DURATION(x -> EsqlDataTypeConverter.parseTemporalAmount(x, EsqlDataTypes.TIME_DURATION)),
        STRING_DATETIME_TO_LONG(x -> EsqlDataTypeConverter.foldExpressionToLong((Expression) x)),
        STRING_TO_CHRONO_FIELD(x -> EsqlDataTypeConverter.foldExpressionToChronoField((Expression) x)),
        STRING_TO_IP(x -> EsqlDataTypeConverter.convertStringToIP((BytesRef) x));

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
}
