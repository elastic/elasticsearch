/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.time.IsoLocale;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.Nullability;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.sql.expression.TypeResolutions.isDate;
import static org.elasticsearch.xpack.sql.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NonIsoDateTimeProcessor.NonIsoDateTimeExtractor;

public class DatePart extends ThreeArgsDateTimeFunction {

    public enum Part implements DateTimeField {
        YEAR((dt, sw) -> DateTimeExtractor.YEAR.extract(dt), "years", "yyyy", "yy"),
        QUARTER((dt, sw) -> QuarterProcessor.quarter(dt), "quarters", "qq", "q"),
        MONTH((dt, sw) -> DateTimeExtractor.MONTH_OF_YEAR.extract(dt), "months", "mm", "m"),
        DAYOFYEAR((dt, sw) -> DateTimeExtractor.DAY_OF_YEAR.extract(dt), "dy", "y"),
        DAY((dt, sw) -> DateTimeExtractor.DAY_OF_MONTH.extract(dt), "days", "dd", "d"),
        WEEK((dt, sw) -> {
            if (sw == StartOfWeek.MONDAY) {
                return DateTimeExtractor.ISO_WEEK_OF_YEAR.extract(dt);
            } else {
                return NonIsoDateTimeExtractor.WEEK_OF_YEAR.extract(dt);
            }
        }, "weeks", "wk", "ww"),
        WEEKDAY((dt, sw) -> {
            if (sw == StartOfWeek.MONDAY) {
                return DateTimeExtractor.ISO_DAY_OF_WEEK.extract(dt);
            } else {
                return NonIsoDateTimeExtractor.DAY_OF_WEEK.extract(dt);
            }
        }, "weekdays", "dw"),
        HOUR((dt, sw) -> DateTimeExtractor.HOUR_OF_DAY.extract(dt), "hours", "hh"),
        MINUTE((dt, sw) -> DateTimeExtractor.MINUTE_OF_HOUR.extract(dt), "minutes", "mi", "n"),
        SECOND((dt, sw) -> DateTimeExtractor.SECOND_OF_MINUTE.extract(dt), "seconds", "ss", "s"),
        MILLISECOND((dt, sw) -> dt.get(ChronoField.MILLI_OF_SECOND), "milliseconds", "ms"),
        MICROSECOND((dt, sw) -> dt.get(ChronoField.MICRO_OF_SECOND), "microseconds", "mcs"),
        NANOSECOND((dt, sw) -> dt.getNano(), "nanoseconds", "ns"),
        TZOFFSET((dt, sw) -> dt.getOffset().getTotalSeconds() / 60, "tz");

        private static final Map<String, Part> NAME_TO_PART;
        private static final List<String> VALID_VALUES;

        static {
            NAME_TO_PART = DateTimeField.initializeResolutionMap(values());
            VALID_VALUES = DateTimeField.initializeValidValues(values());
        }

        private BiFunction<ZonedDateTime, StartOfWeek, Integer> extractFunction;
        private Set<String> aliases;

        Part(BiFunction<ZonedDateTime, StartOfWeek, Integer> extractFunction, String... aliases) {
            this.extractFunction = extractFunction;
            this.aliases = Set.of(aliases);
        }

        @Override
        public Iterable<String> aliases() {
            return aliases;
        }

        public static List<String> findSimilar(String match) {
            return DateTimeField.findSimilar(NAME_TO_PART.keySet(), match);
        }

        public static Part resolve(String truncateTo) {
            return DateTimeField.resolveMatch(NAME_TO_PART, truncateTo);
        }

        public Integer extract(ZonedDateTime dateTime, StartOfWeek startOfWeek) {
            return extractFunction.apply(dateTime, startOfWeek);
        }
    }

    enum StartOfWeek {
        SUNDAY,
        MONDAY;

        public static StartOfWeek resolve(String match) {
            try {
                return valueOf(match.toUpperCase(IsoLocale.ROOT));
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    public DatePart(Source source, Expression truncateTo, Expression timestamp, Expression startOfWeek, ZoneId zoneId) {
        super(source, truncateTo, timestamp, startOfWeek == null ? Literal.of(Source.EMPTY, "Sunday") : startOfWeek, zoneId);
    }

    // Used by DatePartProcessorTests
    DatePart(Source source, Expression truncateTo, Expression timestamp, ZoneId zoneId) {
        this(source, truncateTo, timestamp, null, zoneId);
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = isString(first(), sourceText(), Expressions.ParamOrdinal.FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        if (first().foldable()) {
            String datePartValue = (String) first().fold();
            if (datePartValue != null && resolveDateTimeField(datePartValue) == false) {
                List<String> similar = findSimilarDateTimeFields(datePartValue);
                if (similar.isEmpty()) {
                    return new TypeResolution(format(null, "first argument of [{}] must be one of {} or their aliases; found value [{}]",
                        sourceText(),
                        validDateTimeFieldValues(),
                        Expressions.name(first())));
                } else {
                    return new TypeResolution(format(null, "Unknown value [{}] for first argument of [{}]; did you mean {}?",
                        Expressions.name(first()),
                        sourceText(),
                        similar));
                }
            }
        }
        resolution = isDate(second(), sourceText(), Expressions.ParamOrdinal.SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isString(third(), sourceText(), Expressions.ParamOrdinal.THIRD);
        if (resolution.unresolved()) {
            return resolution;
        }
        if (third().foldable()) {
            String startOfWeekStr = (String) third().fold();
            if (StartOfWeek.resolve(startOfWeekStr) == null) {
                return new TypeResolution(format(null, "third argument of [{}] must be one of {}; found value [{}]",
                    sourceText(),
                    StartOfWeek.values(),
                    Expressions.name(third())));
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }

    @Override
    protected ThreeArgsDateTimeFunction replaceChildren(Expression newFirst, Expression newSecond, Expression newThird) {
        return new DatePart(source(), newFirst, newSecond, newThird, zoneId());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DatePart::new, first(), second(), third(), zoneId());
    }

    @Override
    public Nullability nullable() {
        return Nullability.UNKNOWN;
    }

    @Override
    protected boolean resolveDateTimeField(String dateTimeField) {
        return Part.resolve(dateTimeField) != null;
    }

    @Override
    protected List<String> findSimilarDateTimeFields(String dateTimeField) {
        return Part.findSimilar(dateTimeField);
    }

    @Override
    protected String scriptMethodName() {
        return "datePart";
    }

    @Override
    public Object fold() {
        return DatePartProcessor.process(first().fold(), second().fold(), third().fold(), zoneId());
    }

    @Override
    protected List<String> validDateTimeFieldValues() {
        return Part.VALID_VALUES;
    }
}
