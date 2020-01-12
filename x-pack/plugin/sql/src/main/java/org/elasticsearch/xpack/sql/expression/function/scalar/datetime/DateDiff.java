/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isDate;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.sql.util.DateUtils.DAY_IN_MILLIS;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;

public class DateDiff extends ThreeArgsDateTimeFunction {

    public enum Part implements DateTimeField {

        YEAR((start, end) ->  end.getYear() - start.getYear(), "years", "yyyy", "yy"),
        QUARTER((start, end) -> QuarterProcessor.quarter(end) - QuarterProcessor.quarter(start) + (YEAR.diff(start, end) * 4),
            "quarters", "qq", "q"),
        MONTH((start, end) -> safeInt(end.getLong(ChronoField.PROLEPTIC_MONTH) - start.getLong(ChronoField.PROLEPTIC_MONTH)),
            "months", "mm", "m"),
        DAYOFYEAR((start, end) -> safeInt(diffInDays(start, end)), "dy", "y"),
        DAY(DAYOFYEAR::diff, "days", "dd", "d"),
        WEEK((start, end) -> {
            long startInDays =  start.toInstant().toEpochMilli() / DAY_IN_MILLIS -
                    DatePart.Part.WEEKDAY.extract(start.withZoneSameInstant(UTC));
            long endInDays =  end.toInstant().toEpochMilli() / DAY_IN_MILLIS -
                    DatePart.Part.WEEKDAY.extract(end.withZoneSameInstant(UTC));
            return safeInt((endInDays - startInDays) / 7);
        }, "weeks", "wk", "ww"),
        WEEKDAY(DAYOFYEAR::diff,  "weekdays", "dw"),
        HOUR((start, end) -> safeInt(diffInHours(start, end)),  "hours", "hh"),
        MINUTE((start, end) -> safeInt(diffInMinutes(start, end)), "minutes", "mi", "n"),
        SECOND((start, end) -> safeInt(end.toEpochSecond() - start.toEpochSecond()), "seconds", "ss", "s"),
        MILLISECOND((start, end) -> safeInt(end.toInstant().toEpochMilli() - start.toInstant().toEpochMilli()),
            "milliseconds", "ms"),
        MICROSECOND((start, end) -> {
            long secondsDiff = diffInSeconds(start, end);
            long microsDiff = end.toInstant().getLong(ChronoField.MICRO_OF_SECOND) -
                start.toInstant().getLong(ChronoField.MICRO_OF_SECOND);
            return safeInt(secondsDiff * 1_000_000L + microsDiff);
        }, "microseconds", "mcs"),
        NANOSECOND((start, end) -> {
            long secondsDiff = diffInSeconds(start, end);
            int nanosDiff = end.getNano() - start.getNano();
            return safeInt(secondsDiff * 1_000_000_000L + nanosDiff);
        }, "nanoseconds", "ns");

        private static final Map<String, Part> NAME_TO_PART;
        private static final List<String> VALID_VALUES;

        static {
            NAME_TO_PART = DateTimeField.initializeResolutionMap(values());
            VALID_VALUES = DateTimeField.initializeValidValues(values());
        }

        private BiFunction<ZonedDateTime, ZonedDateTime, Integer> diffFunction;
        private Set<String> aliases;

        Part(BiFunction<ZonedDateTime, ZonedDateTime, Integer> diffFunction, String... aliases) {
            this.diffFunction = diffFunction;
            this.aliases = Set.of(aliases);
        }

        @Override
        public Iterable<String> aliases() {
            return aliases;
        }

        public static List<String> findSimilar(String match) {
            return DateTimeField.findSimilar(NAME_TO_PART.keySet(), match);
        }

        public static Part resolve(String dateTimeUnit) {
            return DateTimeField.resolveMatch(NAME_TO_PART, dateTimeUnit);
        }

        public Integer diff(ZonedDateTime startTimestamp, ZonedDateTime endTimestamp) {
            return diffFunction.apply(startTimestamp, endTimestamp);
        }

        private static long diffInSeconds(ZonedDateTime start, ZonedDateTime end) {
            return end.toEpochSecond() - start.toEpochSecond();
        }

        private static int safeInt(long diff) {
            if (diff > Integer.MAX_VALUE || diff < Integer.MIN_VALUE) {
                throw new SqlIllegalArgumentException("The DATE_DIFF function resulted in an overflow; the number of units " +
                    "separating two date/datetime instances is too large. Try to use DATE_DIFF with a less precise unit.");
            } else {
                return Long.valueOf(diff).intValue();
            }
        }

        private static long diffInMinutes(ZonedDateTime start, ZonedDateTime end) {
            // Truncate first to minutes (ignore any seconds and sub-seconds fields)
            return (end.toEpochSecond() / 60) - (start.toEpochSecond() / 60);
        }

        private static long diffInHours(ZonedDateTime start, ZonedDateTime end) {
            return diffInMinutes(start, end) / 60;
        }

        private static long diffInDays(ZonedDateTime start, ZonedDateTime end) {
            return diffInHours(start, end) / 24;
        }
    }

    public DateDiff(Source source, Expression unit, Expression startTimestamp, Expression endTimestamp, ZoneId zoneId) {
        super(source, unit, startTimestamp, endTimestamp, zoneId);
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

        resolution = isDate(third(), sourceText(), Expressions.ParamOrdinal.THIRD);
        if (resolution.unresolved()) {
            return resolution;
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }

    @Override
    protected ThreeArgsDateTimeFunction replaceChildren(Expression newFirst, Expression newSecond, Expression newThird) {
        return new DateDiff(source(), newFirst, newSecond, newThird, zoneId());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DateDiff::new, first(), second(), third(), zoneId());
    }

    @Override
    public Nullability nullable() {
        return Nullability.UNKNOWN;
    }

    @Override
    protected Pipe createPipe(Pipe unit, Pipe startTimestamp, Pipe endTimestamp, ZoneId zoneId) {
        return new DateDiffPipe(source(), this, unit, startTimestamp, endTimestamp, zoneId);
    }

    @Override
    protected String scriptMethodName() {
        return "dateDiff";
    }

    @Override
    public Object fold() {
        return DateDiffProcessor.process(first().fold(), second().fold(), third().fold(), zoneId());
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
    protected List<String> validDateTimeFieldValues() {
        return Part.VALID_VALUES;
    }
}
