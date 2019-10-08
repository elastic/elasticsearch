/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import java.time.Duration;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public class DateTrunc extends BinaryDateTimeFunction {

    private static final long SECONDS_PER_MINUTE = 60;
    private static final long SECONDS_PER_HOUR = SECONDS_PER_MINUTE * 60;
    private static final long SECONDS_PER_DAY = SECONDS_PER_HOUR * 24;

    public enum Part implements DateTimeField {

        MILLENNIUM((Object r) -> {
            if(r instanceof ZonedDateTime){
                ZonedDateTime dt = (ZonedDateTime) r;
                int year = dt.getYear();
                int firstYearOfMillennium = year - (year % 1000);
                return dt
                    .with(ChronoField.YEAR, firstYearOfMillennium)
                    .with(ChronoField.MONTH_OF_YEAR, 1)
                    .with(ChronoField.DAY_OF_MONTH, 1)
                    .toLocalDate().atStartOfDay(dt.getZone());
            } else if(r instanceof IntervalDayTime){
                return new IntervalDayTime(Duration.ZERO,((IntervalDayTime) r).dataType());
            } else {
                Period period = ((IntervalYearMonth) r).interval();
                int year = period.getYears();
                int firstYearOfMillennium = year - (year % 1000);
                return new IntervalYearMonth(Period.ZERO.plusYears(firstYearOfMillennium),((IntervalYearMonth) r).dataType());
            }
        },"millennia"),
        CENTURY((Object r) -> {
            if(r instanceof ZonedDateTime){
                ZonedDateTime dt = (ZonedDateTime) r;
                int year = dt.getYear();
                int firstYearOfCentury = year - (year % 100);
                return dt
                    .with(ChronoField.YEAR, firstYearOfCentury)
                    .with(ChronoField.MONTH_OF_YEAR, 1)
                    .with(ChronoField.DAY_OF_MONTH, 1)
                    .toLocalDate().atStartOfDay(dt.getZone());
            } else if(r instanceof IntervalDayTime){
                return new IntervalDayTime(Duration.ZERO,((IntervalDayTime) r).dataType());
            } else {
                Period period = ((IntervalYearMonth) r).interval();
                int year = period.getYears();
                int firstYearOfCentury = year - (year % 100);
                return new IntervalYearMonth(Period.ZERO.plusYears(firstYearOfCentury),((IntervalYearMonth) r).dataType());
            }
        }, "centuries"),
        DECADE((Object r) -> {
            if(r instanceof ZonedDateTime){
                ZonedDateTime dt = (ZonedDateTime) r;
                int year = dt.getYear();
                int firstYearOfDecade = year - (year % 10);
                return dt
                    .with(ChronoField.YEAR, firstYearOfDecade)
                    .with(ChronoField.MONTH_OF_YEAR, 1)
                    .with(ChronoField.DAY_OF_MONTH, 1)
                    .toLocalDate().atStartOfDay(dt.getZone());
            } else if(r instanceof IntervalDayTime){
                return new IntervalDayTime(Duration.ZERO,((IntervalDayTime) r).dataType());
            } else {
                Period period = ((IntervalYearMonth) r).interval();
                int year = period.getYears();
                int firstYearOfDecade = year - (year % 10);
                return new IntervalYearMonth(Period.ZERO.plusYears(firstYearOfDecade),((IntervalYearMonth) r).dataType());
            }
        }, "decades"),
        YEAR((Object r) -> {
            if(r instanceof ZonedDateTime){
                ZonedDateTime dt = (ZonedDateTime) r;
                return dt.with(ChronoField.MONTH_OF_YEAR, 1)
                    .with(ChronoField.DAY_OF_MONTH, 1)
                    .toLocalDate().atStartOfDay(dt.getZone());
            } else if(r instanceof IntervalDayTime){
                return new IntervalDayTime(Duration.ZERO,((IntervalDayTime) r).dataType());
            } else {
                Period period = ((IntervalYearMonth) r).interval();
                int year = period.getYears();
                return new IntervalYearMonth(Period.ZERO.plusYears(year),((IntervalYearMonth) r).dataType());
            }
        },"years", "yy", "yyyy"),
        QUARTER((Object r) -> {
            if(r instanceof ZonedDateTime){
                ZonedDateTime dt = (ZonedDateTime) r;
                int month = dt.getMonthValue();
                int firstMonthOfQuarter = (((month - 1) / 3) * 3) + 1;
                return dt
                    .with(ChronoField.MONTH_OF_YEAR, firstMonthOfQuarter)
                    .with(ChronoField.DAY_OF_MONTH, 1)
                    .toLocalDate().atStartOfDay(dt.getZone());
            } else if(r instanceof IntervalDayTime){
                return new IntervalDayTime(Duration.ZERO,((IntervalDayTime) r).dataType());
            } else {
                Period period = ((IntervalYearMonth) r).interval();
                int month = period.getMonths();
                int year = period.getYears();
                int firstMonthOfQuarter = (month / 3) * 3;
                return new IntervalYearMonth(Period.ZERO.plusYears(year).plusMonths(firstMonthOfQuarter),((IntervalYearMonth) r).dataType());
            }
        }, "quarters", "qq", "q"),
        MONTH((Object r) -> {
            if(r instanceof ZonedDateTime){
                ZonedDateTime dt = (ZonedDateTime) r;
                return dt.with(ChronoField.DAY_OF_MONTH, 1)
                    .toLocalDate().atStartOfDay(dt.getZone());
            } else if(r instanceof IntervalDayTime){
                return new IntervalDayTime(Duration.ZERO,((IntervalDayTime) r).dataType());
            } else {
                return r;
            }
        },"months", "mm", "m"),
        WEEK((Object r) -> {
            if (r instanceof ZonedDateTime) {
                ZonedDateTime dt = (ZonedDateTime) r;
                return dt.with(ChronoField.DAY_OF_WEEK, 1)
                    .toLocalDate().atStartOfDay(dt.getZone());
            } else if (r instanceof IntervalDayTime) {
                return new IntervalDayTime(Duration.ZERO, ((IntervalDayTime) r).dataType());
            } else {
                return r;
            }
        }, "weeks", "wk", "ww"),
        DAY((Object r) -> {
            if (r instanceof ZonedDateTime) {
                ZonedDateTime dt = (ZonedDateTime) r;
                return dt.toLocalDate().atStartOfDay(dt.getZone());
            }
            return truncateIntervalSmallerThanWeek(r,ChronoUnit.DAYS);
        }, "days", "dd", "d"),
        HOUR((Object r) -> {
            if (r instanceof ZonedDateTime) {
                ZonedDateTime dt = (ZonedDateTime) r;
                int hour = dt.getHour();
                return dt.toLocalDate().atStartOfDay(dt.getZone())
                    .with(ChronoField.HOUR_OF_DAY, hour);
            }
            return truncateIntervalSmallerThanWeek(r,ChronoUnit.HOURS);
        }, "hours", "hh"),
        MINUTE((Object r) -> {
            if (r instanceof ZonedDateTime) {
                ZonedDateTime dt = (ZonedDateTime) r;
                int hour = dt.getHour();
                int minute = dt.getMinute();
                return dt.toLocalDate().atStartOfDay(dt.getZone())
                    .with(ChronoField.HOUR_OF_DAY, hour)
                    .with(ChronoField.MINUTE_OF_HOUR, minute);
            }
            return truncateIntervalSmallerThanWeek(r,ChronoUnit.MINUTES);
        }, "minutes", "mi", "n"),
        SECOND((Object r) -> {
            if (r instanceof ZonedDateTime) {
                ZonedDateTime dt = (ZonedDateTime) r;
                return dt.with(ChronoField.NANO_OF_SECOND, 0);
            }
            return truncateIntervalSmallerThanWeek(r,ChronoUnit.SECONDS);
        }, "seconds", "ss", "s"),
        MILLISECOND((Object r) -> {
            if (r instanceof ZonedDateTime) {
                ZonedDateTime dt = (ZonedDateTime) r;
                int micros = dt.get(ChronoField.MICRO_OF_SECOND);
                return dt.with(ChronoField.MILLI_OF_SECOND, (micros / 1000));
            }
            return truncateIntervalSmallerThanWeek(r,ChronoUnit.MILLIS);
        }, "milliseconds", "ms"),
        MICROSECOND((Object r) -> {
            if (r instanceof ZonedDateTime) {
                ZonedDateTime dt = (ZonedDateTime) r;
                int nanos = dt.getNano();
                return dt.with(ChronoField.MICRO_OF_SECOND, (nanos / 1000));
            }
            return r;
        }, "microseconds", "mcs"),
        NANOSECOND(dt -> dt, "nanoseconds", "ns");


        private static final Map<String, Part> NAME_TO_PART;
        private static final List<String> VALID_VALUES;

        static {
            NAME_TO_PART = DateTimeField.initializeResolutionMap(values());
            VALID_VALUES = DateTimeField.initializeValidValues(values());
        }

        private Function<Object, Object> truncateFunction;
        private Set<String> aliases;

        Part(Function<Object, Object> truncateFunction, String... aliases) {
            this.truncateFunction = truncateFunction;
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

        public Object truncate(Object dateTime) {
            return truncateFunction.apply(dateTime);
        }

        private static Object truncateIntervalSmallerThanWeek(Object r, ChronoUnit unit){
            if (r instanceof IntervalDayTime) {
                Duration d = ((IntervalDayTime) r).interval();
                int isNegative = 1;
                if (d.isNegative()) {
                    d = d.negated();
                    isNegative = -1;
                }
                long durationInSec = d.getSeconds();
                long day = durationInSec / SECONDS_PER_DAY;
                System.out.println(day);
                durationInSec = durationInSec % SECONDS_PER_DAY;
                long hour = durationInSec / SECONDS_PER_HOUR;
                durationInSec = durationInSec % SECONDS_PER_HOUR;
                long min = durationInSec / SECONDS_PER_MINUTE;
                durationInSec = durationInSec % SECONDS_PER_MINUTE;
                long sec = durationInSec;
                long miliseccond = TimeUnit.NANOSECONDS.toMillis(d.getNano());
                Duration newDuration = Duration.ZERO;
                if(unit.ordinal() <= ChronoUnit.DAYS.ordinal())
                    newDuration = newDuration.plusDays(day*isNegative);
                if(unit.ordinal() <= ChronoUnit.HOURS.ordinal())
                    newDuration = newDuration.plusHours(hour*isNegative);
                if(unit.ordinal() <= ChronoUnit.MINUTES.ordinal())
                    newDuration = newDuration.plusMinutes(min*isNegative);
                if(unit.ordinal() <= ChronoUnit.SECONDS.ordinal())
                    newDuration = newDuration.plusSeconds(sec*isNegative);
                if(unit.ordinal() <= ChronoUnit.MILLIS.ordinal())
                    newDuration = newDuration.plusMillis(miliseccond*isNegative);
                return new IntervalDayTime(newDuration, ((IntervalDayTime) r).dataType());
            } else {
                return r;
            }
        }
    }

    public DateTrunc(Source source, Expression truncateTo, Expression timestamp, ZoneId zoneId) {
        super(source, truncateTo, timestamp, zoneId, TRUNC);
    }

    @Override
    public DataType dataType() {
        return right().dataType();
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = isString(left(), sourceText(), Expressions.ParamOrdinal.FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        if (left().foldable()) {
            String datePartValue = (String) left().fold();
            if (datePartValue != null && resolveDateTimeField(datePartValue) == false) {
                List<String> similar = findSimilarDateTimeFields(datePartValue);
                if (similar.isEmpty()) {
                    return new TypeResolution(format(null, "first argument of [{}] must be one of {} or their aliases, found value [{}]",
                        sourceText(),
                        validDateTimeFieldValues(),
                        Expressions.name(left())));
                } else {
                    return new TypeResolution(format(null, "Unknown value [{}] for first argument of [{}]; did you mean {}?",
                        Expressions.name(left()),
                        sourceText(),
                        similar));
                }
            }
        }
        resolution = isDateOrInterval(right(), sourceText(), Expressions.ParamOrdinal.SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression newTruncateTo, Expression newTimestamp) {
        return new DateTrunc(source(), newTruncateTo, newTimestamp, zoneId());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DateTrunc::new, left(), right(), zoneId());
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
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
        return "dateTrunc";
    }

    @Override
    public Object fold() {
        return DateTruncProcessor.process(left().fold(), right().fold(), zoneId());
    }

    @Override
    protected List<String> validDateTimeFieldValues() {
        return Part.VALID_VALUES;
    }
}
