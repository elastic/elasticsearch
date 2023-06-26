/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.expression.literal.interval.IntervalDayTime;
import org.elasticsearch.xpack.sql.expression.literal.interval.IntervalYearMonth;

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
import java.util.function.UnaryOperator;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.util.DateUtils.SECONDS_PER_DAY;
import static org.elasticsearch.xpack.ql.util.DateUtils.SECONDS_PER_HOUR;
import static org.elasticsearch.xpack.ql.util.DateUtils.SECONDS_PER_MINUTE;
import static org.elasticsearch.xpack.sql.expression.SqlTypeResolutions.isDateOrInterval;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.isInterval;

public class DateTrunc extends BinaryDateTimeDatePartFunction {

    public enum Part implements DateTimeField {

        MILLENNIUM(dt -> {
            int year = dt.getYear();
            int firstYearOfMillennium = year - (year % 1000);
            return dt.with(ChronoField.YEAR, firstYearOfMillennium)
                .with(ChronoField.MONTH_OF_YEAR, 1)
                .with(ChronoField.DAY_OF_MONTH, 1)
                .toLocalDate()
                .atStartOfDay(dt.getZone());
        }, idt -> new IntervalDayTime(Duration.ZERO, idt.dataType()), iym -> {
            Period period = iym.interval();
            int year = period.getYears();
            int firstYearOfMillennium = year - (year % 1000);
            return new IntervalYearMonth(Period.ZERO.plusYears(firstYearOfMillennium), iym.dataType());
        }, "millennia"),
        CENTURY(dt -> {
            int year = dt.getYear();
            int firstYearOfCentury = year - (year % 100);
            return dt.with(ChronoField.YEAR, firstYearOfCentury)
                .with(ChronoField.MONTH_OF_YEAR, 1)
                .with(ChronoField.DAY_OF_MONTH, 1)
                .toLocalDate()
                .atStartOfDay(dt.getZone());
        }, idt -> new IntervalDayTime(Duration.ZERO, idt.dataType()), iym -> {
            Period period = iym.interval();
            int year = period.getYears();
            int firstYearOfCentury = year - (year % 100);
            return new IntervalYearMonth(Period.ZERO.plusYears(firstYearOfCentury), iym.dataType());
        }, "centuries"),
        DECADE(dt -> {
            int year = dt.getYear();
            int firstYearOfDecade = year - (year % 10);
            return dt.with(ChronoField.YEAR, firstYearOfDecade)
                .with(ChronoField.MONTH_OF_YEAR, 1)
                .with(ChronoField.DAY_OF_MONTH, 1)
                .toLocalDate()
                .atStartOfDay(dt.getZone());
        }, idt -> new IntervalDayTime(Duration.ZERO, idt.dataType()), iym -> {
            Period period = iym.interval();
            int year = period.getYears();
            int firstYearOfDecade = year - (year % 10);
            return new IntervalYearMonth(Period.ZERO.plusYears(firstYearOfDecade), iym.dataType());
        }, "decades"),
        YEAR(dt -> {
            return dt.with(ChronoField.MONTH_OF_YEAR, 1).with(ChronoField.DAY_OF_MONTH, 1).toLocalDate().atStartOfDay(dt.getZone());
        }, idt -> new IntervalDayTime(Duration.ZERO, idt.dataType()), iym -> {
            Period period = iym.interval();
            int year = period.getYears();
            return new IntervalYearMonth(Period.ZERO.plusYears(year), iym.dataType());
        }, "years", "yy", "yyyy"),
        QUARTER(dt -> {
            int month = dt.getMonthValue();
            int firstMonthOfQuarter = (((month - 1) / 3) * 3) + 1;
            return dt.with(ChronoField.MONTH_OF_YEAR, firstMonthOfQuarter)
                .with(ChronoField.DAY_OF_MONTH, 1)
                .toLocalDate()
                .atStartOfDay(dt.getZone());
        }, idt -> new IntervalDayTime(Duration.ZERO, (idt.dataType())), iym -> {
            Period period = iym.interval();
            int month = period.getMonths();
            int year = period.getYears();
            int firstMonthOfQuarter = (month / 3) * 3;
            return new IntervalYearMonth(Period.ZERO.plusYears(year).plusMonths(firstMonthOfQuarter), iym.dataType());
        }, "quarters", "qq", "q"),
        MONTH(
            dt -> { return dt.with(ChronoField.DAY_OF_MONTH, 1).toLocalDate().atStartOfDay(dt.getZone()); },
            idt -> new IntervalDayTime(Duration.ZERO, idt.dataType()),
            iym -> iym,
            "months",
            "mm",
            "m"
        ),
        WEEK(dt -> {
            return dt.with(ChronoField.DAY_OF_WEEK, 1).toLocalDate().atStartOfDay(dt.getZone());
        }, idt -> new IntervalDayTime(Duration.ZERO, idt.dataType()), iym -> iym, "weeks", "wk", "ww"),
        DAY(
            dt -> dt.toLocalDate().atStartOfDay(dt.getZone()),
            idt -> truncateIntervalSmallerThanWeek(idt, ChronoUnit.DAYS),
            iym -> iym,
            "days",
            "dd",
            "d"
        ),
        HOUR(dt -> {
            int hour = dt.getHour();
            return dt.toLocalDate().atStartOfDay(dt.getZone()).with(ChronoField.HOUR_OF_DAY, hour);
        }, idt -> truncateIntervalSmallerThanWeek(idt, ChronoUnit.HOURS), iym -> iym, "hours", "hh"),
        MINUTE(dt -> {
            int hour = dt.getHour();
            int minute = dt.getMinute();
            return dt.toLocalDate().atStartOfDay(dt.getZone()).with(ChronoField.HOUR_OF_DAY, hour).with(ChronoField.MINUTE_OF_HOUR, minute);
        }, idt -> truncateIntervalSmallerThanWeek(idt, ChronoUnit.MINUTES), iym -> iym, "minutes", "mi", "n"),
        SECOND(
            dt -> dt.with(ChronoField.NANO_OF_SECOND, 0),
            idt -> truncateIntervalSmallerThanWeek(idt, ChronoUnit.SECONDS),
            iym -> iym,
            "seconds",
            "ss",
            "s"
        ),
        MILLISECOND(dt -> {
            int micros = dt.get(ChronoField.MICRO_OF_SECOND);
            return dt.with(ChronoField.MILLI_OF_SECOND, (micros / 1000));
        }, idt -> truncateIntervalSmallerThanWeek(idt, ChronoUnit.MILLIS), iym -> iym, "milliseconds", "ms"),
        MICROSECOND(dt -> {
            int nanos = dt.getNano();
            return dt.with(ChronoField.MICRO_OF_SECOND, (nanos / 1000));
        }, idt -> idt, iym -> iym, "microseconds", "mcs"),
        NANOSECOND(dt -> dt, idt -> idt, iym -> iym, "nanoseconds", "ns");

        private static final Map<String, Part> NAME_TO_PART;
        private static final List<String> VALID_VALUES;

        static {
            NAME_TO_PART = DateTimeField.initializeResolutionMap(values());
            VALID_VALUES = DateTimeField.initializeValidValues(values());
        }

        private UnaryOperator<IntervalYearMonth> truncateFunctionIntervalYearMonth;
        private UnaryOperator<ZonedDateTime> truncateFunctionZonedDateTime;
        private UnaryOperator<IntervalDayTime> truncateFunctionIntervalDayTime;
        private Set<String> aliases;

        Part(
            UnaryOperator<ZonedDateTime> truncateFunctionZonedDateTime,
            UnaryOperator<IntervalDayTime> truncateFunctionIntervalDayTime,
            UnaryOperator<IntervalYearMonth> truncateFunctionIntervalYearMonth,
            String... aliases
        ) {
            this.truncateFunctionIntervalYearMonth = truncateFunctionIntervalYearMonth;
            this.truncateFunctionZonedDateTime = truncateFunctionZonedDateTime;
            this.truncateFunctionIntervalDayTime = truncateFunctionIntervalDayTime;
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

        public ZonedDateTime truncate(ZonedDateTime dateTime) {
            return truncateFunctionZonedDateTime.apply(dateTime);
        }

        public IntervalDayTime truncate(IntervalDayTime dateTime) {
            return truncateFunctionIntervalDayTime.apply(dateTime);
        }

        public IntervalYearMonth truncate(IntervalYearMonth dateTime) {
            return truncateFunctionIntervalYearMonth.apply(dateTime);
        }

        private static IntervalDayTime truncateIntervalSmallerThanWeek(IntervalDayTime r, ChronoUnit unit) {
            Duration d = r.interval();
            int isNegative = 1;
            if (d.isNegative()) {
                d = d.negated();
                isNegative = -1;
            }
            long durationInSec = d.getSeconds();
            long day = durationInSec / SECONDS_PER_DAY;
            durationInSec = durationInSec % SECONDS_PER_DAY;
            long hour = durationInSec / SECONDS_PER_HOUR;
            durationInSec = durationInSec % SECONDS_PER_HOUR;
            long min = durationInSec / SECONDS_PER_MINUTE;
            durationInSec = durationInSec % SECONDS_PER_MINUTE;
            long sec = durationInSec;
            long miliseccond = TimeUnit.NANOSECONDS.toMillis(d.getNano());
            Duration newDuration = Duration.ZERO;
            if (unit.ordinal() <= ChronoUnit.DAYS.ordinal()) {
                newDuration = newDuration.plusDays(day * isNegative);
            }
            if (unit.ordinal() <= ChronoUnit.HOURS.ordinal()) {
                newDuration = newDuration.plusHours(hour * isNegative);
            }
            if (unit.ordinal() <= ChronoUnit.MINUTES.ordinal()) {
                newDuration = newDuration.plusMinutes(min * isNegative);
            }
            if (unit.ordinal() <= ChronoUnit.SECONDS.ordinal()) {
                newDuration = newDuration.plusSeconds(sec * isNegative);
            }
            if (unit.ordinal() <= ChronoUnit.MILLIS.ordinal()) {
                newDuration = newDuration.plusMillis(miliseccond * isNegative);
            }
            return new IntervalDayTime(newDuration, r.dataType());
        }
    }

    public DateTrunc(Source source, Expression truncateTo, Expression timestamp, ZoneId zoneId) {
        super(source, truncateTo, timestamp, zoneId);
    }

    @Override
    public DataType dataType() {
        if (isInterval(right().dataType())) {
            return right().dataType();
        }
        return DataTypes.DATETIME;
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = super.resolveType();
        if (resolution.unresolved()) {
            return resolution;
        }
        resolution = isDateOrInterval(right(), sourceText(), SECOND);
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
    protected String scriptMethodName() {
        return "dateTrunc";
    }

    @Override
    public Object fold() {
        return DateTruncProcessor.process(left().fold(), right().fold(), zoneId());
    }

    @Override
    protected Pipe createPipe(Pipe truncateTo, Pipe timestamp, ZoneId zoneId) {
        return new DateTruncPipe(source(), this, truncateTo, timestamp, zoneId);
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
