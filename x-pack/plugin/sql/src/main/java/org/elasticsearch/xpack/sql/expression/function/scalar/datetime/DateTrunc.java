/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

public class DateTrunc extends BinaryDateTimeFunction {

    public enum Part implements DateTimeField {

        MILLENNIUM(dt -> {
            int year = dt.getYear();
            int firstYearOfMillenium = year - (year % 1000);
            return dt
                .with(ChronoField.YEAR, firstYearOfMillenium)
                .with(ChronoField.MONTH_OF_YEAR, 1)
                .with(ChronoField.DAY_OF_MONTH, 1)
                .toLocalDate().atStartOfDay(dt.getZone());
        },"millennia"),
        CENTURY(dt -> {
            int year = dt.getYear();
            int firstYearOfCentury = year - (year % 100);
            return dt
                .with(ChronoField.YEAR, firstYearOfCentury)
                .with(ChronoField.MONTH_OF_YEAR, 1)
                .with(ChronoField.DAY_OF_MONTH, 1)
                .toLocalDate().atStartOfDay(dt.getZone());
        }, "centuries"),
        DECADE(dt -> {
            int year = dt.getYear();
            int firstYearOfDecade = year - (year % 10);
            return dt
                .with(ChronoField.YEAR, firstYearOfDecade)
                .with(ChronoField.MONTH_OF_YEAR, 1)
                .with(ChronoField.DAY_OF_MONTH, 1)
                .toLocalDate().atStartOfDay(dt.getZone());
        }, "decades"),
        YEAR(dt -> dt
            .with(ChronoField.MONTH_OF_YEAR, 1)
            .with(ChronoField.DAY_OF_MONTH, 1)
            .toLocalDate().atStartOfDay(dt.getZone()),
            "years", "yy", "yyyy"),
        QUARTER(dt -> {
            int month = dt.getMonthValue();
            int firstMonthOfQuarter = (((month - 1) / 3) * 3) + 1;
            return dt
                .with(ChronoField.MONTH_OF_YEAR, firstMonthOfQuarter)
                .with(ChronoField.DAY_OF_MONTH, 1)
                .toLocalDate().atStartOfDay(dt.getZone());
        }, "quarters", "qq", "q"),
        MONTH(dt -> dt
            .with(ChronoField.DAY_OF_MONTH, 1)
            .toLocalDate().atStartOfDay(dt.getZone()),
            "months", "mm", "m"),
        WEEK(dt -> dt
            .with(ChronoField.DAY_OF_WEEK, 1)
            .toLocalDate().atStartOfDay(dt.getZone()),
            "weeks", "wk", "ww"),
        DAY(dt -> dt.toLocalDate().atStartOfDay(dt.getZone()), "days", "dd", "d"),
        HOUR(dt -> {
            int hour = dt.getHour();
            return dt.toLocalDate().atStartOfDay(dt.getZone())
                .with(ChronoField.HOUR_OF_DAY, hour);
        }, "hours", "hh"),
        MINUTE(dt -> {
            int hour = dt.getHour();
            int minute = dt.getMinute();
            return dt.toLocalDate().atStartOfDay(dt.getZone())
                .with(ChronoField.HOUR_OF_DAY, hour)
                .with(ChronoField.MINUTE_OF_HOUR, minute);
        }, "minutes", "mi", "n"),
        SECOND(dt -> dt.with(ChronoField.NANO_OF_SECOND, 0), "seconds", "ss", "s"),
        MILLISECOND(dt -> {
            int micros = dt.get(ChronoField.MICRO_OF_SECOND);
            return dt.with(ChronoField.MILLI_OF_SECOND, (micros / 1000));
        }, "milliseconds", "ms"),
        MICROSECOND(dt -> {
            int nanos = dt.getNano();
            return dt.with(ChronoField.MICRO_OF_SECOND, (nanos / 1000));
        }, "microseconds", "mcs"),
        NANOSECOND(dt -> dt, "nanoseconds", "ns");

        private static final Map<String, Part> NAME_TO_PART;
        private static final List<String> VALID_VALUES;

        static {
            NAME_TO_PART = DateTimeField.initializeResolutionMap(values());
            VALID_VALUES = DateTimeField.initializeValidValues(values());
        }

        private UnaryOperator<ZonedDateTime> truncateFunction;
        private Set<String> aliases;

        Part(UnaryOperator<ZonedDateTime> truncateFunction, String... aliases) {
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

        public ZonedDateTime truncate(ZonedDateTime dateTime) {
            return truncateFunction.apply(dateTime);
        }
    }

    public DateTrunc(Source source, Expression truncateTo, Expression timestamp, ZoneId zoneId) {
        super(source, truncateTo, timestamp, zoneId);
    }

    @Override
    public DataType dataType() {
        return DataType.DATETIME;
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
