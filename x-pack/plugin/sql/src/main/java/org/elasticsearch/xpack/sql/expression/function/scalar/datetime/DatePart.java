/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.time.IsoLocale;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Nullability;
import org.elasticsearch.xpack.sql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NonIsoDateTimeProcessor.NonIsoDateTimeExtractor;

public class DatePart extends BinaryDateTimeFunction {

    public enum Part {
        YEAR(DateTimeExtractor.YEAR::extract, "years", "yyyy", "yy"),
        QUARTER(QuarterProcessor::quarter, "quarters", "qq", "q"),
        MONTH(DateTimeExtractor.MONTH_OF_YEAR::extract, "months", "mm", "m"),
        DAYOFYEAR(DateTimeExtractor.DAY_OF_YEAR::extract, "dy", "y"),
        DAY(DateTimeExtractor.DAY_OF_MONTH::extract, "days", "dd", "d"),
        WEEK(NonIsoDateTimeExtractor.WEEK_OF_YEAR::extract, "weeks", "wk", "ww"),
        WEEKDAY(NonIsoDateTimeExtractor.DAY_OF_WEEK::extract, "weekdays", "dw"),
        HOUR(DateTimeExtractor.HOUR_OF_DAY::extract, "hours", "hh"),
        MINUTE(DateTimeExtractor.MINUTE_OF_HOUR::extract, "minutes", "mi", "n"),
        SECOND(DateTimeExtractor.SECOND_OF_MINUTE::extract, "seconds", "ss", "s"),
        MILLISECOND(dt -> {
                return dt.get(ChronoField.MILLI_OF_SECOND);
            }, "milliseconds", "ms"),
        MICROSECOND(dt -> {
            return dt.get(ChronoField.MICRO_OF_SECOND);
            }, "microseconds", "mcs"),
        NANOSECOND(ZonedDateTime::getNano, "nanoseconds", "ns"),
        TZOFFSET(dt -> dt.getOffset().getTotalSeconds() / 60, "tz");

        private static final Map<String, Part> NAME_TO_PART;
        private static final List<String> VALID_VALUES;

        static {
            NAME_TO_PART = new HashMap<>();
            VALID_VALUES = new ArrayList<>(Part.values().length);

            for (Part datePart : Part.values()) {
                String lowerCaseName = datePart.name().toLowerCase(IsoLocale.ROOT);

                NAME_TO_PART.put(lowerCaseName, datePart);
                for (String alias : datePart.aliases) {
                    NAME_TO_PART.put(alias, datePart);
                }

                VALID_VALUES.add(datePart.name());
            }
        }

        private Set<String> aliases;
        private Function<ZonedDateTime, Integer> extractFunction;

        Part(Function<ZonedDateTime, Integer> extractFunction, String... aliases) {
            this.extractFunction = extractFunction;
            this.aliases = Set.of(aliases);
        }

        public static Part resolveTruncate(String truncateTo) {
            return NAME_TO_PART.get(truncateTo.toLowerCase(IsoLocale.ROOT));
        }

        public static List<String> findSimilar(String match) {
            return StringUtils.findSimilar(match, NAME_TO_PART.keySet());
        }

        public Integer extract(ZonedDateTime dateTime) {
            return extractFunction.apply(dateTime);
        }
    }

    public DatePart(Source source, Expression truncateTo, Expression timestamp, ZoneId zoneId) {
        super(source, truncateTo, timestamp, zoneId, BinaryDateTimeProcessor.BinaryDateOperation.PART);
    }

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression newTruncateTo, Expression newTimestamp) {
        return new DatePart(source(), newTruncateTo, newTimestamp, zoneId());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DatePart::new, left(), right(), zoneId());
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }

    @Override
    protected boolean resolveDateTimeField(String dateTimeField) {
        return Part.resolveTruncate(dateTimeField) != null;
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
