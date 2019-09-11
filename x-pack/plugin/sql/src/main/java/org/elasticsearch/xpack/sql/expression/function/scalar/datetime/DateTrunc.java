/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.time.IsoLocale;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.Nullability;
import org.elasticsearch.xpack.sql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.sql.expression.TypeResolutions.isDate;
import static org.elasticsearch.xpack.sql.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTruncProcessor.process;
import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

public class DateTrunc extends BinaryScalarFunction {

    public enum Part {

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

        static {
            NAME_TO_PART = new HashMap<>();

            for (Part datePart : Part.values()) {
                String lowerCaseName = datePart.name().toLowerCase(IsoLocale.ROOT);

                NAME_TO_PART.put(lowerCaseName, datePart);
                for (String alias : datePart.aliases) {
                    NAME_TO_PART.put(alias, datePart);
                }
            }
        }

        private Set<String> aliases;
        private Function<ZonedDateTime, ZonedDateTime> truncateFunction;

        Part(Function<ZonedDateTime, ZonedDateTime> truncateFunction, String... aliases) {
            this.truncateFunction = truncateFunction;
            this.aliases = Set.of(aliases);
        }

        public static Part resolveTruncate(String truncateTo) {
            return NAME_TO_PART.get(truncateTo.toLowerCase(IsoLocale.ROOT));
        }

        public static List<String> findSimilar(String match) {
            return StringUtils.findSimilar(match, NAME_TO_PART.keySet());
        }

        public ZonedDateTime truncate(ZonedDateTime dateTime) {
            return truncateFunction.apply(dateTime);
        }
    }

    private final ZoneId zoneId;

    public DateTrunc(Source source, Expression truncateTo, Expression timestamp, ZoneId zoneId) {
        super(source, truncateTo, timestamp);
        this.zoneId = zoneId;
    }

    @Override
    public DataType dataType() {
        return DataType.DATETIME;
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = isString(left(), sourceText(), Expressions.ParamOrdinal.FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        if (left().foldable()) {
            String truncateToValue = (String) left().fold();
            if (truncateToValue != null && Part.resolveTruncate(truncateToValue) == null) {
                List<String> similar = Part.findSimilar(truncateToValue);
                if (similar.isEmpty()) {
                    return new TypeResolution(format(null, "first argument of [{}] must be one of {} or their aliases, found value [{}]",
                        sourceText(),
                        Part.values(),
                        Expressions.name(left())));
                } else {
                    return new TypeResolution(format(null, "Unknown value [{}] for first argument of [{}]; did you mean {}?",
                        Expressions.name(left()),
                        sourceText(),
                        similar));
                }
            }
        }
        resolution = isDate(right(), sourceText(), Expressions.ParamOrdinal.SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression newTruncateTo, Expression newTimestamp) {
        return new DateTrunc(source(), newTruncateTo, newTimestamp, zoneId);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DateTrunc::new, left(), right(), zoneId);
    }

    @Override
    protected Pipe makePipe() {
        return new DateTruncPipe(source(), this, Expressions.pipe(left()), Expressions.pipe(right()), zoneId);
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }

    @Override
    public Object fold() {
        return process(left().fold(), right().fold(), zoneId);
    }

    @Override
    protected ScriptTemplate asScriptFrom(ScriptTemplate leftScript, ScriptTemplate rightScript) {
        return new ScriptTemplate(
            formatTemplate("{sql}.dateTrunc(" + leftScript.template() + "," + rightScript.template()+ ",{})"),
            paramsBuilder()
                .script(leftScript.params())
                .script(rightScript.params())
                .variable(zoneId.getId())
                .build(),
            dataType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), zoneId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        DateTrunc dateTrunc = (DateTrunc) o;
        return Objects.equals(zoneId, dateTrunc.zoneId);
    }
}
