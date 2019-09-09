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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.sql.expression.TypeResolutions.isDate;
import static org.elasticsearch.xpack.sql.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTruncProcessor.process;
import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

public class DateTrunc extends BinaryScalarFunction {

    public enum DatePart {

        MILLENNIUM("millennia"),
        CENTURY("centuries"),
        DECADE("decades"),
        YEAR("years", "yy", "yyyy"),
        QUARTER("quarters", "qq", "q"),
        MONTH("months", "mm", "m"),
        WEEK("weeks", "wk", "ww"),
        DAY("days", "dd", "d"),
        HOUR("hours", "hh"),
        MINUTE("minutes", "mi", "n"),
        SECOND("seconds", "ss", "s"),
        MILLISECOND("milliseconds", "ms"),
        MICROSECOND("microseconds", "mcs"),
        NANOSECOND("nanoseconds", "ns");

        private static final Set<String> ALL_DATE_PARTS;
        private static final Map<String, DatePart> RESOLVE_MAP;

        static {
            ALL_DATE_PARTS = new HashSet<>();
            RESOLVE_MAP = new HashMap<>();

            for (DatePart datePart : DatePart.values()) {
                String lowerCaseName = datePart.name().toLowerCase(IsoLocale.ROOT);
                ALL_DATE_PARTS.add(lowerCaseName);
                ALL_DATE_PARTS.addAll(datePart.aliases);

                RESOLVE_MAP.put(lowerCaseName, datePart);
                for (String alias : datePart.aliases) {
                    RESOLVE_MAP.put(alias, datePart);
                }
            }
        }

        private Set<String> aliases;

        DatePart(String... aliases) {
            this.aliases = Set.of(aliases);
        }

        public static DatePart resolveTruncate(String truncateTo) {
            return RESOLVE_MAP.get(truncateTo.toLowerCase(IsoLocale.ROOT));
        }

        public static List<String> findSimilar(String match) {
            return StringUtils.findSimilar(match, ALL_DATE_PARTS);
        }

        public static ZonedDateTime truncate(ZonedDateTime dateTime, DateTrunc.DatePart datePart) {
            ZonedDateTime truncated = null;
            switch (datePart) {
                case MILLENNIUM:
                    int year = dateTime.getYear();
                    int firstYearOfMillenium = year - (year % 1000);
                    truncated = dateTime
                        .with(ChronoField.YEAR, firstYearOfMillenium)
                        .with(ChronoField.MONTH_OF_YEAR, 1)
                        .with(ChronoField.DAY_OF_MONTH, 1)
                        .toLocalDate().atStartOfDay(dateTime.getZone());
                    break;
                case CENTURY:
                    year = dateTime.getYear();
                    int firstYearOfCentury = year - (year % 100);
                    truncated = dateTime
                        .with(ChronoField.YEAR, firstYearOfCentury)
                        .with(ChronoField.MONTH_OF_YEAR, 1)
                        .with(ChronoField.DAY_OF_MONTH, 1)
                        .toLocalDate().atStartOfDay(dateTime.getZone());
                    break;
                case DECADE:
                    year = dateTime.getYear();
                    int firstYearOfDecade = year - (year % 10);
                    truncated = dateTime
                        .with(ChronoField.YEAR, firstYearOfDecade)
                        .with(ChronoField.MONTH_OF_YEAR, 1)
                        .with(ChronoField.DAY_OF_MONTH, 1)
                        .toLocalDate().atStartOfDay(dateTime.getZone());
                    break;
                case YEAR:
                    truncated = dateTime
                        .with(ChronoField.MONTH_OF_YEAR, 1)
                        .with(ChronoField.DAY_OF_MONTH, 1)
                        .toLocalDate().atStartOfDay(dateTime.getZone());
                    break;
                case QUARTER:
                    int month = dateTime.getMonthValue();
                    int firstMonthOfQuarter = (((month - 1) / 3) * 3) + 1;
                    truncated = dateTime
                        .with(ChronoField.MONTH_OF_YEAR, firstMonthOfQuarter)
                        .with(ChronoField.DAY_OF_MONTH, 1)
                        .toLocalDate().atStartOfDay(dateTime.getZone());
                    break;
                case MONTH:
                    truncated = dateTime
                        .with(ChronoField.DAY_OF_MONTH, 1)
                        .toLocalDate().atStartOfDay(dateTime.getZone());
                    break;
                case WEEK:
                    truncated = dateTime
                        .with(ChronoField.DAY_OF_WEEK, 1)
                        .toLocalDate().atStartOfDay(dateTime.getZone());
                    break;
                case DAY:
                    truncated = dateTime
                        .toLocalDate().atStartOfDay(dateTime.getZone());
                    break;
                case HOUR:
                    int hour = dateTime.getHour();
                    truncated = dateTime.toLocalDate().atStartOfDay(dateTime.getZone())
                        .with(ChronoField.HOUR_OF_DAY, hour);
                    break;
                case MINUTE:
                    hour = dateTime.getHour();
                    int minute = dateTime.getMinute();
                    truncated = dateTime.toLocalDate().atStartOfDay(dateTime.getZone())
                        .with(ChronoField.HOUR_OF_DAY, hour)
                        .with(ChronoField.MINUTE_OF_HOUR, minute);
                    break;
                case SECOND:
                    truncated = dateTime
                        .with(ChronoField.NANO_OF_SECOND, 0);
                    break;
                case MILLISECOND:
                    int micros = dateTime.get(ChronoField.MICRO_OF_SECOND);
                    truncated = dateTime
                        .with(ChronoField.MILLI_OF_SECOND, (micros / 1000));
                    break;
                case MICROSECOND:
                    int nanos = dateTime.getNano();
                    truncated = dateTime
                        .with(ChronoField.MICRO_OF_SECOND, (nanos / 1000));
                    break;
                case NANOSECOND:
                    truncated = dateTime;
                    break;
            }
            return truncated;
        }
    }


    private final ZoneId zoneId;

    public DateTrunc(Source source, Expression truncateTo, Expression timestamp, ZoneId zoneId) {
        super(source, truncateTo, timestamp);
        this.zoneId = zoneId;
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
            String truncateToValue = (String) left().fold();
            if (DatePart.resolveTruncate(truncateToValue) == null) {
                List<String> similar = DatePart.findSimilar(truncateToValue);
                if (similar.isEmpty()) {
                    return new TypeResolution(format(null, "first argument of [{}] must be one of {} or their aliases, found value [{}]",
                        sourceText(),
                        DatePart.values(),
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
