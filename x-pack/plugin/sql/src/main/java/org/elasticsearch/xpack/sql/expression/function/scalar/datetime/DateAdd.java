/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isInteger;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.sql.expression.SqlTypeResolutions.isDate;

public class DateAdd extends ThreeArgsDateTimeFunction {

    public enum Part implements DateTimeField {
        YEAR((dt, i) -> dt.plus(i, ChronoUnit.YEARS), "years", "yyyy", "yy"),
        QUARTER((dt, i) -> dt.plus(i * 3, ChronoUnit.MONTHS), "quarters", "qq", "q"),
        MONTH((dt, i) -> dt.plus(i, ChronoUnit.MONTHS), "months", "mm", "m"),
        DAYOFYEAR((dt, i) -> dt.plus(i, ChronoUnit.DAYS), "dy", "y"),
        DAY((dt, i) -> dt.plus(i, ChronoUnit.DAYS), "days", "dd", "d"),
        WEEK((dt, i) -> dt.plus(i, ChronoUnit.WEEKS), "weeks", "wk", "ww"),
        WEEKDAY((dt, i) -> dt.plus(i, ChronoUnit.DAYS),  "weekdays", "dw"),
        HOUR((dt, i) -> dt.plus(i, ChronoUnit.HOURS),  "hours", "hh"),
        MINUTE((dt, i) -> dt.plus(i, ChronoUnit.MINUTES), "minutes", "mi", "n"),
        SECOND((dt, i) -> dt.plus(i, ChronoUnit.SECONDS), "seconds", "ss", "s"),
        MILLISECOND((dt, i) -> dt.plus(i, ChronoUnit.MILLIS), "milliseconds", "ms"),
        MICROSECOND((dt, i) -> dt.plus(i, ChronoUnit.MICROS), "microseconds", "mcs"),
        NANOSECOND((dt, i) -> dt.plus(i, ChronoUnit.NANOS), "nanoseconds", "ns");

        private static final Map<String, Part> NAME_TO_PART;
        private static final List<String> VALID_VALUES;

        static {
            NAME_TO_PART = DateTimeField.initializeResolutionMap(values());
            VALID_VALUES = DateTimeField.initializeValidValues(values());
        }

        private BiFunction<ZonedDateTime, Integer, ZonedDateTime> addFunction;
        private Set<String> aliases;

        Part(BiFunction<ZonedDateTime, Integer, ZonedDateTime> addFunction, String... aliases) {
            this.addFunction = addFunction;
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

        public ZonedDateTime add(ZonedDateTime dateTime, Integer numberOfUnits) {
            return addFunction.apply(dateTime, numberOfUnits);
        }
    }

    public DateAdd(Source source, Expression unit, Expression numberOfUnits, Expression timestamp, ZoneId zoneId) {
        super(source, unit, numberOfUnits, timestamp, zoneId);
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

        resolution = isInteger(second(), sourceText(), Expressions.ParamOrdinal.SECOND);
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
        return DataTypes.DATETIME;
    }

    @Override
    protected ThreeArgsDateTimeFunction replaceChildren(Expression newFirst, Expression newSecond, Expression newThird) {
        return new DateAdd(source(), newFirst, newSecond, newThird, zoneId());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DateAdd::new, first(), second(), third(), zoneId());
    }

    @Override
    protected Pipe createPipe(Pipe unit, Pipe numberOfUnits, Pipe timestamp, ZoneId zoneId) {
        return new DateAddPipe(source(), this, unit, numberOfUnits, timestamp, zoneId);
    }

    @Override
    protected String scriptMethodName() {
        return "dateAdd";
    }

    @Override
    public Object fold() {
        return DateAddProcessor.process(first().fold(), second().fold(), third().fold(), zoneId());
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
