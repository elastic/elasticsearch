/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.InvalidArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isDate;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;

/**
 * Subtract the second argument from the third argument and return their difference
 * in multiples of the unit specified in the first argument.
 * If the second argument (start) is greater than the third argument (end), then negative values are returned.
 */
public class DateDiff extends ScalarFunction implements OptionalArgument, EvaluatorMapper {

    public static final ZoneId UTC = ZoneId.of("Z");

    private final Expression unit;
    private final Expression startTimestamp;
    private final Expression endTimestamp;

    public enum Part implements DateTimeField {

        YEAR((start, end) -> end.getYear() - start.getYear(), "years", "yyyy", "yy"),
        QUARTER((start, end) -> safeInt(IsoFields.QUARTER_YEARS.between(start, end)), "quarters", "qq", "q"),
        MONTH((start, end) -> safeInt(ChronoUnit.MONTHS.between(start, end)), "months", "mm", "m"),
        DAYOFYEAR((start, end) -> safeInt(ChronoUnit.DAYS.between(start, end)), "dy", "y"),
        DAY(DAYOFYEAR::diff, "days", "dd", "d"),
        WEEK((start, end) -> safeInt(ChronoUnit.WEEKS.between(start, end)), "weeks", "wk", "ww"),
        WEEKDAY(DAYOFYEAR::diff, "weekdays", "dw"),
        HOUR((start, end) -> safeInt(ChronoUnit.HOURS.between(start, end)), "hours", "hh"),
        MINUTE((start, end) -> safeInt(ChronoUnit.MINUTES.between(start, end)), "minutes", "mi", "n"),
        SECOND((start, end) -> safeInt(ChronoUnit.SECONDS.between(start, end)), "seconds", "ss", "s"),
        MILLISECOND((start, end) -> safeInt(ChronoUnit.MILLIS.between(start, end)), "milliseconds", "ms"),
        MICROSECOND((start, end) -> safeInt(ChronoUnit.MICROS.between(start, end)), "microseconds", "mcs"),
        NANOSECOND((start, end) -> safeInt(ChronoUnit.NANOS.between(start, end)), "nanoseconds", "ns");

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

        public Integer diff(ZonedDateTime startTimestamp, ZonedDateTime endTimestamp) {
            return diffFunction.apply(startTimestamp, endTimestamp);
        }

        @Override
        public Iterable<String> aliases() {
            return aliases;
        }

        private static int safeInt(long diff) {
            if (diff > Integer.MAX_VALUE || diff < Integer.MIN_VALUE) {
                throw new InvalidArgumentException(
                    "The DATE_DIFF function resulted in an overflow; the number of units "
                        + "separating two date/datetime instances is too large. Try to use DATE_DIFF with a less precise unit."
                );
            } else {
                return Long.valueOf(diff).intValue();
            }
        }

        public static List<String> findSimilar(String match) {
            return DateTimeField.findSimilar(NAME_TO_PART.keySet(), match);
        }

        public static Part resolve(String dateTimeUnit) {
            return DateTimeField.resolveMatch(NAME_TO_PART, dateTimeUnit);
        }
    }

    @FunctionInfo(returnType = "integer",
        description = "Subtract 2 dates and return their difference in multiples of a unit specified in the 1st argument")
    public DateDiff(
        Source source,
        @Param(name = "unit", type = { "text" }, description = "A valid date unit") Expression unit,
        @Param(
            name = "startTimestamp",
            type = { "date" },
            description = "A string representing a start timestamp"
        ) Expression startTimestamp,
        @Param(name = "endTimestamp", type = { "date" }, description = "A string representing a end timestamp") Expression endTimestamp
    ) {
        super(source, List.of(unit, startTimestamp, endTimestamp));
        this.unit = unit;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    @Evaluator
    static int process(BytesRef unit, long startTimestamp, long endTimestamp) throws IllegalArgumentException {
        ZonedDateTime zdtStart = ZonedDateTime.ofInstant(Instant.ofEpochMilli(startTimestamp), UTC);
        ZonedDateTime zdtEnd = ZonedDateTime.ofInstant(Instant.ofEpochMilli(endTimestamp), UTC);

        Part datePartField = Part.resolve(unit.utf8ToString());
        if (datePartField == null) {
            List<String> similar = Part.findSimilar(unit.utf8ToString());
            if (similar.isEmpty()) {
                throw new InvalidArgumentException(
                    "A value of {} or their aliases is required; received [{}]",
                    Part.values(),
                    unit.utf8ToString()
                );
            } else {
                throw new InvalidArgumentException(
                    "Received value [{}] is not valid date part to add; " + "did you mean {}?",
                    unit.utf8ToString(),
                    similar
                );
            }
        }

        Integer result = datePartField.diff(zdtStart, zdtEnd);

        return result;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        ExpressionEvaluator.Factory unitEvaluator = toEvaluator.apply(unit);
        ExpressionEvaluator.Factory startTimestampEvaluator = toEvaluator.apply(startTimestamp);
        ExpressionEvaluator.Factory endTimestampEvaluator = toEvaluator.apply(endTimestamp);
        return new DateDiffEvaluator.Factory(source(), unitEvaluator, startTimestampEvaluator, endTimestampEvaluator);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isString(unit, sourceText(), FIRST).and(isDate(startTimestamp, sourceText(), SECOND))
            .and(isDate(endTimestamp, sourceText(), THIRD));

        if (resolution.unresolved()) {
            return resolution;
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
    }

    @Override
    public boolean foldable() {
        return unit.foldable() && startTimestamp.foldable() && endTimestamp.foldable();
    }

    @Override
    public DataType dataType() {
        return DataTypes.INTEGER;
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException("functions do not support scripting");
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new DateDiff(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DateDiff::new, children().get(0), children().get(1), children().get(2));
    }
}
