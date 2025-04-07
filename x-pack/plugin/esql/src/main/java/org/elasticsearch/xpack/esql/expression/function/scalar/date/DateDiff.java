/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeToInt;

/**
 * Subtract the second argument from the third argument and return their difference
 * in multiples of the unit specified in the first argument.
 * If the second argument (start) is greater than the third argument (end), then negative values are returned.
 */
public class DateDiff extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "DateDiff", DateDiff::new);

    public static final ZoneId UTC = ZoneId.of("Z");

    private final Expression unit;
    private final Expression startTimestamp;
    private final Expression endTimestamp;

    /**
     * Represents units that can be used for DATE_DIFF function and how the difference
     * between 2 dates is calculated
     */
    public enum Part implements DateTimeField {

        YEAR((start, end) -> safeToInt(ChronoUnit.YEARS.between(start, end)), "years", "yyyy", "yy"),
        QUARTER((start, end) -> safeToInt(IsoFields.QUARTER_YEARS.between(start, end)), "quarters", "qq", "q"),
        MONTH((start, end) -> safeToInt(ChronoUnit.MONTHS.between(start, end)), "months", "mm", "m"),
        DAYOFYEAR((start, end) -> safeToInt(ChronoUnit.DAYS.between(start, end)), "dy", "y"),
        DAY(DAYOFYEAR::diff, "days", "dd", "d"),
        WEEK((start, end) -> safeToInt(ChronoUnit.WEEKS.between(start, end)), "weeks", "wk", "ww"),
        WEEKDAY(DAYOFYEAR::diff, "weekdays", "dw"),
        HOUR((start, end) -> safeToInt(ChronoUnit.HOURS.between(start, end)), "hours", "hh"),
        MINUTE((start, end) -> safeToInt(ChronoUnit.MINUTES.between(start, end)), "minutes", "mi", "n"),
        SECOND((start, end) -> safeToInt(ChronoUnit.SECONDS.between(start, end)), "seconds", "ss", "s"),
        MILLISECOND((start, end) -> safeToInt(ChronoUnit.MILLIS.between(start, end)), "milliseconds", "ms"),
        MICROSECOND((start, end) -> safeToInt(ChronoUnit.MICROS.between(start, end)), "microseconds", "mcs"),
        NANOSECOND((start, end) -> safeToInt(ChronoUnit.NANOS.between(start, end)), "nanoseconds", "ns");

        private static final Map<String, Part> NAME_TO_PART = DateTimeField.initializeResolutionMap(values());

        private final BiFunction<ZonedDateTime, ZonedDateTime, Integer> diffFunction;
        private final Set<String> aliases;

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

        public static Part resolve(String dateTimeUnit) {
            Part datePartField = DateTimeField.resolveMatch(NAME_TO_PART, dateTimeUnit);
            if (datePartField == null) {
                List<String> similar = DateTimeField.findSimilar(NAME_TO_PART.keySet(), dateTimeUnit);
                String errorMessage;
                if (similar.isEmpty() == false) {
                    errorMessage = String.format(
                        Locale.ROOT,
                        "Received value [%s] is not valid date part to add; did you mean %s?",
                        dateTimeUnit,
                        similar
                    );
                } else {
                    errorMessage = String.format(
                        Locale.ROOT,
                        "A value of %s or their aliases is required; received [%s]",
                        Arrays.asList(Part.values()),
                        dateTimeUnit
                    );
                }
                throw new IllegalArgumentException(errorMessage);
            }

            return datePartField;
        }
    }

    @FunctionInfo(
        returnType = "integer",
        description = """
            Subtracts the `startTimestamp` from the `endTimestamp` and returns the difference in multiples of `unit`.
            If `startTimestamp` is later than the `endTimestamp`, negative values are returned.""",
        detailedDescription = """
            **Datetime difference units**

            | unit | abbreviations |
            | --- | --- |
            | year | years, yy, yyyy |
            | quarter | quarters, qq, q |
            | month | months, mm, m |
            | dayofyear | dy, y |
            | day | days, dd, d |
            | week | weeks, wk, ww |
            | weekday | weekdays, dw |
            | hour | hours, hh |
            | minute | minutes, mi, n |
            | second | seconds, ss, s |
            | millisecond | milliseconds, ms |
            | microsecond | microseconds, mcs |
            | nanosecond | nanoseconds, ns |

            Note that while there is an overlap between the function’s supported units and
            {{esql}}’s supported time span literals, these sets are distinct and not
            interchangeable. Similarly, the supported abbreviations are conveniently shared
            with implementations of this function in other established products and not
            necessarily common with the date-time nomenclature used by {{es}}.""",
        examples = { @Example(file = "date", tag = "docsDateDiff"), @Example(description = """
            When subtracting in calendar units - like year, month a.s.o. - only the fully elapsed units are counted.
            To avoid this and obtain also remainders, simply switch to the next smaller unit and do the date math accordingly.
            """, file = "date", tag = "evalDateDiffYearForDocs") }
    )
    public DateDiff(
        Source source,
        @Param(name = "unit", type = { "keyword", "text" }, description = "Time difference unit") Expression unit,
        @Param(
            name = "startTimestamp",
            type = { "date", "date_nanos" },
            description = "A string representing a start timestamp"
        ) Expression startTimestamp,
        @Param(
            name = "endTimestamp",
            type = { "date", "date_nanos" },
            description = "A string representing an end timestamp"
        ) Expression endTimestamp
    ) {
        super(source, List.of(unit, startTimestamp, endTimestamp));
        this.unit = unit;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    private DateDiff(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(unit);
        out.writeNamedWriteable(startTimestamp);
        out.writeNamedWriteable(endTimestamp);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    Expression unit() {
        return unit;
    }

    Expression startTimestamp() {
        return startTimestamp;
    }

    Expression endTimestamp() {
        return endTimestamp;
    }

    @Evaluator(extraName = "ConstantMillis", warnExceptions = { IllegalArgumentException.class, InvalidArgumentException.class })
    static int processMillis(@Fixed Part datePartFieldUnit, long startTimestamp, long endTimestamp) throws IllegalArgumentException {
        ZonedDateTime zdtStart = ZonedDateTime.ofInstant(Instant.ofEpochMilli(startTimestamp), UTC);
        ZonedDateTime zdtEnd = ZonedDateTime.ofInstant(Instant.ofEpochMilli(endTimestamp), UTC);
        return datePartFieldUnit.diff(zdtStart, zdtEnd);
    }

    @Evaluator(extraName = "Millis", warnExceptions = { IllegalArgumentException.class, InvalidArgumentException.class })
    static int processMillis(BytesRef unit, long startTimestamp, long endTimestamp) throws IllegalArgumentException {
        return processMillis(Part.resolve(unit.utf8ToString()), startTimestamp, endTimestamp);
    }

    @Evaluator(extraName = "ConstantNanos", warnExceptions = { IllegalArgumentException.class, InvalidArgumentException.class })
    static int processNanos(@Fixed Part datePartFieldUnit, long startTimestamp, long endTimestamp) throws IllegalArgumentException {
        ZonedDateTime zdtStart = ZonedDateTime.ofInstant(DateUtils.toInstant(startTimestamp), UTC);
        ZonedDateTime zdtEnd = ZonedDateTime.ofInstant(DateUtils.toInstant(endTimestamp), UTC);
        return datePartFieldUnit.diff(zdtStart, zdtEnd);
    }

    @Evaluator(extraName = "Nanos", warnExceptions = { IllegalArgumentException.class, InvalidArgumentException.class })
    static int processNanos(BytesRef unit, long startTimestamp, long endTimestamp) throws IllegalArgumentException {
        return processNanos(Part.resolve(unit.utf8ToString()), startTimestamp, endTimestamp);
    }

    @Evaluator(extraName = "ConstantNanosMillis", warnExceptions = { IllegalArgumentException.class, InvalidArgumentException.class })
    static int processNanosMillis(@Fixed Part datePartFieldUnit, long startTimestampNanos, long endTimestampMillis)
        throws IllegalArgumentException {
        ZonedDateTime zdtStart = ZonedDateTime.ofInstant(DateUtils.toInstant(startTimestampNanos), UTC);
        ZonedDateTime zdtEnd = ZonedDateTime.ofInstant(Instant.ofEpochMilli(endTimestampMillis), UTC);
        return datePartFieldUnit.diff(zdtStart, zdtEnd);
    }

    @Evaluator(extraName = "NanosMillis", warnExceptions = { IllegalArgumentException.class, InvalidArgumentException.class })
    static int processNanosMillis(BytesRef unit, long startTimestampNanos, long endTimestampMillis) throws IllegalArgumentException {
        return processNanosMillis(Part.resolve(unit.utf8ToString()), startTimestampNanos, endTimestampMillis);
    }

    @Evaluator(extraName = "ConstantMillisNanos", warnExceptions = { IllegalArgumentException.class, InvalidArgumentException.class })
    static int processMillisNanos(@Fixed Part datePartFieldUnit, long startTimestampMillis, long endTimestampNanos)
        throws IllegalArgumentException {
        ZonedDateTime zdtStart = ZonedDateTime.ofInstant(Instant.ofEpochMilli(startTimestampMillis), UTC);
        ZonedDateTime zdtEnd = ZonedDateTime.ofInstant(DateUtils.toInstant(endTimestampNanos), UTC);
        return datePartFieldUnit.diff(zdtStart, zdtEnd);
    }

    @Evaluator(extraName = "MillisNanos", warnExceptions = { IllegalArgumentException.class, InvalidArgumentException.class })
    static int processMillisNanos(BytesRef unit, long startTimestampMillis, long endTimestampNanos) throws IllegalArgumentException {
        return processMillisNanos(Part.resolve(unit.utf8ToString()), startTimestampMillis, endTimestampNanos);
    }

    @FunctionalInterface
    public interface DateDiffFactory {
        ExpressionEvaluator.Factory build(
            Source source,
            ExpressionEvaluator.Factory unitsEvaluator,
            ExpressionEvaluator.Factory startTimestampEvaluator,
            ExpressionEvaluator.Factory endTimestampEvaluator
        );
    }

    @FunctionalInterface
    public interface DateDiffConstantFactory {
        ExpressionEvaluator.Factory build(
            Source source,
            Part unitsEvaluator,
            ExpressionEvaluator.Factory startTimestampEvaluator,
            ExpressionEvaluator.Factory endTimestampEvaluator
        );
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (startTimestamp.dataType() == DATETIME && endTimestamp.dataType() == DATETIME) {
            return toEvaluator(toEvaluator, DateDiffConstantMillisEvaluator.Factory::new, DateDiffMillisEvaluator.Factory::new);
        } else if (startTimestamp.dataType() == DATE_NANOS && endTimestamp.dataType() == DATE_NANOS) {
            return toEvaluator(toEvaluator, DateDiffConstantNanosEvaluator.Factory::new, DateDiffNanosEvaluator.Factory::new);
        } else if (startTimestamp.dataType() == DATE_NANOS && endTimestamp.dataType() == DATETIME) {
            return toEvaluator(toEvaluator, DateDiffConstantNanosMillisEvaluator.Factory::new, DateDiffNanosMillisEvaluator.Factory::new);
        } else if (startTimestamp.dataType() == DATETIME && endTimestamp.dataType() == DATE_NANOS) {
            return toEvaluator(toEvaluator, DateDiffConstantMillisNanosEvaluator.Factory::new, DateDiffMillisNanosEvaluator.Factory::new);
        }
        throw new UnsupportedOperationException(
            "Invalid types ["
                + startTimestamp.dataType()
                + ", "
                + endTimestamp.dataType()
                + "] "
                + "If you see this error, there is a bug in DateDiff.resolveType()"
        );
    }

    private ExpressionEvaluator.Factory toEvaluator(
        ToEvaluator toEvaluator,
        DateDiffConstantFactory constantFactory,
        DateDiffFactory dateDiffFactory
    ) {
        ExpressionEvaluator.Factory startTimestampEvaluator = toEvaluator.apply(startTimestamp);
        ExpressionEvaluator.Factory endTimestampEvaluator = toEvaluator.apply(endTimestamp);

        if (unit.foldable()) {
            try {
                Part datePartField = Part.resolve(((BytesRef) unit.fold(toEvaluator.foldCtx())).utf8ToString());
                return constantFactory.build(source(), datePartField, startTimestampEvaluator, endTimestampEvaluator);
            } catch (IllegalArgumentException e) {
                throw new InvalidArgumentException("invalid unit format for [{}]: {}", sourceText(), e.getMessage());
            }
        }
        ExpressionEvaluator.Factory unitEvaluator = toEvaluator.apply(unit);
        return dateDiffFactory.build(source(), unitEvaluator, startTimestampEvaluator, endTimestampEvaluator);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        String operationName = sourceText();
        TypeResolution resolution = isString(unit, sourceText(), FIRST).and(
            TypeResolutions.isType(startTimestamp, DataType::isDate, operationName, SECOND, "datetime or date_nanos")
        ).and(TypeResolutions.isType(endTimestamp, DataType::isDate, operationName, THIRD, "datetime or date_nanos"));

        if (resolution.unresolved()) {
            return resolution;
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean foldable() {
        return unit.foldable() && startTimestamp.foldable() && endTimestamp.foldable();
    }

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
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
