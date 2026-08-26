/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * Formats a duration given in nanoseconds as a human-readable string
 * (e.g. {@code 1_500_000_000} → {@code "1.5s"}).
 *
 * <p>The input is interpreted as nanoseconds. The output uses the largest appropriate unit
 * from: {@code nanos}, {@code micros}, {@code ms}, {@code s}, {@code m}, {@code h}, {@code d}.
 * Fractional values are shown with at most one decimal place. An optional second argument
 * pins the output to a specific unit instead of auto-scaling, e.g. {@code FMT_DURATION(nanos, "s")}.</p>
 */
public class FmtDuration extends EsqlScalarFunction implements OptionalArgument, AnyNullIsNull {

    private static final long MICROS = 1_000L;
    private static final long MILLIS = MICROS * 1_000;
    private static final long SECONDS = MILLIS * 1_000;
    private static final long MINUTES = SECONDS * 60;
    private static final long HOURS = MINUTES * 60;
    private static final long DAYS = HOURS * 24;

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FmtDuration",
        FmtDuration::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(FmtDuration.class)
        .binary(FmtDuration::new)
        .name("fmt_duration");

    private final Expression nanoseconds, unit;

    @FunctionInfo(
        returnType = "keyword",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.6.0") },
        briefSummary = "Formats a duration given in nanoseconds as a human-readable string.",
        description = """
            Returns a human-readable representation of a duration given in nanoseconds.
            For example, `1500000000` (1.5 billion nanoseconds) becomes `"1.5s"`.
            Supported units: `nanos`, `micros`, `ms`, `s`, `m`, `h`, `d`. If a `unit` is provided,
            the output is pinned to that unit instead of auto-scaling to the largest unit that fits.""",
        examples = @Example(file = "format", tag = "fmt_duration")
    )
    public FmtDuration(
        Source source,
        @Param(
            name = "nanoseconds",
            type = { "integer", "long" },
            description = "The duration in nanoseconds to format. If `null`, the function returns `null`."
        ) Expression nanoseconds,
        @Param(
            optional = true,
            name = "unit",
            type = { "keyword" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT, allowedValues = { "nanos", "micros", "ms", "s", "m", "h", "d" }),
            description = "The unit to pin the output to: one of `nanos`, `micros`, `ms`, `s`, `m`, `h`, `d`. "
                + "If omitted, the largest unit that keeps the value at least 1 is used."
        ) Expression unit
    ) {
        super(source, unit != null ? Arrays.asList(nanoseconds, unit) : Arrays.asList(nanoseconds));
        this.nanoseconds = nanoseconds;
        this.unit = unit;
    }

    private FmtDuration(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(nanoseconds);
        out.writeOptionalNamedWriteable(unit);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return KEYWORD;
    }

    public Expression nanoseconds() {
        return nanoseconds;
    }

    public Expression unit() {
        return unit;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        TypeResolution resolution = isType(
            nanoseconds,
            dt -> dt == DataType.INTEGER || dt == DataType.LONG,
            sourceText(),
            FIRST,
            "integer",
            "long"
        );
        if (resolution.unresolved()) {
            return resolution;
        }
        return unit == null ? TypeResolution.TYPE_RESOLVED : isType(unit, dt -> dt == DataType.KEYWORD, sourceText(), SECOND, "keyword");
    }

    @Override
    public boolean foldable() {
        return nanoseconds.foldable() && (unit == null || unit.foldable());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FmtDuration(source(), newChildren.get(0), unit == null ? null : newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FmtDuration::new, nanoseconds, unit);
    }

    @Evaluator(extraName = "FromLong", warnExceptions = IllegalArgumentException.class)
    static BytesRef processLong(long nanoseconds) {
        return new BytesRef(TimeValue.timeValueNanos(nanoseconds).toHumanReadableString(1));
    }

    @Evaluator(extraName = "FromInt", warnExceptions = IllegalArgumentException.class)
    static BytesRef processInt(int nanoseconds) {
        return new BytesRef(TimeValue.timeValueNanos(nanoseconds).toHumanReadableString(1));
    }

    @Evaluator(extraName = "FromLongWithUnit", warnExceptions = IllegalArgumentException.class)
    static BytesRef processLongWithUnit(long nanoseconds, BytesRef unit) {
        return new BytesRef(formatWithUnit(nanoseconds, unit.utf8ToString()));
    }

    @Evaluator(extraName = "FromIntWithUnit", warnExceptions = IllegalArgumentException.class)
    static BytesRef processIntWithUnit(int nanoseconds, BytesRef unit) {
        return new BytesRef(formatWithUnit(nanoseconds, unit.utf8ToString()));
    }

    /**
     * Formats {@code nanoseconds} pinned to the given unit rather than auto-scaling. Mirrors
     * {@link TimeValue#toHumanReadableString} rejection of durations less than {@code -1}.
     */
    static String formatWithUnit(long nanoseconds, String unit) {
        if (nanoseconds < -1) {
            throw new IllegalArgumentException("duration cannot be negative, was given [" + nanoseconds + "]");
        }
        double divisor = switch (unit.toLowerCase(Locale.ROOT)) {
            case "nanos" -> 1;
            case "micros" -> MICROS;
            case "ms" -> MILLIS;
            case "s" -> SECONDS;
            case "m" -> MINUTES;
            case "h" -> HOURS;
            case "d" -> DAYS;
            default -> throw new IllegalArgumentException(
                "Unsupported unit [" + unit + "], expected one of [nanos, micros, ms, s, m, h, d]"
            );
        };
        return Strings.format1Decimals(nanoseconds / divisor, unit.toLowerCase(Locale.ROOT));
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var nanosEvaluator = toEvaluator.apply(nanoseconds);
        DataType nanosType = nanoseconds.dataType();
        if (unit == null) {
            if (nanosType == DataType.LONG) {
                return new FmtDurationFromLongEvaluator.Factory(source(), nanosEvaluator);
            }
            if (nanosType == DataType.INTEGER) {
                return new FmtDurationFromIntEvaluator.Factory(source(), nanosEvaluator);
            }
            throw EsqlIllegalArgumentException.illegalDataType(nanosType);
        }
        var unitEvaluator = toEvaluator.apply(unit);
        if (nanosType == DataType.LONG) {
            return new FmtDurationFromLongWithUnitEvaluator.Factory(source(), nanosEvaluator, unitEvaluator);
        }
        if (nanosType == DataType.INTEGER) {
            return new FmtDurationFromIntWithUnitEvaluator.Factory(source(), nanosEvaluator, unitEvaluator);
        }
        throw EsqlIllegalArgumentException.illegalDataType(nanosType);
    }
}
