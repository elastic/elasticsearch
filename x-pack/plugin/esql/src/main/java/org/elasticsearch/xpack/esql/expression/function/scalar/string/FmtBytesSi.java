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
 * Formats a byte count as a human-readable string using SI (base-1000) units
 * (e.g. {@code 1500} → {@code "1.5KB"}).
 *
 * <p>The output uses 1000-based unit suffixes: {@code B}, {@code KB}, {@code MB},
 * {@code GB}, {@code TB}, {@code PB} (all with an upper-case {@code B}, unlike
 * {@link FmtBytes}'s binary suffixes, to avoid reading like bit units). Values smaller
 * than 1 KB are shown in bytes. Fractional values are shown with at most one decimal
 * place. An optional second argument pins the output to a specific unit instead of
 * auto-scaling, e.g. {@code FMT_BYTES_SI(bytes, "MB")}.</p>
 */
public class FmtBytesSi extends EsqlScalarFunction implements OptionalArgument, AnyNullIsNull {

    private static final long KB = 1_000L;
    private static final long MB = KB * 1_000;
    private static final long GB = MB * 1_000;
    private static final long TB = GB * 1_000;
    private static final long PB = TB * 1_000;

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FmtBytesSi",
        FmtBytesSi::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(FmtBytesSi.class)
        .binary(FmtBytesSi::new)
        .name("fmt_bytes_si");

    private final Expression bytes, unit;

    @FunctionInfo(
        returnType = "keyword",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.6.0") },
        briefSummary = "Formats a byte count as a human-readable string using SI (base-1000) units.",
        description = """
            Returns a human-readable representation of a byte count using SI (base-1000) units.
            For example, `1500` becomes `"1.5KB"`.
            Supported units: `B`, `KB`, `MB`, `GB`, `TB`, `PB`. If a `unit` is provided, the output
            is pinned to that unit instead of auto-scaling to the largest unit that fits.""",
        examples = @Example(file = "format", tag = "fmt_bytes_si")
    )
    public FmtBytesSi(
        Source source,
        @Param(
            name = "bytes",
            type = { "integer", "long" },
            description = "The number of bytes to format. If `null`, the function returns `null`."
        ) Expression bytes,
        @Param(
            optional = true,
            name = "unit",
            type = { "keyword" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT, allowedValues = { "b", "kb", "mb", "gb", "tb", "pb" }),
            description = "The unit to pin the output to: one of `B`, `KB`, `MB`, `GB`, `TB`, `PB` (case-insensitive). "
                + "If omitted, the largest unit that keeps the value at least 1 is used."
        ) Expression unit
    ) {
        super(source, unit != null ? Arrays.asList(bytes, unit) : Arrays.asList(bytes));
        this.bytes = bytes;
        this.unit = unit;
    }

    private FmtBytesSi(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(bytes);
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

    public Expression bytes() {
        return bytes;
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
            bytes,
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
        return bytes.foldable() && (unit == null || unit.foldable());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FmtBytesSi(source(), newChildren.get(0), unit == null ? null : newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FmtBytesSi::new, bytes, unit);
    }

    @Evaluator(extraName = "FromLong", warnExceptions = IllegalArgumentException.class)
    static BytesRef processLong(long bytes) {
        return new BytesRef(formatBytesSi(bytes));
    }

    @Evaluator(extraName = "FromInt", warnExceptions = IllegalArgumentException.class)
    static BytesRef processInt(int bytes) {
        return new BytesRef(formatBytesSi(bytes));
    }

    @Evaluator(extraName = "FromLongWithUnit", warnExceptions = IllegalArgumentException.class)
    static BytesRef processLongWithUnit(long bytes, BytesRef unit) {
        return new BytesRef(formatWithUnit(bytes, unit.utf8ToString()));
    }

    @Evaluator(extraName = "FromIntWithUnit", warnExceptions = IllegalArgumentException.class)
    static BytesRef processIntWithUnit(int bytes, BytesRef unit) {
        return new BytesRef(formatWithUnit(bytes, unit.utf8ToString()));
    }

    /**
     * Mirrors {@link org.elasticsearch.common.unit.ByteSizeValue#toString} formatting rules
     * (at most one fractional digit, negative values other than {@code -1} rejected) but with
     * base-1000 units and an upper-case {@code B} suffix to avoid reading like bit units.
     */
    static String formatBytesSi(long bytes) {
        if (bytes < -1) {
            throw new IllegalArgumentException("Values less than [-1] bytes are not supported: [" + bytes + "]");
        }
        double value = bytes;
        String suffix = "B";
        if (bytes >= PB) {
            value = bytes / (double) PB;
            suffix = "PB";
        } else if (bytes >= TB) {
            value = bytes / (double) TB;
            suffix = "TB";
        } else if (bytes >= GB) {
            value = bytes / (double) GB;
            suffix = "GB";
        } else if (bytes >= MB) {
            value = bytes / (double) MB;
            suffix = "MB";
        } else if (bytes >= KB) {
            value = bytes / (double) KB;
            suffix = "KB";
        }
        return Strings.format1Decimals(value, suffix);
    }

    /**
     * Formats {@code bytes} pinned to the given unit rather than auto-scaling. {@code -1} is
     * mirrored from {@link #formatBytesSi} as the "unbounded" sentinel and rendered the same
     * way regardless of the requested unit, rather than being divided into a near-zero fraction.
     */
    static String formatWithUnit(long bytes, String unit) {
        if (bytes < -1) {
            throw new IllegalArgumentException("Values less than [-1] bytes are not supported: [" + bytes + "]");
        }
        String canonicalUnit;
        double divisor;
        switch (unit.toLowerCase(Locale.ROOT)) {
            case "b" -> {
                canonicalUnit = "B";
                divisor = 1;
            }
            case "kb" -> {
                canonicalUnit = "KB";
                divisor = KB;
            }
            case "mb" -> {
                canonicalUnit = "MB";
                divisor = MB;
            }
            case "gb" -> {
                canonicalUnit = "GB";
                divisor = GB;
            }
            case "tb" -> {
                canonicalUnit = "TB";
                divisor = TB;
            }
            case "pb" -> {
                canonicalUnit = "PB";
                divisor = PB;
            }
            default -> throw new IllegalArgumentException("Unsupported unit [" + unit + "], expected one of [B, KB, MB, GB, TB, PB]");
        }
        if (bytes == -1) {
            return formatBytesSi(bytes);
        }
        return Strings.format1Decimals(bytes / divisor, canonicalUnit);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var bytesEvaluator = toEvaluator.apply(bytes);
        DataType bytesType = bytes.dataType();
        if (unit == null) {
            if (bytesType == DataType.LONG) {
                return new FmtBytesSiFromLongEvaluator.Factory(source(), bytesEvaluator);
            }
            if (bytesType == DataType.INTEGER) {
                return new FmtBytesSiFromIntEvaluator.Factory(source(), bytesEvaluator);
            }
            throw EsqlIllegalArgumentException.illegalDataType(bytesType);
        }
        var unitEvaluator = toEvaluator.apply(unit);
        if (bytesType == DataType.LONG) {
            return new FmtBytesSiFromLongWithUnitEvaluator.Factory(source(), bytesEvaluator, unitEvaluator);
        }
        if (bytesType == DataType.INTEGER) {
            return new FmtBytesSiFromIntWithUnitEvaluator.Factory(source(), bytesEvaluator, unitEvaluator);
        }
        throw EsqlIllegalArgumentException.illegalDataType(bytesType);
    }
}
