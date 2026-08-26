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
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * Formats a byte count as a human-readable string using SI (base-1000) units
 * (e.g. {@code 1500} → {@code "1.5Kb"}).
 *
 * <p>The output uses 1000-based unit suffixes: {@code b}, {@code Kb}, {@code Mb},
 * {@code Gb}, {@code Tb}, {@code Pb}. Values smaller than 1 Kb are shown in bytes.
 * Fractional values are shown with at most one decimal place.</p>
 */
public class FmtBytesSi extends UnaryScalarFunction implements AnyNullIsNull {

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
        .unary(FmtBytesSi::new)
        .name("fmt_bytes_si");

    @FunctionInfo(returnType = "keyword", description = """
        Returns a human-readable representation of a byte count using SI (base-1000) units.
        For example, `1500` becomes `"1.5Kb"`.
        Supported units: `b`, `Kb`, `Mb`, `Gb`, `Tb`, `Pb`.""", examples = @Example(file = "format", tag = "fmt_bytes_si"))
    public FmtBytesSi(
        Source source,
        @Param(
            name = "bytes",
            type = { "integer", "long" },
            description = "The number of bytes to format. If `null`, the function returns `null`."
        ) Expression bytes
    ) {
        super(source, bytes);
    }

    private FmtBytesSi(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        return childrenResolved() == false
            ? new TypeResolution("Unresolved children")
            : isType(field(), dt -> dt == DataType.INTEGER || dt == DataType.LONG, sourceText(), DEFAULT, "integer", "long");
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FmtBytesSi(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FmtBytesSi::new, field());
    }

    @Evaluator(extraName = "FromLong")
    static BytesRef processLong(long bytes) {
        return new BytesRef(formatBytesSi(bytes));
    }

    @Evaluator(extraName = "FromInt")
    static BytesRef processInt(int bytes) {
        return new BytesRef(formatBytesSi(bytes));
    }

    /**
     * Mirrors {@link org.elasticsearch.common.unit.ByteSizeValue#toString} formatting rules
     * (at most one fractional digit, negative values rejected) but with base-1000 units.
     */
    static String formatBytesSi(long bytes) {
        if (bytes < -1) {
            throw new IllegalArgumentException("Values less than [-1] bytes are not supported: [" + bytes + "]");
        }
        double value = bytes;
        String suffix = "b";
        if (bytes >= PB) {
            value = bytes / (double) PB;
            suffix = "Pb";
        } else if (bytes >= TB) {
            value = bytes / (double) TB;
            suffix = "Tb";
        } else if (bytes >= GB) {
            value = bytes / (double) GB;
            suffix = "Gb";
        } else if (bytes >= MB) {
            value = bytes / (double) MB;
            suffix = "Mb";
        } else if (bytes >= KB) {
            value = bytes / (double) KB;
            suffix = "Kb";
        }
        return Strings.format1Decimals(value, suffix);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return switch (PlannerUtils.toElementType(field().dataType())) {
            case LONG -> new FmtBytesSiFromLongEvaluator.Factory(source(), toEvaluator.apply(field()));
            case INT -> new FmtBytesSiFromIntEvaluator.Factory(source(), toEvaluator.apply(field()));
            case NULL -> ConstantEvaluators.CONSTANT_NULL_FACTORY;
            default -> throw new IllegalArgumentException("Unsupported type: " + field().dataType());
        };
    }
}
