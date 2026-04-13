/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.TimeValue;
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
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isWholeNumber;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * Formats a duration given in nanoseconds as a human-readable string
 * (e.g. {@code 1_500_000_000} → {@code "1.5s"}).
 *
 * <p>The input is interpreted as nanoseconds. The output uses the largest appropriate unit
 * from: {@code nanos}, {@code micros}, {@code ms}, {@code s}, {@code m}, {@code h}, {@code d}.
 * Fractional values are shown with at most one decimal place.</p>
 */
public class FmtDuration extends UnaryScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FmtDuration",
        FmtDuration::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(FmtDuration.class)
        .unary(FmtDuration::new)
        .name("fmt_duration");

    @FunctionInfo(
        returnType = "keyword",
        description = """
            Returns a human-readable representation of a duration given in nanoseconds.
            For example, `1500000000` (1.5 billion nanoseconds) becomes `"1.5s"`.
            Supported units: `nanos`, `micros`, `ms`, `s`, `m`, `h`, `d`.""",
        examples = @Example(file = "format", tag = "fmt_duration")
    )
    public FmtDuration(
        Source source,
        @Param(
            name = "nanoseconds",
            type = { "integer", "long", "unsigned_long" },
            description = "The duration in nanoseconds to format. If `null`, the function returns `null`."
        ) Expression nanoseconds
    ) {
        super(source, nanoseconds);
    }

    private FmtDuration(StreamInput in) throws IOException {
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
        return childrenResolved() == false ? new TypeResolution("Unresolved children") : isWholeNumber(field(), sourceText(), DEFAULT);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FmtDuration(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FmtDuration::new, field());
    }

    @Evaluator(extraName = "FromLong")
    static BytesRef processLong(long nanoseconds) {
        return new BytesRef(TimeValue.timeValueNanos(nanoseconds).toHumanReadableString(1));
    }

    @Evaluator(extraName = "FromInt")
    static BytesRef processInt(int nanoseconds) {
        return new BytesRef(TimeValue.timeValueNanos(nanoseconds).toHumanReadableString(1));
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return switch (PlannerUtils.toElementType(field().dataType())) {
            case LONG -> new FmtDurationFromLongEvaluator.Factory(source(), toEvaluator.apply(field()));
            case INT -> new FmtDurationFromIntEvaluator.Factory(source(), toEvaluator.apply(field()));
            case NULL -> ConstantEvaluators.CONSTANT_NULL_FACTORY;
            default -> throw new IllegalArgumentException("Unsupported type: " + field().dataType());
        };
    }
}
