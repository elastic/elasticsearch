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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.ann.Evaluator;
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
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isWholeNumber;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * Formats a byte count as a human-readable string using binary (base-1024) units
 * (e.g. {@code 1536} → {@code "1.5kb"}).
 *
 * <p>The output uses 1024-based unit suffixes: {@code b}, {@code kb}, {@code mb},
 * {@code gb}, {@code tb}, {@code pb}. Values smaller than 1 kb are shown in bytes.
 * Fractional values are shown with at most one decimal place.</p>
 */
public class FmtBytes extends UnaryScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FmtBytes",
        FmtBytes::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(FmtBytes.class)
        .unary(FmtBytes::new)
        .name("fmt_bytes");

    @FunctionInfo(
        returnType = "keyword",
        description = """
            Returns a human-readable representation of a byte count using binary (base-1024) units.
            For example, `1536` becomes `"1.5kb"`.
            Supported units: `b`, `kb`, `mb`, `gb`, `tb`, `pb`.""",
        examples = @Example(file = "format", tag = "fmt_bytes")
    )
    public FmtBytes(
        Source source,
        @Param(
            name = "bytes",
            type = { "integer", "long", "unsigned_long" },
            description = "The number of bytes to format. If `null`, the function returns `null`."
        ) Expression bytes
    ) {
        super(source, bytes);
    }

    private FmtBytes(StreamInput in) throws IOException {
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
        return new FmtBytes(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FmtBytes::new, field());
    }

    @Evaluator(extraName = "FromLong")
    static BytesRef processLong(long bytes) {
        return new BytesRef(ByteSizeValue.ofBytes(bytes).toString());
    }

    @Evaluator(extraName = "FromInt")
    static BytesRef processInt(int bytes) {
        return new BytesRef(ByteSizeValue.ofBytes(bytes).toString());
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return switch (PlannerUtils.toElementType(field().dataType())) {
            case LONG -> new FmtBytesFromLongEvaluator.Factory(source(), toEvaluator.apply(field()));
            case INT -> new FmtBytesFromIntEvaluator.Factory(source(), toEvaluator.apply(field()));
            case NULL -> ConstantEvaluators.CONSTANT_NULL_FACTORY;
            default -> throw new IllegalArgumentException("Unsupported type: " + field().dataType());
        };
    }
}
