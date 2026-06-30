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
import java.util.Locale;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * Formats a ratio (0.0–1.0) as a human-readable percentage string
 * (e.g. {@code 0.5} → {@code "50%"}).
 *
 * <p>The input is multiplied by 100 and displayed with up to one decimal place,
 * followed by a {@code %} sign. Trailing {@code .0} is omitted for whole numbers.</p>
 */
public class FmtPercent extends UnaryScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FmtPercent",
        FmtPercent::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(FmtPercent.class)
        .unary(FmtPercent::new)
        .name("fmt_percent");

    @FunctionInfo(
        returnType = "keyword",
        description = """
            Returns a human-readable percentage representation of a ratio between 0 and 1.
            For example, `0.5` becomes `"50%"` and `0.123` becomes `"12.3%"`.
            The input is multiplied by 100 and formatted with at most one decimal place.""",
        examples = @Example(file = "format", tag = "fmt_percent")
    )
    public FmtPercent(
        Source source,
        @Param(
            name = "ratio",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "A numeric ratio between 0 and 1 to format as a percentage. If `null`, the function returns `null`."
        ) Expression ratio
    ) {
        super(source, ratio);
    }

    private FmtPercent(StreamInput in) throws IOException {
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
        return childrenResolved() == false ? new TypeResolution("Unresolved children") : isNumeric(field(), sourceText(), DEFAULT);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FmtPercent(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FmtPercent::new, field());
    }

    @Evaluator(extraName = "FromDouble")
    static BytesRef processDouble(double ratio) {
        return new BytesRef(formatPercent(ratio * 100.0));
    }

    @Evaluator(extraName = "FromLong")
    static BytesRef processLong(long ratio) {
        return new BytesRef(formatPercent(ratio * 100.0));
    }

    @Evaluator(extraName = "FromInt")
    static BytesRef processInt(int ratio) {
        return new BytesRef(formatPercent(ratio * 100.0));
    }

    static String formatPercent(double pct) {
        String s = String.format(Locale.ROOT, "%.1f", pct);
        if (s.endsWith(".0")) {
            return s.substring(0, s.length() - 2) + "%";
        }
        return s + "%";
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return switch (PlannerUtils.toElementType(field().dataType())) {
            case DOUBLE -> new FmtPercentFromDoubleEvaluator.Factory(source(), toEvaluator.apply(field()));
            case LONG -> new FmtPercentFromLongEvaluator.Factory(source(), toEvaluator.apply(field()));
            case INT -> new FmtPercentFromIntEvaluator.Factory(source(), toEvaluator.apply(field()));
            case NULL -> ConstantEvaluators.CONSTANT_NULL_FACTORY;
            default -> throw new IllegalArgumentException("Unsupported type: " + field().dataType());
        };
    }
}
