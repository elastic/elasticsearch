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
import java.util.Locale;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * Formats a number as a percentage string (e.g. {@code 0.75} → {@code "75%"}).
 *
 * <p>The input is a number between 0 and 1 (or 0 to 100). The output is formatted
 * as a percentage with a `%` suffix.</p>
 */
public class FmtPercent extends UnaryScalarFunction implements AnyNullIsNull {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FmtPercent",
        FmtPercent::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(FmtPercent.class).unary(FmtPercent::new).name("fmt_percent");

    @FunctionInfo(returnType = "keyword", description = """
        Returns a human-readable representation of a number as a percentage.
        For example, `0.75` becomes `"75%"` and `75` becomes `"7500%"`.
        The input is multiplied by 100 and formatted with a `%` suffix.""", examples = @Example(file = "format", tag = "fmt_percent"))
    public FmtPercent(
        Source source,
        @Param(
            name = "value",
            type = { "integer", "long", "double" },
            description = "The number to format as a percentage. If `null`, the function returns `null`."
        ) Expression value
    ) {
        super(source, value);
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
        return childrenResolved() == false
            ? new TypeResolution("Unresolved children")
            : isType(
                field(),
                dt -> dt == DataType.INTEGER || dt == DataType.LONG || dt == DataType.DOUBLE,
                sourceText(),
                DEFAULT,
                "numeric"
            );
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FmtPercent(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FmtPercent::new, field());
    }

    @Evaluator(extraName = "FromLong")
    static BytesRef processLong(long value) {
        return new BytesRef(formatPercent(value));
    }

    @Evaluator(extraName = "FromInt")
    static BytesRef processInt(int value) {
        return new BytesRef(formatPercent(value));
    }

    @Evaluator(extraName = "FromDouble")
    static BytesRef processDouble(double value) {
        return new BytesRef(formatPercent(value));
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return switch (PlannerUtils.toElementType(field().dataType())) {
            case LONG -> new FmtPercentFromLongEvaluator.Factory(source(), toEvaluator.apply(field()));
            case INT -> new FmtPercentFromIntEvaluator.Factory(source(), toEvaluator.apply(field()));
            case DOUBLE -> new FmtPercentFromDoubleEvaluator.Factory(source(), toEvaluator.apply(field()));
            case NULL -> ConstantEvaluators.CONSTANT_NULL_FACTORY;
            default -> throw new IllegalArgumentException("Unsupported type: " + field().dataType());
        };
    }

    private static String formatPercent(long value) {
        return formatPercent((double) value);
    }

    private static String formatPercent(int value) {
        return formatPercent((double) value);
    }

    private static String formatPercent(double value) {
        double percent = value * 100.0;
        if (percent == (long) percent) {
            return ((long) percent) + "%";
        } else {
            return String.format(Locale.ROOT, "%.1f%%", percent);
        }
    }
}
