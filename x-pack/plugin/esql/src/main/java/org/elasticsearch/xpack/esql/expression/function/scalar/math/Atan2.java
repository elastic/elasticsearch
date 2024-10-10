/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
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
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;

/**
 * Inverse cosine trigonometric function.
 */
public class Atan2 extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Atan2", Atan2::new);

    private final Expression y;
    private final Expression x;

    @FunctionInfo(
        returnType = "double",
        description = "The {wikipedia}/Atan2[angle] between the positive x-axis and the ray from the\n"
            + "origin to the point (x , y) in the Cartesian plane, expressed in radians.",
        examples = @Example(file = "floats", tag = "atan2")
    )
    public Atan2(
        Source source,
        @Param(
            name = "y_coordinate",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "y coordinate. If `null`, the function returns `null`."
        ) Expression y,
        @Param(
            name = "x_coordinate",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "x coordinate. If `null`, the function returns `null`."
        ) Expression x
    ) {
        super(source, List.of(y, x));
        this.y = y;
        this.x = x;
    }

    private Atan2(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(y);
        out.writeNamedWriteable(x);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Atan2(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Atan2::new, y, x);
    }

    @Evaluator
    static double process(double y, double x) {
        return Math.atan2(y, x);
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isNumeric(y, sourceText(), TypeResolutions.ParamOrdinal.FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        return isNumeric(x, sourceText(), TypeResolutions.ParamOrdinal.SECOND);
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var yEval = Cast.cast(source(), y.dataType(), DataType.DOUBLE, toEvaluator.apply(y));
        var xEval = Cast.cast(source(), x.dataType(), DataType.DOUBLE, toEvaluator.apply(x));
        return new Atan2Evaluator.Factory(source(), yEval, xEval);
    }

    public Expression y() {
        return y;
    }

    public Expression x() {
        return x;
    }
}
