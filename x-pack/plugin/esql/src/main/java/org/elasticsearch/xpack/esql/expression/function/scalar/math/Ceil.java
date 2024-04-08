/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

/**
 * Round a number up to the nearest integer.
 * <p>
 *     Note that doubles are rounded up to the nearest valid double that is
 *     an integer ala {@link Math#ceil}.
 * </p>
 */
public class Ceil extends UnaryScalarFunction {
    @FunctionInfo(
        returnType = { "double", "integer", "long", "unsigned_long" },
        description = "Round a number up to the nearest integer.",
        note = "This is a noop for `long` (including unsigned) and `integer`. For `double` this picks the closest `double` value to "
            + "the integer similar to {javadoc}/java.base/java/lang/Math.html#ceil(double)[Math.ceil].",
        examples = @Example(file = "math", tag = "ceil")
    )
    public Ceil(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Numeric expression. If `null`, the function returns `null`."
        ) Expression n
    ) {
        super(source, n);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        if (dataType().isInteger()) {
            return toEvaluator.apply(field());
        }
        var fieldEval = toEvaluator.apply(field());
        return new CeilDoubleEvaluator.Factory(source(), fieldEval);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isNumeric(field, sourceText(), DEFAULT);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Ceil(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Ceil::new, field());
    }

    @Evaluator(extraName = "Double")
    static double process(double val) {
        return Math.ceil(val);
    }
}
