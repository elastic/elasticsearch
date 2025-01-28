/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;

/**
 * Common base for trigonometric functions.
 */
abstract class AbstractTrigonometricFunction extends UnaryScalarFunction {
    AbstractTrigonometricFunction(Source source, Expression field) {
        super(source, field);
    }

    protected AbstractTrigonometricFunction(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Build an evaluator for this function given the evaluator for it's input.
     */
    protected abstract EvalOperator.ExpressionEvaluator.Factory doubleEvaluator(EvalOperator.ExpressionEvaluator.Factory field);

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return doubleEvaluator(Cast.cast(source(), field().dataType(), DataType.DOUBLE, toEvaluator.apply(field())));
    }

    @Override
    protected final TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isNumeric(field, sourceText(), DEFAULT);
    }

    @Override
    public final DataType dataType() {
        return DataType.DOUBLE;
    }
}
