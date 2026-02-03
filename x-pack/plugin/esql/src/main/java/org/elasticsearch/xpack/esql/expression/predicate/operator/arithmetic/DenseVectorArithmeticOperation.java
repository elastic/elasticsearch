/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;

/**
 * Adds support for dense_vector data types. Specifically provides the logic when either left or right type is a dense_vector.
 */
public abstract class DenseVectorArithmeticOperation extends EsqlArithmeticOperation {
    private final BinaryEvaluator denseVectors;

    protected DenseVectorArithmeticOperation(
        Source source,
        Expression left,
        Expression right,
        OperationSymbol op,
        BinaryEvaluator ints,
        BinaryEvaluator longs,
        BinaryEvaluator ulongs,
        BinaryEvaluator doubles,
        BinaryEvaluator denseVectors
    ) {
        super(source, left, right, op, ints, longs, ulongs, doubles);
        this.denseVectors = denseVectors;
    }

    DenseVectorArithmeticOperation(
        StreamInput in,
        OperationSymbol op,
        BinaryEvaluator ints,
        BinaryEvaluator longs,
        BinaryEvaluator ulongs,
        BinaryEvaluator doubles,
        BinaryEvaluator denseVectors
    ) throws IOException {
        super(in, op, ints, longs, ulongs, doubles);
        this.denseVectors = denseVectors;
    }

    @Override
    protected TypeResolution resolveInputType(Expression e, TypeResolutions.ParamOrdinal paramOrdinal) {
        return TypeResolutions.isType(
            e,
            t -> t.isNumeric() || t == DENSE_VECTOR || DataType.isNull(t),
            sourceText(),
            paramOrdinal,
            "numeric",
            "dense_vector"
        );
    }

    @Override
    protected TypeResolution checkCompatibility() {
        // dense_vectors arithmetic only supported when both arguments are dense_vectors or one argument is null
        DataType leftType = left().dataType();
        DataType rightType = right().dataType();
        if (leftType == DENSE_VECTOR || rightType == DENSE_VECTOR) {
            if ((leftType == DENSE_VECTOR || leftType == NULL) && (rightType == DENSE_VECTOR || rightType == NULL)) {
                return TypeResolution.TYPE_RESOLVED;
            }
            return new TypeResolution(formatIncompatibleTypesMessage(symbol(), leftType, rightType));
        }
        return super.checkCompatibility();
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (dataType() == DENSE_VECTOR) {
            return this.denseVectors.apply(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
        }
        return super.toEvaluator(toEvaluator);
    }
}
