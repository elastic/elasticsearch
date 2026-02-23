/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.isNullOrNumeric;

/**
 * Adds support for dense_vector data types. Specifically provides the logic when either left or right type is a dense_vector.
 */
public abstract class DenseVectorArithmeticOperation extends EsqlArithmeticOperation {
    private static final String ERROR_MSG = "[{}] should evaluate to a dense_vector or scalar constant";

    private final DenseVectorBinaryEvaluator denseVectors;

    /** Set of arithmetic (quad) functions for dense_vectors. */
    public interface DenseVectorBinaryEvaluator {
        // when both arguments are dense_vectors
        EvalOperator.ExpressionEvaluator.Factory vectorsOperation(
            Source source,
            EvalOperator.ExpressionEvaluator.Factory lhs,
            EvalOperator.ExpressionEvaluator.Factory rhs
        );

        // when lhs is a scalar and rhs is a dense_vector
        EvalOperator.ExpressionEvaluator.Factory scalarVectorOperation(
            Source source,
            float lhs,
            EvalOperator.ExpressionEvaluator.Factory rhs
        );

        // when lhs is a dense_vector and rhs is a scalar
        EvalOperator.ExpressionEvaluator.Factory vectorScalarOperation(
            Source source,
            EvalOperator.ExpressionEvaluator.Factory lhs,
            float rhs
        );
    }

    protected DenseVectorArithmeticOperation(
        Source source,
        Expression left,
        Expression right,
        OperationSymbol op,
        BinaryEvaluator ints,
        BinaryEvaluator longs,
        BinaryEvaluator ulongs,
        BinaryEvaluator doubles,
        DenseVectorBinaryEvaluator denseVectors
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
        DenseVectorBinaryEvaluator denseVectors
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
        // dense_vectors arithmetic only supported when both arguments are dense_vectors or one argument is numeric or null
        DataType leftType = left().dataType();
        DataType rightType = right().dataType();
        if (leftType == DENSE_VECTOR || rightType == DENSE_VECTOR) {
            if (leftType == NULL || rightType == NULL) {
                return TypeResolution.TYPE_RESOLVED;
            }
            if (leftType != DENSE_VECTOR) {
                return validateScalarOperand(left(), leftType, leftType, rightType);
            }
            if (rightType != DENSE_VECTOR) {
                return validateScalarOperand(right(), rightType, leftType, rightType);
            }
            return TypeResolution.TYPE_RESOLVED;
        }
        return super.checkCompatibility();
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var commonType = dataType();
        if (commonType == DENSE_VECTOR) {
            if (left().dataType() == DENSE_VECTOR && right().dataType() == DENSE_VECTOR) {
                return denseVectors.vectorsOperation(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            }
            if (left().dataType() != DENSE_VECTOR) {
                float lhs = (Float) DataTypeConverter.convert(left().fold(toEvaluator.foldCtx()), FLOAT);
                return denseVectors.scalarVectorOperation(source(), lhs, toEvaluator.apply(right()));
            } else {
                float rhs = (Float) DataTypeConverter.convert(right().fold(toEvaluator.foldCtx()), FLOAT);
                return denseVectors.vectorScalarOperation(source(), toEvaluator.apply(left()), rhs);
            }
        }
        return super.toEvaluator(toEvaluator);
    }

    private TypeResolution validateScalarOperand(Expression operand, DataType operandType, DataType leftType, DataType rightType) {
        if (false == isSupportedScalar(operandType)) {
            return new TypeResolution(formatIncompatibleTypesMessage(symbol(), leftType, rightType));
        }
        if (false == operand.foldable()) {
            return new TypeResolution(LoggerMessageFormat.format(null, ERROR_MSG, operand.sourceText()));
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    private static boolean isSupportedScalar(DataType dataType) {
        return isNullOrNumeric(dataType) && dataType != UNSIGNED_LONG;
    }

}
