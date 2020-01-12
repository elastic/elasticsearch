/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.ql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryMathProcessor.BinaryMathOperation;

import java.util.Objects;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

public abstract class BinaryNumericFunction extends BinaryScalarFunction {

    private final BinaryMathOperation operation;

    BinaryNumericFunction(Source source, Expression left, Expression right, BinaryMathOperation operation) {
        super(source, left, right);
        this.operation = operation;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isNumeric(left(), sourceText(), ParamOrdinal.FIRST);
        if (resolution.unresolved()) {
            return resolution;

        }
        return isNumeric(right(), sourceText(), ParamOrdinal.SECOND);
    }

    @Override
    public Object fold() {
        return operation.apply((Number) left().fold(), (Number) right().fold());
    }

    @Override
    protected Pipe makePipe() {
        return new BinaryMathPipe(source(), this, Expressions.pipe(left()), Expressions.pipe(right()), operation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left(), right(), operation);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        BinaryNumericFunction other = (BinaryNumericFunction) obj;
        return Objects.equals(other.left(), left())
            && Objects.equals(other.right(), right())
            && Objects.equals(other.operation, operation);
    }
}
