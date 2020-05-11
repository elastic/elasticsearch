/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypeConverter;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

public abstract class ArithmeticOperation extends BinaryOperator<Object, Object, Object, BinaryArithmeticOperation> {

    private DataType dataType;

    protected ArithmeticOperation(Source source, Expression left, Expression right, BinaryArithmeticOperation operation) {
        super(source, left, right, operation);
    }
    
    @Override
    protected TypeResolution resolveInputType(Expression e, Expressions.ParamOrdinal paramOrdinal) {
        return isNumeric(e, sourceText(), paramOrdinal);
    }

    @Override
    public ArithmeticOperation swapLeftAndRight() {
        return this;
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            dataType = DataTypeConverter.commonType(left().dataType(), right().dataType());
        }
        return dataType;
    }

    @Override
    protected Pipe makePipe() {
        return new BinaryArithmeticPipe(source(), this, Expressions.pipe(left()), Expressions.pipe(right()), function());
    }
}
