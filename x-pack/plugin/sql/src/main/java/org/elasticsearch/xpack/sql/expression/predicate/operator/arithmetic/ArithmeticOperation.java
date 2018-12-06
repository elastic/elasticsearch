/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.BinaryArithmeticProcessor.BinaryArithmeticOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypeConversion;

public abstract class ArithmeticOperation extends BinaryOperator<Object, Object, Object, BinaryArithmeticOperation> {

    private DataType dataType;

    protected ArithmeticOperation(Location location, Expression left, Expression right, BinaryArithmeticOperation operation) {
        super(location, left, right, operation);
    }
    
    @Override
    protected TypeResolution resolveInputType(Expression e, Expressions.ParamOrdinal paramOrdinal) {
        return Expressions.typeMustBeNumeric(e, symbol(), paramOrdinal);
    }

    @Override
    public ArithmeticOperation swapLeftAndRight() {
        return this;
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            dataType = DataTypeConversion.commonType(left().dataType(), right().dataType());
        }
        return dataType;
    }

    @Override
    protected Pipe makePipe() {
        return new BinaryArithmeticPipe(location(), this, Expressions.pipe(left()), Expressions.pipe(right()), function());
    }
}