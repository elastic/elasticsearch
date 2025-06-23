/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal;
import org.elasticsearch.xpack.esql.core.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.BinaryArithmeticOperation;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.commonType;

public abstract class ArithmeticOperation extends BinaryOperator<Object, Object, Object, BinaryArithmeticOperation> {

    private DataType dataType;

    protected ArithmeticOperation(Source source, Expression left, Expression right, BinaryArithmeticOperation operation) {
        super(source, left, right, operation);
    }

    @Override
    protected TypeResolution resolveInputType(Expression e, ParamOrdinal paramOrdinal) {
        return isNumeric(e, sourceText(), paramOrdinal);
    }

    @Override
    public ArithmeticOperation swapLeftAndRight() {
        return this;
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            dataType = commonType(left().dataType(), right().dataType());
        }
        return dataType;
    }
}
