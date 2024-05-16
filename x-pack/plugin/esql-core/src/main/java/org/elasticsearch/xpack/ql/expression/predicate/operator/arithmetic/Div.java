/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypeConverter;

/**
 * Division function ({@code a / b}).
 */
public class Div extends ArithmeticOperation implements BinaryComparisonInversible {

    private DataType dataType;

    public Div(Source source, Expression left, Expression right) {
        this(source, left, right, null);
    }

    public Div(Source source, Expression left, Expression right, DataType dataType) {
        super(source, left, right, DefaultBinaryArithmeticOperation.DIV);
        this.dataType = dataType;
    }

    @Override
    protected NodeInfo<Div> info() {
        return NodeInfo.create(this, Div::new, left(), right(), dataType);
    }

    @Override
    protected Div replaceChildren(Expression newLeft, Expression newRight) {
        return new Div(source(), newLeft, newRight, dataType);
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            dataType = DataTypeConverter.commonType(left().dataType(), right().dataType());
        }
        return dataType;
    }

    @Override
    public ArithmeticOperationFactory binaryComparisonInverse() {
        return Mul::new;
    }
}
