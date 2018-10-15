/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.BinaryArithmeticProcessor.BinaryArithmeticOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypeConversion;

/**
 * Division function ({@code a / b}).
 */
public class Div extends ArithmeticOperation {

    public Div(Location location, Expression left, Expression right) {
        super(location, left, right, BinaryArithmeticOperation.DIV);
    }

    @Override
    protected NodeInfo<Div> info() {
        return NodeInfo.create(this, Div::new, left(), right());
    }

    @Override
    protected Div replaceChildren(Expression newLeft, Expression newRight) {
        return new Div(location(), newLeft, newRight);
    }

    @Override
    public DataType dataType() {
        return DataTypeConversion.commonType(left().dataType(), right().dataType());
    }
}
