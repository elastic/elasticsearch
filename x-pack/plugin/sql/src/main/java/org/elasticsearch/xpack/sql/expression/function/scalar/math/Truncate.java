/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryMathProcessor.BinaryMathOperation;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

/**
 * Function that takes two parameters: one is the field/value itself, the other is a non-floating point numeric
 * which indicates how the truncation should behave. If positive, it will truncate the number till that
 * parameter count digits after the decimal point. If negative, it will truncate the number till that parameter
 * count digits before the decimal point, starting at the decimal point.
 */
public class Truncate extends BinaryNumericFunction {
    
    public Truncate(Source source, Expression left, Expression right) {
        super(source, left, right == null ? Literal.of(left.source(), 0) : right, BinaryMathOperation.TRUNCATE);
    }

    @Override
    protected NodeInfo<Truncate> info() {
        return NodeInfo.create(this, Truncate::new, left(), right());
    }

    @Override
    protected Truncate replaceChildren(Expression newLeft, Expression newRight) {
        return new Truncate(source(), newLeft, newRight);
    }

    @Override
    public DataType dataType() {
        return left().dataType();
    }
}
