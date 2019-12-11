/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryOptionalMathProcessor.BinaryOptionalMathOperation;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;

import java.util.List;

/**
 * Function that takes two parameters: one is the field/value itself, the other is a non-floating point numeric
 * which indicates how the rounding should behave. If positive, it will round the number till that parameter
 * count digits after the decimal point. If negative, it will round the number till that paramter count
 * digits before the decimal point, starting at the decimal point.
 */
public class Round extends BinaryOptionalNumericFunction {
    
    public Round(Source source, Expression left, Expression right) {
        super(source, left, right);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Round::new, left(), right());
    }

    @Override
    protected BinaryOptionalMathOperation operation() {
        return BinaryOptionalMathOperation.ROUND;
    }
    
    @Override
    protected final Expression replacedChildrenInstance(List<Expression> newChildren) {
        return new Round(source(), newChildren.get(0), right() == null ? null : newChildren.get(1));
    }
}
