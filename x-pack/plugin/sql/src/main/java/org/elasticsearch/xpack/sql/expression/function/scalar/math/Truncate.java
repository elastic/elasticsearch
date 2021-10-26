/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryOptionalMathProcessor.BinaryOptionalMathOperation;

import java.util.List;

/**
 * Function that takes two parameters: one is the field/value itself, the other is a non-floating point numeric
 * which indicates how the truncation should behave. If positive, it will truncate the number till that
 * parameter count digits after the decimal point. If negative, it will truncate the number till that parameter
 * count digits before the decimal point, starting at the decimal point.
 */
public class Truncate extends BinaryOptionalNumericFunction implements OptionalArgument {

    public Truncate(Source source, Expression left, Expression right) {
        super(source, left, right);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Truncate::new, left(), right());
    }

    @Override
    protected BinaryOptionalMathOperation operation() {
        return BinaryOptionalMathOperation.TRUNCATE;
    }

    @Override
    protected final Expression replacedChildrenInstance(List<Expression> newChildren) {
        return new Truncate(source(), newChildren.get(0), right() == null ? null : newChildren.get(1));
    }
}
