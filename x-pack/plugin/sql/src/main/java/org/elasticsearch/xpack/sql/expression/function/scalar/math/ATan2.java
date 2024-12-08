/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryMathProcessor.BinaryMathOperation;

/**
 * <a href="https://en.wikipedia.org/wiki/Atan2">Multi-valued inverse tangent</a>
 * function.
 */
public class ATan2 extends BinaryNumericFunction {

    public ATan2(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryMathOperation.ATAN2);
    }

    @Override
    protected NodeInfo<ATan2> info() {
        return NodeInfo.create(this, ATan2::new, left(), right());
    }

    @Override
    protected ATan2 replaceChildren(Expression newLeft, Expression newRight) {
        return new ATan2(source(), newLeft, newRight);
    }
}
