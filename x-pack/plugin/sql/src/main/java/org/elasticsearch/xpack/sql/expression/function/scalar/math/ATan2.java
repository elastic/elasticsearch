/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryMathProcessor.BinaryMathOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

/**
 * <a href="https://en.wikipedia.org/wiki/Atan2">Multi-valued inverse tangent</a>
 * function.
 */
public class ATan2 extends BinaryNumericFunction {

    public ATan2(Location location, Expression left, Expression right) {
        super(location, left, right, BinaryMathOperation.ATAN2);
    }

    @Override
    protected NodeInfo<ATan2> info() {
        return NodeInfo.create(this, ATan2::new, left(), right());
    }

    @Override
    protected ATan2 replaceChildren(Expression newLeft, Expression newRight) {
        return new ATan2(location(), newLeft, newRight);
    }
}
