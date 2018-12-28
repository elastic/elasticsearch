/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryMathProcessor.BinaryMathOperation;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

public class Power extends BinaryNumericFunction {

    public Power(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryMathOperation.POWER);
    }

    @Override
    protected NodeInfo<Power> info() {
        return NodeInfo.create(this, Power::new, left(), right());
    }

    @Override
    protected Power replaceChildren(Expression newLeft, Expression newRight) {
        return new Power(source(), newLeft, newRight);
    }
}
