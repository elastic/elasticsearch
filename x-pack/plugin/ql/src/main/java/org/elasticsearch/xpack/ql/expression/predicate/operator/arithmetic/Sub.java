/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * Subtraction function ({@code a - b}).
 */
public class Sub extends DateTimeArithmeticOperation {

    public Sub(Source source, Expression left, Expression right) {
        super(source, left, right, DefaultBinaryArithmeticOperation.SUB);
    }

    @Override
    protected NodeInfo<Sub> info() {
        return NodeInfo.create(this, Sub::new, left(), right());
    }

    @Override
    protected Sub replaceChildren(Expression newLeft, Expression newRight) {
        return new Sub(source(), newLeft, newRight);
    }
}
