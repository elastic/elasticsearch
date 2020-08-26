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
 * Addition function ({@code a + b}).
 */
public class Add extends DateTimeArithmeticOperation {
    public Add(Source source, Expression left, Expression right) {
        super(source, left, right, DefaultBinaryArithmeticOperation.ADD);
    }

    @Override
    protected NodeInfo<Add> info() {
        return NodeInfo.create(this, Add::new, left(), right());
    }

    @Override
    protected Add replaceChildren(Expression left, Expression right) {
        return new Add(source(), left, right);
    }
}
