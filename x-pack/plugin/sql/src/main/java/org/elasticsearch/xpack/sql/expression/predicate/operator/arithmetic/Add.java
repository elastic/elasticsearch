/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.BinaryComparisonInversible;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * Addition function ({@code a + b}).
 */
public class Add extends DateTimeArithmeticOperation implements BinaryComparisonInversible {
    public Add(Source source, Expression left, Expression right) {
        super(source, left, right, SqlBinaryArithmeticOperation.ADD);
    }

    @Override
    protected NodeInfo<Add> info() {
        return NodeInfo.create(this, Add::new, left(), right());
    }

    @Override
    protected Add replaceChildren(Expression left, Expression right) {
        return new Add(source(), left, right);
    }

    @Override
    public Add swapLeftAndRight() {
        return new Add(source(), right(), left());
    }

    @Override
    public ArithmeticOperationFactory binaryComparisonInverse() {
        return Sub::new;
    }

    @Override
    protected boolean isCommutative() {
        return true;
    }
}
