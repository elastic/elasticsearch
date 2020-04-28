/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.Negatable;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

public class LessThan extends BinaryComparison implements Negatable<BinaryComparison> {

    public LessThan(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryComparisonOperation.LT);
    }

    @Override
    protected NodeInfo<LessThan> info() {
        return NodeInfo.create(this, LessThan::new, left(), right());
    }

    @Override
    protected LessThan replaceChildren(Expression newLeft, Expression newRight) {
        return new LessThan(source(), newLeft, newRight);
    }

    @Override
    public GreaterThan swapLeftAndRight() {
        return new GreaterThan(source(), right(), left());
    }

    @Override
    public GreaterThanOrEqual negate() {
        return new GreaterThanOrEqual(source(), left(), right());
    }
}
