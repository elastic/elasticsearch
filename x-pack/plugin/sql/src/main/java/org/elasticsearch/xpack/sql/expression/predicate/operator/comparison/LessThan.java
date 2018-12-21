/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.predicate.Negatable;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

public class LessThan extends BinaryComparison implements Negatable<BinaryComparison> {

    public LessThan(Location location, Expression left, Expression right) {
        super(location, left, right, BinaryComparisonOperation.LT);
    }

    @Override
    protected NodeInfo<LessThan> info() {
        return NodeInfo.create(this, LessThan::new, left(), right());
    }

    @Override
    protected LessThan replaceChildren(Expression newLeft, Expression newRight) {
        return new LessThan(location(), newLeft, newRight);
    }

    @Override
    public GreaterThan swapLeftAndRight() {
        return new GreaterThan(location(), right(), left());
    }

    @Override
    public GreaterThanOrEqual negate() {
        return new GreaterThanOrEqual(location(), left(), right());
    }
}
