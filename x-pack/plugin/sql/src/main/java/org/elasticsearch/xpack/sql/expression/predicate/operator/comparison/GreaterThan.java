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

public class GreaterThan extends BinaryComparison implements Negatable<BinaryComparison> {

    public GreaterThan(Location location, Expression left, Expression right) {
        super(location, left, right, BinaryComparisonOperation.GT);
    }

    @Override
    protected NodeInfo<GreaterThan> info() {
        return NodeInfo.create(this, GreaterThan::new, left(), right());
    }

    @Override
    protected GreaterThan replaceChildren(Expression newLeft, Expression newRight) {
        return new GreaterThan(location(), newLeft, newRight);
    }

    @Override
    public LessThan swapLeftAndRight() {
        return new LessThan(location(), right(), left());
    }

    @Override
    public LessThanOrEqual negate() {
        return new LessThanOrEqual(location(), left(), right());
    }
}
