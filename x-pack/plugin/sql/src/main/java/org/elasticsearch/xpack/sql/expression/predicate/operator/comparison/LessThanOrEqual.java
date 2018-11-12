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

public class LessThanOrEqual extends BinaryComparison implements Negatable<BinaryComparison> {

    public LessThanOrEqual(Location location, Expression left, Expression right) {
        super(location, left, right, BinaryComparisonOperation.LTE);
    }

    @Override
    protected NodeInfo<LessThanOrEqual> info() {
        return NodeInfo.create(this, LessThanOrEqual::new, left(), right());
    }

    @Override
    protected LessThanOrEqual replaceChildren(Expression newLeft, Expression newRight) {
        return new LessThanOrEqual(location(), newLeft, newRight);
    }

    @Override
    public GreaterThanOrEqual swapLeftAndRight() {
        return new GreaterThanOrEqual(location(), right(), left());
    }

    @Override
    public GreaterThan negate() {
        return new GreaterThan(location(), left(), right());
    }
}
