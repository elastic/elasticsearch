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

public class Equals extends BinaryComparison implements Negatable<BinaryComparison> {

    public Equals(Location location, Expression left, Expression right) {
        super(location, left, right, BinaryComparisonOperation.EQ);
    }

    @Override
    protected NodeInfo<Equals> info() {
        return NodeInfo.create(this, Equals::new, left(), right());
    }

    @Override
    protected Equals replaceChildren(Expression newLeft, Expression newRight) {
        return new Equals(location(), newLeft, newRight);
    }

    @Override
    public Equals swapLeftAndRight() {
        return new Equals(location(), right(), left());
    }

    @Override
    public BinaryComparison negate() {
        return new NotEquals(location(), left(), right());
    }
}
