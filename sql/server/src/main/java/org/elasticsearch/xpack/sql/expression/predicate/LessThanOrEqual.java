/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.xpack.sql.expression.BinaryOperator.Negateable;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

public class LessThanOrEqual extends BinaryComparison implements Negateable {

    public LessThanOrEqual(Location location, Expression left, Expression right) {
        super(location, left, right);
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
    public Object fold() {
        Integer compare = compare(left().fold(), right().fold());
        return compare != null && compare.intValue() <= 0;
    }

    @Override
    public GreaterThanOrEqual swapLeftAndRight() {
        return new GreaterThanOrEqual(location(), right(), left());
    }

    @Override
    public GreaterThan negate() {
        return new GreaterThan(location(), left(), right());
    }

    @Override
    public String symbol() {
        return "<=";
    }
}
