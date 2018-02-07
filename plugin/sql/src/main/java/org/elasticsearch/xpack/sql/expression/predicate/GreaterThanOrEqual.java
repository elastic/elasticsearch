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

public class GreaterThanOrEqual extends BinaryComparison implements Negateable {

    public GreaterThanOrEqual(Location location, Expression left, Expression right) {
        super(location, left, right);
    }

    @Override
    protected NodeInfo<GreaterThanOrEqual> info() {
        return NodeInfo.create(this, GreaterThanOrEqual::new, left(), right());
    }

    @Override
    protected GreaterThanOrEqual replaceChildren(Expression newLeft, Expression newRight) {
        return new GreaterThanOrEqual(location(), newLeft, newRight);
    }

    public Object fold() {
        Integer compare = compare(left().fold(), right().fold());
        return compare != null && compare.intValue() >= 0;
    }

    @Override
    public LessThanOrEqual swapLeftAndRight() {
        return new LessThanOrEqual(location(), right(), left());
    }

    @Override
    public LessThan negate() {
        return new LessThan(location(), left(), right());
    }

    @Override
    public String symbol() {
        return ">=";
    }
}
