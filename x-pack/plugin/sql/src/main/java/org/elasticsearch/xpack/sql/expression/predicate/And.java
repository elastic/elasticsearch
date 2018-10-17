/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.predicate.BinaryOperator.Negateable;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.Objects;

public class And extends BinaryLogic implements Negateable {

    public And(Location location, Expression left, Expression right) {
        super(location, left, right, "&&");
    }

    @Override
    protected NodeInfo<And> info() {
        return NodeInfo.create(this, And::new, left(), right());
    }

    @Override
    protected BinaryOperator replaceChildren(Expression newLeft, Expression newRight) {
        return new And(location(), newLeft, newRight);
    }

    @Override
    public Object fold() {
        return Objects.equals(left().fold(), Boolean.TRUE) && Objects.equals(right().fold(), Boolean.TRUE);
    }

    @Override
    public Or negate() {
        return new Or(location(), new Not(location(), left()), new Not(location(), right()));
    }

    @Override
    public And swapLeftAndRight() {
        return new And(location(), right(), left());
    }
}
