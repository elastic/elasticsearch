/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.logical;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.predicate.Negatable;
import org.elasticsearch.xpack.sql.expression.predicate.logical.BinaryLogicProcessor.BinaryLogicOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

public class And extends BinaryLogic implements Negatable<BinaryLogic> {

    public And(Location location, Expression left, Expression right) {
        super(location, left, right, BinaryLogicOperation.AND);
    }

    @Override
    protected NodeInfo<And> info() {
        return NodeInfo.create(this, And::new, left(), right());
    }

    @Override
    protected And replaceChildren(Expression newLeft, Expression newRight) {
        return new And(location(), newLeft, newRight);
    }

    @Override
    public And swapLeftAndRight() {
        return new And(location(), right(), left());
    }

    @Override
    public Or negate() {
        return new Or(location(), new Not(location(), left()), new Not(location(), right()));
    }
}
