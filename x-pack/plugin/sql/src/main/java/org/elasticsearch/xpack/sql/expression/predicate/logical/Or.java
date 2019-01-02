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

public class Or extends BinaryLogic implements Negatable<BinaryLogic> {

    public Or(Location location, Expression left, Expression right) {
        super(location, left, right, BinaryLogicOperation.OR);
    }

    @Override
    protected NodeInfo<Or> info() {
        return NodeInfo.create(this, Or::new, left(), right());
    }

    @Override
    protected Or replaceChildren(Expression newLeft, Expression newRight) {
        return new Or(location(), newLeft, newRight);
    }

    @Override
    public Or swapLeftAndRight() {
        return new Or(location(), right(), left());
    }

    @Override
    public And negate() {
        return new And(location(), new Not(location(), left()), new Not(location(), right()));
    }
}
