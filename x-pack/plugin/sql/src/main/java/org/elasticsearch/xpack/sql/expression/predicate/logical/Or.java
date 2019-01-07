/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.logical;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.predicate.Negatable;
import org.elasticsearch.xpack.sql.expression.predicate.logical.BinaryLogicProcessor.BinaryLogicOperation;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

public class Or extends BinaryLogic implements Negatable<BinaryLogic> {

    public Or(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryLogicOperation.OR);
    }

    @Override
    protected NodeInfo<Or> info() {
        return NodeInfo.create(this, Or::new, left(), right());
    }

    @Override
    protected Or replaceChildren(Expression newLeft, Expression newRight) {
        return new Or(source(), newLeft, newRight);
    }

    @Override
    public Or swapLeftAndRight() {
        return new Or(source(), right(), left());
    }

    @Override
    public And negate() {
        return new And(source(), new Not(source(), left()), new Not(source(), right()));
    }
}
