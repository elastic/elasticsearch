/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.logical;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.Negatable;
import org.elasticsearch.xpack.ql.expression.predicate.Predicates;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogicProcessor.BinaryLogicOperation;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

public class And extends BinaryLogic implements Negatable<BinaryLogic> {

    public And(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryLogicOperation.AND);
    }

    @Override
    protected NodeInfo<And> info() {
        return NodeInfo.create(this, And::new, left(), right());
    }

    @Override
    protected And replaceChildren(Expression newLeft, Expression newRight) {
        return new And(source(), newLeft, newRight);
    }

    @Override
    public And swapLeftAndRight() {
        return new And(source(), right(), left());
    }

    @Override
    public Or negate() {
        return new Or(source(), Not.negate(left()), Not.negate(right()));
    }

    @Override
    protected Expression canonicalize() {
        // NB: this add a circular dependency between Predicates / Logical package
        return Predicates.combineAnd(Predicates.splitAnd(super.canonicalize()));
    }
}
