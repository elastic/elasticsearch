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
        return new And(source(), Not.negate(left()), Not.negate(right()));
    }

    @Override
    protected Expression canonicalize() {
        // NB: this add a circular dependency between Predicates / Logical package
        return Predicates.combineOr(Predicates.splitOr(super.canonicalize()));
    }
}
