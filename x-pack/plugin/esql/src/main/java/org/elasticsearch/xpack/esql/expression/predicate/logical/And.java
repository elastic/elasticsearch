/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.Negatable;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;

import java.io.IOException;

public class And extends BinaryLogic implements Negatable<BinaryLogic> {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "And", And::new);

    public And(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryLogicOperation.AND);
    }

    private And(StreamInput in) throws IOException {
        super(in, BinaryLogicOperation.AND);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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
