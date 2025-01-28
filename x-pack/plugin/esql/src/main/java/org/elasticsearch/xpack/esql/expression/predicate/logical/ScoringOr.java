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

public class ScoringOr extends BinaryScoringLogic implements Negatable<BinaryLogic> {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ScoringOr",
        ScoringOr::new
    );

    public ScoringOr(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryScoringLogicOperation.OR);
    }

    private ScoringOr(StreamInput in) throws IOException {
        super(in, BinaryScoringLogicOperation.OR);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<ScoringOr> info() {
        return NodeInfo.create(this, ScoringOr::new, left(), right());
    }

    @Override
    protected ScoringOr replaceChildren(Expression newLeft, Expression newRight) {
        return new ScoringOr(source(), newLeft, newRight);
    }

    @Override
    public ScoringOr swapLeftAndRight() {
        return new ScoringOr(source(), right(), left());
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
