/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.wildcard.mapper.regex;

import java.util.Set;

/**
 * Conjunction.
 */
public final class And<T> extends AbstractCompositeExpression<T> {
    public And(Set<Expression<T>> components) {
        super(components);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public And(Expression<T>... components) {
        this(Set.of(components));
    }

    @Override
    protected boolean doesNotAffectOutcome(Expression<T> expression) {
        return expression.alwaysTrue();
    }

    @Override
    protected Expression<T> componentForcesOutcome(Expression<T> expression) {
        if (expression.alwaysFalse()) {
            return expression;
        }
        return null;
    }

    @Override
    protected AbstractCompositeExpression<T> newFrom(Set<Expression<T>> components) {
        return new And<>(components);
    }

    @Override
    protected String toStringJoiner() {
        return " AND ";
    }

    @Override
    public <J> J transform(Expression.Transformer<T, J> transformer) {
        return transformer.and(transformComponents(transformer));
    }
}
