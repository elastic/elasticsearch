/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.wildcard.mapper.regex;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Disjunction.
 */
public final class Or<T> extends AbstractCompositeExpression<T> {
    public static <T> Expression<T> fromExpressionSources(List<? extends ExpressionSource<T>> sources) {
        switch (sources.size()) {
        case 0:
            return True.instance();
        case 1:
            return sources.get(0).expression();
        default:
            Set<Expression<T>> or = new HashSet<>();
            for (ExpressionSource<T> source : sources) {
                or.add(source.expression());
            }
            return new Or<>(Collections.unmodifiableSet(or)).simplify();
        }
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public Or(Expression<T>... components) {
        this(Set.of(components));
    }

    public Or(Set<Expression<T>> components) {
        super(components);
    }

    @Override
    protected boolean doesNotAffectOutcome(Expression<T> expression) {
        return expression.alwaysFalse();
    }

    @Override
    protected Expression<T> componentForcesOutcome(Expression<T> expression) {
        if (expression.alwaysTrue()) {
            return expression;
        }
        return null;
    }

    @Override
    protected AbstractCompositeExpression<T> newFrom(Set<Expression<T>> components) {
        return new Or<>(components);
    }

    @Override
    protected String toStringJoiner() {
        return " OR ";
    }

    @Override
    public <J> J transform(Expression.Transformer<T, J> transformer) {
        return transformer.or(transformComponents(transformer));
    }
}
