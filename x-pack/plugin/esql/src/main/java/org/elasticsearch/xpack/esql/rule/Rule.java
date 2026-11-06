/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.rule;

import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.util.ReflectionUtils;

/**
 * Rules that apply transformation to a tree. In addition, performs
 * type filtering so that a rule that the rule implementation doesn't
 * have to manually filter.
 * <p>
 * Rules <strong>could</strong> could be built as lambdas but most
 * rules are much larger, so we keep them as full-blown subclasses.
 */
public abstract class Rule<E extends T, T extends Node<T>> {

    /**
     * The type token and default name of a rule class are derived through reflection on the generic superclass, which is slow
     * enough to show up when optimizers build their rule lists per query (the local optimizers do, on every data node request).
     * Cache them per class.
     */
    private static final ClassValue<Class<?>> TYPE_TOKENS = new ClassValue<>() {
        @Override
        protected Class<?> computeValue(Class<?> type) {
            return ReflectionUtils.detectSuperTypeForRuleLike(type);
        }
    };
    private static final ClassValue<String> NAMES = new ClassValue<>() {
        @Override
        protected String computeValue(Class<?> type) {
            return ReflectionUtils.ruleLikeNaming(type);
        }
    };

    private final String name;
    @SuppressWarnings("unchecked")
    private final Class<E> typeToken = (Class<E>) TYPE_TOKENS.get(getClass());

    protected Rule() {
        this(null);
    }

    protected Rule(String name) {
        this.name = (name == null ? NAMES.get(getClass()) : name);
    }

    public Class<E> typeToken() {
        return typeToken;
    }

    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return name();
    }

    /**
     * Whether {@link #apply} only ever rewrites nodes that are instances of {@link #typeToken()} (a typed {@code transformDown} or
     * {@code transformUp}), so that it is a no-op on a tree without such a node. The {@link RuleExecutor} skips such rules
     * when the tree holds no matching node, which is most rules of a batch for a small plan.
     */
    public boolean appliesOnlyToTypeToken() {
        return false;
    }

    /**
     * The expression class this rule rewrites when it is a typed expression transform over a query plan, or {@code null}: the
     * {@link RuleExecutor} skips the rule when no expression of the plan is an instance of it.
     */
    public Class<?> expressionTypeToken() {
        return null;
    }

    public abstract T apply(T t);
}
