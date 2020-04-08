/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.wildcard.mapper.regex;


import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A leaf expression.
 */
public final class Leaf<T> implements Expression<T> {
    @SafeVarargs
    public static final <T> Set<Expression<T>> leaves(T... ts) {
        Set<Expression<T>> builder = new HashSet<>();
        for (T t : ts) {
            builder.add(new Leaf<T>(t));
        }
        return Collections.unmodifiableSet(builder);
    }

    private final T t;
    private int hashCode;

    public Leaf(T t) {
        if (t == null) {
            throw new IllegalArgumentException("Leaf value cannot be null");
        }
        this.t = t;
    }

    public String toString() {
        return t.toString();
    }

    @Override
    public Expression<T> simplify() {
        return this;
    }

    @Override
    public boolean alwaysTrue() {
        return false;
    }

    @Override
    public boolean alwaysFalse() {
        return false;
    }

    @Override
    public boolean isComposite() {
        return false;
    }

    @Override
    public <J> J transform(Expression.Transformer<T, J> transformer) {
        return transformer.leaf(t);
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = t.hashCode();
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        @SuppressWarnings("rawtypes")
        Leaf other = (Leaf) obj;
        return t.equals(other.t);
    }
}
