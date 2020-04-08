/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.wildcard.mapper.regex;

/**
 * Always true.
 */
public class True<T> implements Expression<T> {
    private static final True<Object> TRUE = new True<>();

    /**
     * There is only one True.
     */
    @SuppressWarnings("unchecked")
    public static <T> True<T> instance() {
        return (True<T>) TRUE;
    }

    private True() {
        // Only one copy
    }

    public String toString() {
        return "TRUE";
    }

    @Override
    public Expression<T> simplify() {
        return this;
    }

    @Override
    public boolean alwaysTrue() {
        return true;
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
        return transformer.alwaysTrue();
    }
}
