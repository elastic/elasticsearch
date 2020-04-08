/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.wildcard.mapper.regex;

/**
 * Always false.
 */
public class False<T> implements Expression<T> {
    private static final False<Object> TRUE = new False<>();

    /**
     * There is only one false.
     */
    @SuppressWarnings("unchecked")
    public static <T> False<T> instance() {
        return (False<T>) TRUE;
    }

    private False() {
        // Only one instance
    }

    public String toString() {
        return "FALSE";
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
        return true;
    }

    @Override
    public boolean isComposite() {
        return false;
    }

    @Override
    public <J> J transform(Expression.Transformer<T, J> transformer) {
        return transformer.alwaysFalse();
    }
}
