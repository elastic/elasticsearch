/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.rule;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.ql.tree.Node;
import org.elasticsearch.xpack.ql.util.ReflectionUtils;

/**
 * Rules that apply transformation to a tree. In addition, performs
 * type filtering so that a rule that the rule implementation doesn't
 * have to manually filter.
 * <p>
 * Rules <strong>could</strong> could be built as lambdas but most
 * rules are much larger, so we keep them as full-blown subclasses.
 */
public abstract class Rule<E extends T, T extends Node<T>> {

    protected Logger log = LogManager.getLogger(getClass());

    private final String name;
    private final Class<E> typeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());

    protected Rule() {
        this(null);
    }

    protected Rule(String name) {
        this.name = (name == null ? ReflectionUtils.ruleLikeNaming(getClass()) : name);
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

    public abstract T apply(T t);
}
