/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.rule;

import org.elasticsearch.xpack.ql.tree.Node;

public abstract class ParameterizedRule<E extends T, T extends Node<T>, P> extends Rule<E, T> {

    public abstract T apply(T t, P p);

    public T apply(T t) {
        throw new RuleExecutionException("Cannot call parameterized rule without parameter");
    }
}
