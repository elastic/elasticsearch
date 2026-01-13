/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.tree;

import org.elasticsearch.test.ESTestCase;

import java.util.function.Function;

/**
 * Shim to expose protected methods to ESQL proper's NodeSubclassTests.
 */
public class NodeSubclassTests extends ESTestCase {

    // TODO once Node has been move to ESQL proper remove this shim and these methods.
    protected final NodeInfo<?> info(Node<?> node) {
        return node.info();
    }

    protected final <E, T extends Node<T>> T transformNodeProps(Node<T> n, Class<E> typeToken, Function<? super E, ? extends E> rule) {
        return n.transformNodeProps(typeToken, rule);
    }
}
