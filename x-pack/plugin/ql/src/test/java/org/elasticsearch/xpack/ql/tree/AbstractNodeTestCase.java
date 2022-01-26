/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.tree;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.List;

/**
 * Superclass for tests of subclasses of {@link Node}.
 */
public abstract class AbstractNodeTestCase<T extends B, B extends Node<B>> extends ESTestCase {
    /**
     * Make a new random instance.
     */
    protected abstract T randomInstance();

    /**
     * Mutate an instance into some other similar instance that
     * shouldn't be {@link #equals} to the original.
     */
    protected abstract T mutate(T instance);

    /**
     * Copy and instance so it isn't {@code ==} but should still
     * be {@link #equals}.
     */
    protected abstract T copy(T instance);

    /**
     * Test this subclass's implementation of {@link Node#transformNodeProps}.
     */
    public abstract void testTransform();

    /**
     * Test this subclass's implementation of {@link Node#replaceChildren(List)}.
     */
    public abstract void testReplaceChildren();

    public final void testHashCodeAndEquals() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(randomInstance(), this::copy, this::mutate);
    }
}
