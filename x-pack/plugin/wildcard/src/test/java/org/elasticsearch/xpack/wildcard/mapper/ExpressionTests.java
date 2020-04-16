/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.wildcard.mapper.regex.And;
import org.elasticsearch.xpack.wildcard.mapper.regex.False;
import org.elasticsearch.xpack.wildcard.mapper.regex.Leaf;
import org.elasticsearch.xpack.wildcard.mapper.regex.Or;
import org.elasticsearch.xpack.wildcard.mapper.regex.True;

public class ExpressionTests extends ESTestCase {
    private final Leaf<String> foo = new Leaf<>("foo");
    private final Leaf<String> bar = new Leaf<>("bar");
    private final Leaf<String> baz = new Leaf<>("baz");

    public void testSimple() {
        assertEquals(True.instance(), True.instance());
        assertEquals(True.instance().hashCode(), True.instance().hashCode());
        assertEquals(False.instance(), False.instance());
        assertEquals(False.instance().hashCode(), False.instance().hashCode());
        assertNotEquals(True.instance(), False.instance());
        assertNotEquals(False.instance(), True.instance());

        Leaf<String> leaf = new Leaf<>("foo");
        assertEquals(leaf, leaf);
        assertNotEquals(True.instance(), leaf);
        assertNotEquals(False.instance(), leaf);
    }

    public void testExtract() {
        assertEquals(new And<>(foo, new Or<>(bar, baz)),
                new Or<>(
                        new And<>(foo, bar),
                        new And<>(foo, baz)
                ).simplify());
    }

    public void testExtractToEmpty() {
        assertEquals(new And<>(foo, bar),
                new Or<>(
                        new And<>(foo, bar),
                        new And<>(foo, bar, baz)
                ).simplify());
    }

    public void testExtractSingle() {
        assertEquals(foo,
                new Or<>(
                        new And<>(foo, bar),
                        foo
                ).simplify());
    }

    public void testExtractDuplicates() {
        assertEquals(foo, new Or<>(foo, new And<>(foo)).simplify());
    }
}
