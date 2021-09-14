/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.comparison;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.TestUtils;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.In;

import java.util.Arrays;

import static org.elasticsearch.xpack.ql.expression.Literal.NULL;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class InTests extends ESTestCase {

    private static final Literal ONE = L(1);
    private static final Literal TWO = L(2);
    private static final Literal THREE = L(3);

    public void testInWithContainedValue() {
        In in = new In(EMPTY, TWO, Arrays.asList(ONE, TWO, THREE));
        assertTrue(in.fold());
    }

    public void testInWithNotContainedValue() {
        In in = new In(EMPTY, THREE, Arrays.asList(ONE, TWO));
        assertFalse(in.fold());
    }

    public void testHandleNullOnLeftValue() {
        In in = new In(EMPTY, NULL, Arrays.asList(ONE, TWO, THREE));
        assertNull(in.fold());
        in = new In(EMPTY, NULL, Arrays.asList(ONE, NULL, THREE));
        assertNull(in.fold());

    }

    public void testHandleNullsOnRightValue() {
        In in = new In(EMPTY, THREE, Arrays.asList(ONE, NULL, THREE));
        assertTrue(in.fold());
        in = new In(EMPTY, ONE, Arrays.asList(TWO, NULL, THREE));
        assertNull(in.fold());
    }

    private static Literal L(Object value) {
        return TestUtils.of(EMPTY, value);
    }
}
