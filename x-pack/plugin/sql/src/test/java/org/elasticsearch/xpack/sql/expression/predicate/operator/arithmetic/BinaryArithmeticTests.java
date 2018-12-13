/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.Literal;

import static org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Arithmetics.mod;
import static org.elasticsearch.xpack.sql.tree.Location.EMPTY;

public class BinaryArithmeticTests extends ESTestCase {

    public void testAddNumbers() {
        assertEquals(Long.valueOf(3), add(1L, 2L));
    }

    public void testMod() {
        assertEquals(2, mod(10, 8));
        assertEquals(2, mod(10, -8));
        assertEquals(-2, mod(-10, 8));
        assertEquals(-2, mod(-10, -8));

        assertEquals(2L, mod(10L, 8));
        assertEquals(2L, mod(10, -8L));
        assertEquals(-2L, mod(-10L, 8L));
        assertEquals(-2L, mod(-10L, -8L));

        assertEquals(2.3000002f, mod(10.3f, 8L));
        assertEquals(1.5f, mod(10, -8.5f));
        assertEquals(-1.8000002f, mod(-10.3f, 8.5f));
        assertEquals(-1.8000002f, mod(-10.3f, -8.5f));

        assertEquals(2.3000000000000007d, mod(10.3d, 8L));
        assertEquals(1.5d, mod(10, -8.5d));
        assertEquals(-1.8000001907348633d, mod(-10.3f, 8.5d));
        assertEquals(-1.8000000000000007, mod(-10.3d, -8.5d));
    }

    @SuppressWarnings("unchecked")
    private static <T> T add(Object l, Object r) {
        Add add = new Add(EMPTY, L(l), L(r));
        assertTrue(add.foldable());
        return (T) add.fold();
    }

    private static Literal L(Object value) {
        return Literal.of(EMPTY, value);
    }
}
