/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;

import java.util.Arrays;
import java.util.List;

public class MathOperationTests extends ESTestCase {

    public void testAbsLongMax() {
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> MathOperation.ABS.apply(Long.MIN_VALUE));
        assertTrue(ex.getMessage().contains("cannot be negated"));
    }

    public void testAbsIntegerMax() {
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> MathOperation.ABS.apply(Integer.MIN_VALUE));
        assertTrue(ex.getMessage().contains("cannot be negated"));
    }

    public void testAbsShortMax() {
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> MathOperation.ABS.apply(Short.MIN_VALUE));
        assertTrue(ex.getMessage().contains("out of"));
    }

    public void testAbsPreservesType() {
        assertEquals((byte) 42, MathOperation.ABS.apply((byte) -42));
        assertEquals((short) 42, MathOperation.ABS.apply((short) -42));
        assertEquals(42, MathOperation.ABS.apply(-42));
        assertEquals((long) 42, MathOperation.ABS.apply((long) -42));
        assertEquals(42f, MathOperation.ABS.apply(-42f));
        assertEquals(42d, MathOperation.ABS.apply(-42d));
    }

    public void testSignIntegerType() {
        List<Number> negative = Arrays.asList((byte) -42, (short) -42, -42, -42L, -42.0f, -42.0d);
        List<Number> zero = Arrays.asList((byte) 0, (short) 0, 0, 0L, 0.0f, 0.0d);
        List<Number> positive = Arrays.asList((byte) 42, (short) 42, 42, 42L, 42.0f, 42.0d);

        for (Number number : negative) {
            Number result = MathOperation.SIGN.apply(number);
            assertEquals(Integer.class, result.getClass());
            assertEquals(-1, result);
        }

        for (Number number : zero) {
            Number result = MathOperation.SIGN.apply(number);
            assertEquals(Integer.class, result.getClass());
            assertEquals(0, result);
        }

        for (Number number : positive) {
            Number result = MathOperation.SIGN.apply(number);
            assertEquals(Integer.class, result.getClass());
            assertEquals(1, result);
        }
    }
}
