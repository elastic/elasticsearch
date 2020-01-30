/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;

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
}
