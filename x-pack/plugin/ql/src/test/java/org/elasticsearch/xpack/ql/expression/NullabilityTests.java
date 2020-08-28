/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.ql.expression.Nullability.FALSE;
import static org.elasticsearch.xpack.ql.expression.Nullability.TRUE;
import static org.elasticsearch.xpack.ql.expression.Nullability.UNKNOWN;

public class NullabilityTests extends ESTestCase {

    public void testLogicalAndOfNullabilities() {
        assertEquals(FALSE, Nullability.and());

        assertEquals(TRUE, Nullability.and(TRUE));
        assertEquals(FALSE, Nullability.and(FALSE));
        assertEquals(UNKNOWN, Nullability.and(UNKNOWN));

        assertEquals(UNKNOWN, Nullability.and(UNKNOWN, UNKNOWN));
        assertEquals(UNKNOWN, Nullability.and(UNKNOWN, TRUE));
        assertEquals(UNKNOWN, Nullability.and(UNKNOWN, FALSE));

        assertEquals(FALSE, Nullability.and(FALSE, FALSE));
        assertEquals(TRUE, Nullability.and(FALSE, TRUE));
        assertEquals(UNKNOWN, Nullability.and(FALSE, UNKNOWN));

        assertEquals(TRUE, Nullability.and(TRUE, TRUE));
        assertEquals(TRUE, Nullability.and(TRUE, FALSE));
        assertEquals(UNKNOWN, Nullability.and(TRUE, UNKNOWN));
    }
}
