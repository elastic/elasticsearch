/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.logical;

import org.elasticsearch.test.ESTestCase;

public class BinaryLogicOperationTests extends ESTestCase {

    public void testOR() {
        assertEquals(true, BinaryLogicOperation.OR.apply(true, false));
        assertEquals(true, BinaryLogicOperation.OR.apply(false, true));
        assertEquals(false, BinaryLogicOperation.OR.apply(false, false));
        assertEquals(true, BinaryLogicOperation.OR.apply(true, true));
    }

    public void testORNullHandling() {
        assertEquals(true, BinaryLogicOperation.OR.apply(true, null));
        assertEquals(true, BinaryLogicOperation.OR.apply(null, true));
        assertNull(BinaryLogicOperation.OR.apply(false, null));
        assertNull(BinaryLogicOperation.OR.apply(null, false));
        assertNull(BinaryLogicOperation.OR.apply(null, null));
    }

    public void testAnd() {
        assertEquals(false, BinaryLogicOperation.AND.apply(true, false));
        assertEquals(false, BinaryLogicOperation.AND.apply(false, true));
        assertEquals(false, BinaryLogicOperation.AND.apply(false, false));
        assertEquals(true, BinaryLogicOperation.AND.apply(true, true));
    }

    public void testAndNullHandling() {
        assertNull(BinaryLogicOperation.AND.apply(true, null));
        assertNull(BinaryLogicOperation.AND.apply(null, true));
        assertEquals(false, BinaryLogicOperation.AND.apply(false, null));
        assertEquals(false, BinaryLogicOperation.AND.apply(null, false));
        assertNull(BinaryLogicOperation.AND.apply(null, null));
    }
}
