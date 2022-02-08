/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.elasticsearch.test.ESTestCase;

import java.util.NoSuchElementException;

public class DenseVectorDocValuesFieldTests extends ESTestCase {
    public void testEmpty() {
        String name = randomAlphaOfLength(8);
        int dims = randomIntBetween(1, 5);
        DenseVectorDocValuesField field = DenseVectorDocValuesField.empty(name, dims);

        assertEquals(name, field.getName());
        assertEquals(dims, field.getScriptDocValues().dims());

        DenseVector dv = field.get();
        QueryVector empty = new QueryVector(new float[0]);

        String message = "Dense vector value missing for a field, use isEmpty() to check for a missing vector value";
        IllegalArgumentException missing = expectThrows(IllegalArgumentException.class, dv::getMagnitude);
        assertEquals(message, missing.getMessage());

        missing = expectThrows(IllegalArgumentException.class, () -> dv.dotProduct(empty));
        assertEquals(message, missing.getMessage());

        missing = expectThrows(IllegalArgumentException.class, () -> dv.l1Norm(empty));
        assertEquals(message, missing.getMessage());

        missing = expectThrows(IllegalArgumentException.class, () -> dv.l2Norm(empty));
        assertEquals(message, missing.getMessage());

        missing = expectThrows(IllegalArgumentException.class, dv::getVector);
        assertEquals(message, missing.getMessage());

        missing = expectThrows(IllegalArgumentException.class, dv::getDims);
        assertEquals(message, missing.getMessage());

        assertTrue(dv.isEmpty());
        assertTrue(field.isEmpty());
        assertEquals(0, dv.size());
        assertEquals(0, field.size());

        assertNull(field.getInternal());
        assertNull(field.get(null));

        assertFalse(dv.iterator().hasNext());
        assertFalse(field.iterator().hasNext());

        expectThrows(NoSuchElementException.class, () -> dv.iterator().next());
        expectThrows(NoSuchElementException.class, () -> field.iterator().next());
        expectThrows(UnsupportedOperationException.class, () -> field.getInternal(0));
    }
}
