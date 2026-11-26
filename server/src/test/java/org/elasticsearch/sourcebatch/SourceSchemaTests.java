/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.elasticsearch.test.ESTestCase;

public class SourceSchemaTests extends ESTestCase {

    public void testEmptySchemaHasRoot() {
        SourceSchema schema = new SourceSchema();
        assertEquals(1, schema.nonLeafCount());
        assertEquals(0, schema.leafCount());
        assertEquals("", schema.getNonLeafName(0));
        assertEquals(0, schema.getNonLeafParent(0)); // self-referential root
    }

    public void testAppendNonLeafIdempotent() {
        SourceSchema schema = new SourceSchema();
        int idx1 = schema.appendNonLeaf("user", 0);
        int idx2 = schema.appendNonLeaf("user", 0);
        assertEquals(idx1, idx2);
        assertEquals(2, schema.nonLeafCount());
    }

    public void testAppendLeafIdempotent() {
        SourceSchema schema = new SourceSchema();
        int idx1 = schema.appendLeaf("name", 0);
        int idx2 = schema.appendLeaf("name", 0);
        assertEquals(idx1, idx2);
        assertEquals(1, schema.leafCount());
    }

    public void testSameLeafNameDifferentParents() {
        SourceSchema schema = new SourceSchema();
        int user = schema.appendNonLeaf("user", 0);
        int admin = schema.appendNonLeaf("admin", 0);
        int leaf1 = schema.appendLeaf("name", user);
        int leaf2 = schema.appendLeaf("name", admin);
        assertNotEquals(leaf1, leaf2);
        assertEquals(2, schema.leafCount());
    }

    public void testFindNonLeaf() {
        SourceSchema schema = new SourceSchema();
        int user = schema.appendNonLeaf("user", 0);
        assertEquals(user, schema.findNonLeaf("user", 0));
        assertEquals(-1, schema.findNonLeaf("nonexistent", 0));
        assertEquals(-1, schema.findNonLeaf("user", 999));
    }

    public void testFindLeaf() {
        SourceSchema schema = new SourceSchema();
        int leaf = schema.appendLeaf("name", 0);
        assertEquals(leaf, schema.findLeaf("name", 0));
        assertEquals(-1, schema.findLeaf("nonexistent", 0));
    }

    public void testGetFullPathFlat() {
        SourceSchema schema = new SourceSchema();
        int nameIdx = schema.appendLeaf("name", 0);
        int ageIdx = schema.appendLeaf("age", 0);
        assertEquals("name", schema.getFullPath(nameIdx));
        assertEquals("age", schema.getFullPath(ageIdx));
    }

    public void testGetFullPathOneLevel() {
        SourceSchema schema = new SourceSchema();
        int user = schema.appendNonLeaf("user", 0);
        int nameIdx = schema.appendLeaf("name", user);
        int ageIdx = schema.appendLeaf("age", user);
        assertEquals("user.name", schema.getFullPath(nameIdx));
        assertEquals("user.age", schema.getFullPath(ageIdx));
    }

    public void testGetFullPathDeepNesting() {
        SourceSchema schema = new SourceSchema();
        int a = schema.appendNonLeaf("a", 0);
        int b = schema.appendNonLeaf("b", a);
        int c = schema.appendNonLeaf("c", b);
        int leaf = schema.appendLeaf("d", c);
        assertEquals("a.b.c.d", schema.getFullPath(leaf));
    }

    public void testGetFullPathMixed() {
        SourceSchema schema = new SourceSchema();
        int user = schema.appendNonLeaf("user", 0);
        int nameIdx = schema.appendLeaf("name", user);
        int statusIdx = schema.appendLeaf("status", 0);
        assertEquals("user.name", schema.getFullPath(nameIdx));
        assertEquals("status", schema.getFullPath(statusIdx));
    }

    public void testGetNonLeafChain() {
        SourceSchema schema = new SourceSchema();
        int a = schema.appendNonLeaf("a", 0);
        int b = schema.appendNonLeaf("b", a);

        int[] chain = schema.getNonLeafChain(0);
        assertEquals(0, chain.length);

        chain = schema.getNonLeafChain(a);
        assertArrayEquals(new int[] { a }, chain);

        chain = schema.getNonLeafChain(b);
        assertArrayEquals(new int[] { a, b }, chain);
    }

    public void testAppendLeafWithPredictionAgreesWithoutPrediction() {
        SourceSchema withPrediction = new SourceSchema();
        SourceSchema plain = new SourceSchema();
        int user = withPrediction.appendNonLeaf("user", 0);
        assertEquals(user, plain.appendNonLeaf("user", 0));
        String[] names = { "a", "b", "c", "a", "b", "d", "c" };
        int[] parents = { 0, 0, user, user, 0, user, 0 };
        for (int round = 0; round < 3; round++) {
            for (int i = 0; i < names.length; i++) {
                // Any prediction is allowed, including ones that are out of range or point at another field.
                int prediction = randomIntBetween(0, withPrediction.leafCount() + 3);
                assertEquals(plain.appendLeaf(names[i], parents[i]), withPrediction.appendLeaf(names[i], parents[i], prediction));
            }
        }
        assertEquals(plain.leafCount(), withPrediction.leafCount());
    }

    public void testAppendLeafWithPredictionChecksNameAndParent() {
        SourceSchema schema = new SourceSchema();
        int user = schema.appendNonLeaf("user", 0);
        int topName = schema.appendLeaf("name", 0);
        int userName = schema.appendLeaf("name", user);
        // Right prediction.
        assertEquals(topName, schema.appendLeaf("name", 0, topName));
        assertEquals(userName, schema.appendLeaf("name", user, userName));
        // Same name under another parent: the prediction must not be taken.
        assertEquals(userName, schema.appendLeaf("name", user, topName));
        assertEquals(topName, schema.appendLeaf("name", 0, userName));
        // A new field is appended even when the prediction points at an existing field.
        int added = schema.appendLeaf("other", 0, topName);
        assertEquals(2, added);
        assertEquals(3, schema.leafCount());
        // Prediction past the end.
        assertEquals(added, schema.appendLeaf("other", 0, 100));
        assertEquals(3, schema.leafCount());
    }

    public void testAppendNonLeafWithPredictionAgreesWithoutPrediction() {
        SourceSchema withPrediction = new SourceSchema();
        SourceSchema plain = new SourceSchema();
        // The parents are the indices the first round assigns: a(0)=1, b(0)=2, a(1)=3, b(1)=4, c(2)=5. Later steps repeat
        // them, so the same name appears under several parents and every parent already exists when it is used.
        String[] names = { "a", "b", "a", "b", "c", "a", "b", "c" };
        int[] parents = { 0, 0, 1, 1, 2, 0, 1, 2 };
        for (int round = 0; round < 3; round++) {
            for (int i = 0; i < names.length; i++) {
                // Any prediction is allowed, including ones that are out of range or point at another field.
                int prediction = randomIntBetween(0, withPrediction.nonLeafCount() + 3);
                assertEquals(plain.appendNonLeaf(names[i], parents[i]), withPrediction.appendNonLeaf(names[i], parents[i], prediction));
            }
        }
        assertEquals(plain.nonLeafCount(), withPrediction.nonLeafCount());
    }

    public void testAppendNonLeafWithPrediction() {
        SourceSchema schema = new SourceSchema();
        int a = schema.appendNonLeaf("a", 0, 1);
        assertEquals(1, a);
        int b = schema.appendNonLeaf("b", a, a + 1);
        assertEquals(2, b);
        assertEquals(a, schema.appendNonLeaf("a", 0, 1));
        assertEquals(b, schema.appendNonLeaf("b", a, 1));
        assertEquals(b, schema.appendNonLeaf("b", a, 2));
        assertEquals(3, schema.nonLeafCount());
        // Prediction past the end.
        assertEquals(b, schema.appendNonLeaf("b", a, 100));
        assertEquals(3, schema.nonLeafCount());
    }

    /** Callers only predict indices at or after the start of the schema; a negative one is a bug, caught by an assertion. */
    public void testNegativePredictionIsRejected() {
        SourceSchema schema = new SourceSchema();
        int user = schema.appendNonLeaf("user", 0);
        schema.appendLeaf("name", user);
        expectThrows(AssertionError.class, () -> schema.appendLeaf("name", user, -1));
        expectThrows(AssertionError.class, () -> schema.appendNonLeaf("user", 0, -1));
    }
}
