/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.test.ESTestCase;

public class EirfSchemaTests extends ESTestCase {

    public void testEmptySchemaHasRoot() {
        EirfSchema schema = new EirfSchema();
        assertEquals(1, schema.nonLeafCount());
        assertEquals(0, schema.leafCount());
        assertEquals("", schema.getNonLeafName(0));
        assertEquals(0, schema.getNonLeafParent(0)); // self-referential root
    }

    public void testAppendNonLeafIdempotent() {
        EirfSchema schema = new EirfSchema();
        int idx1 = schema.appendNonLeaf("user", 0);
        int idx2 = schema.appendNonLeaf("user", 0);
        assertEquals(idx1, idx2);
        assertEquals(2, schema.nonLeafCount());
    }

    public void testAppendLeafIdempotent() {
        EirfSchema schema = new EirfSchema();
        int idx1 = schema.appendLeaf("name", 0);
        int idx2 = schema.appendLeaf("name", 0);
        assertEquals(idx1, idx2);
        assertEquals(1, schema.leafCount());
    }

    public void testSameLeafNameDifferentParents() {
        EirfSchema schema = new EirfSchema();
        int user = schema.appendNonLeaf("user", 0);
        int admin = schema.appendNonLeaf("admin", 0);
        int leaf1 = schema.appendLeaf("name", user);
        int leaf2 = schema.appendLeaf("name", admin);
        assertNotEquals(leaf1, leaf2);
        assertEquals(2, schema.leafCount());
    }

    public void testFindNonLeaf() {
        EirfSchema schema = new EirfSchema();
        int user = schema.appendNonLeaf("user", 0);
        assertEquals(user, schema.findNonLeaf("user", 0));
        assertEquals(-1, schema.findNonLeaf("nonexistent", 0));
        assertEquals(-1, schema.findNonLeaf("user", 999));
    }

    public void testFindLeaf() {
        EirfSchema schema = new EirfSchema();
        int leaf = schema.appendLeaf("name", 0);
        assertEquals(leaf, schema.findLeaf("name", 0));
        assertEquals(-1, schema.findLeaf("nonexistent", 0));
    }

    public void testGetFullPathFlat() {
        EirfSchema schema = new EirfSchema();
        int nameIdx = schema.appendLeaf("name", 0);
        int ageIdx = schema.appendLeaf("age", 0);
        assertEquals("name", schema.getFullPath(nameIdx));
        assertEquals("age", schema.getFullPath(ageIdx));
    }

    public void testGetFullPathOneLevel() {
        EirfSchema schema = new EirfSchema();
        int user = schema.appendNonLeaf("user", 0);
        int nameIdx = schema.appendLeaf("name", user);
        int ageIdx = schema.appendLeaf("age", user);
        assertEquals("user.name", schema.getFullPath(nameIdx));
        assertEquals("user.age", schema.getFullPath(ageIdx));
    }

    public void testGetFullPathDeepNesting() {
        EirfSchema schema = new EirfSchema();
        int a = schema.appendNonLeaf("a", 0);
        int b = schema.appendNonLeaf("b", a);
        int c = schema.appendNonLeaf("c", b);
        int leaf = schema.appendLeaf("d", c);
        assertEquals("a.b.c.d", schema.getFullPath(leaf));
    }

    public void testGetFullPathMixed() {
        EirfSchema schema = new EirfSchema();
        int user = schema.appendNonLeaf("user", 0);
        int nameIdx = schema.appendLeaf("name", user);
        int statusIdx = schema.appendLeaf("status", 0);
        assertEquals("user.name", schema.getFullPath(nameIdx));
        assertEquals("status", schema.getFullPath(statusIdx));
    }

    public void testGetNonLeafChain() {
        EirfSchema schema = new EirfSchema();
        int a = schema.appendNonLeaf("a", 0);
        int b = schema.appendNonLeaf("b", a);

        int[] chain = schema.getNonLeafChain(0);
        assertEquals(0, chain.length);

        chain = schema.getNonLeafChain(a);
        assertArrayEquals(new int[] { a }, chain);

        chain = schema.getNonLeafChain(b);
        assertArrayEquals(new int[] { a, b }, chain);
    }
}
