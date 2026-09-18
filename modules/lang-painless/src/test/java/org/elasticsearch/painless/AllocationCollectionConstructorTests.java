/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

/**
 * End-to-end tests for the {@code @allocates} sizings on the common no-arg collection and string-builder
 * constructors. Each charges a fixed byte cost (the object plus any backing allocated eagerly in the constructor; lazily
 * created backing arrays are not counted) before the object is allocated.
 */
public class AllocationCollectionConstructorTests extends AllocationTestCase {

    public void testHashMapCharged() {
        assertEquals(64L, allocatedBytes("new HashMap(); return \"x\";"));
    }

    public void testLinkedHashMapCharged() {
        assertEquals(80L, allocatedBytes("new LinkedHashMap(); return \"x\";"));
    }

    public void testHashSetCharged() {
        assertEquals(88L, allocatedBytes("new HashSet(); return \"x\";"));
    }

    public void testLinkedHashSetCharged() {
        assertEquals(104L, allocatedBytes("new LinkedHashSet(); return \"x\";"));
    }

    public void testTreeMapCharged() {
        assertEquals(40L, allocatedBytes("new TreeMap(); return \"x\";"));
    }

    public void testTreeSetCharged() {
        assertEquals(64L, allocatedBytes("new TreeSet(); return \"x\";"));
    }

    public void testLinkedListCharged() {
        assertEquals(32L, allocatedBytes("new LinkedList(); return \"x\";"));
    }

    public void testStringBuilderCharged() {
        assertEquals(64L, allocatedBytes("new StringBuilder(); return \"x\";"));
    }

    public void testStringBufferCharged() {
        assertEquals(72L, allocatedBytes("new StringBuffer(); return \"x\";"));
    }

    public void testConstructorTripsLimit() {
        assertTripsLimit("new HashMap(); return \"x\";");
    }
}
