/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import java.util.HashMap;
import java.util.Map;

/**
 * End-to-end tests for {@code @allocates_dynamic} estimators on collection materialization and copying ({@code toArray} and the
 * copy constructors), whose result scales with the source collection's size.
 */
public class AllocationCollectionCopyTests extends AllocationTestCase {

    public void testToArrayCharged() {
        // new ArrayList() charges 40; toArray() on the empty list charges a new Object[0].
        long expected = 40L + AllocSizes.arrayBytes(0, AllocSizes.REFERENCE_SIZE);
        assertEquals(expected, allocatedBytes("new ArrayList().toArray(); return \"x\";"));
    }

    public void testToArrayTripsLimit() {
        assertTripsLimit("new ArrayList().toArray(); return \"x\";");
    }

    public void testMapCopyEmptyCharged() {
        // inner new HashMap() = 64; outer copy of the empty map = the map shell only.
        long expected = 64L + AllocationEstimators.mapCopyBytes(new HashMap<>());
        assertEquals(expected, allocatedBytes("new HashMap(new HashMap()); return \"x\";"));
    }

    public void testMapCopyScalesWithSize() {
        // inner map populated with one entry (put is not charged), then copied: charge scales with the source size.
        Map<String, String> one = new HashMap<>();
        one.put("a", "b");
        long expected = 64L + AllocationEstimators.mapCopyBytes(one);
        assertEquals(expected, allocatedBytes("Map m = new HashMap(); m.put(\"a\", \"b\"); new HashMap(m); return \"x\";"));
    }

    public void testSetCopyEmptyCharged() {
        // inner new ArrayList() = 40; outer HashSet copy of the empty collection = the set shell only.
        long expected = 40L + AllocationEstimators.setCopyBytes(new java.util.ArrayList<>());
        assertEquals(expected, allocatedBytes("new HashSet(new ArrayList()); return \"x\";"));
    }

    public void testLinkedListCopyEmptyCharged() {
        long expected = 40L + AllocationEstimators.linkedListCopyBytes(new java.util.ArrayList<>());
        assertEquals(expected, allocatedBytes("new LinkedList(new ArrayList()); return \"x\";"));
    }

    public void testCopyConstructorTripsLimit() {
        assertTripsLimit("new HashSet(new ArrayList()); return \"x\";");
    }
}
