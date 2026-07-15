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
 * End-to-end tests for the {@code @allocates_constant} sizings on the rarer collection constructors (whose backing arrays are
 * allocated eagerly in the constructor) and the {@code Collections}/{@code Arrays} factory wrappers (small fixed-size views;
 * {@code emptyList/emptyMap/emptySet} return shared singletons and charge nothing).
 */
public class AllocationBaseConstantTests extends AllocationTestCase {

    public void testVectorCharged() {
        assertEquals(128L, allocatedBytes("new Vector(); return \"x\";"));
    }

    public void testArrayDequeCharged() {
        assertEquals(176L, allocatedBytes("new ArrayDeque(); return \"x\";"));
    }

    public void testBitSetCharged() {
        assertEquals(56L, allocatedBytes("new BitSet(); return \"x\";"));
    }

    public void testSingletonListFactoryCharged() {
        // Confirms a static factory method's @allocates_constant fires; a String arg avoids boxing noise.
        assertEquals(24L, allocatedBytes("Collections.singletonList(\"a\"); return \"x\";"));
    }

    public void testEmptyListFactoryChargesNothing() {
        // Collections.emptyList() returns a shared singleton, so it is left unannotated and charges nothing.
        assertEquals(0L, allocatedBytes("Collections.emptyList(); return \"x\";"));
    }

    public void testRareConstructorTripsLimit() {
        assertTripsLimit("new Vector(); return \"x\";");
    }
}
