/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.PainlessTestScript;

/**
 * End-to-end tests for {@code new T()} pre-checks driven by {@code @allocates_constant[bytes="N"]} on a constructor: the
 * declared cost is charged before the object is allocated. Un-annotated constructors charge nothing (annotation-only sizing).
 */
public class AllocationNewObjectPreCheckTests extends AllocationTestCase {

    public void testAnnotatedConstructorCharged() {
        // new ArrayList() is annotated @allocates_constant[bytes="40b"] in java.util.txt.
        assertEquals(40L, allocatedBytes("new ArrayList(); return \"x\";"));
    }

    public void testAnnotatedConstructorTripsLimit() {
        assertTripsLimit("new ArrayList(); return \"x\";");
    }

    public void testUnannotatedConstructorNotCharged() {
        // Annotation-only sizing: an un-annotated constructor charges nothing (documented v1 gap).
        assertEquals(0L, allocatedBytes("new StringBuilder(); return \"x\";"));
    }

    public void testUnannotatedConstructorDoesNotTripLimit() {
        PainlessTestScript script = compile("new StringBuilder(); return \"x\";", "1b");
        script.execute();
        assertEquals(0L, ((PainlessScript) script).getAllocBytes());
    }
}
