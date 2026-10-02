/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

/**
 * Tests that verify some assumptions or expectations about Arrow's behavior.
 */
public class ArrowBaselineTests extends ESTestCase {

    private RootAllocator allocator;

    @Before
    public void setup() {
        allocator = new RootAllocator();
    }

    @After
    public void cleanup() {
        allocator.close();
    }

    public void testValueByteSize() {
        // Buffer size for a single element is the type's size + 1 byte for validity
        // (used in ArrowUtils)
        try (var intVec = new IntVector("foo", allocator)) {
            assertEquals(5, intVec.getBufferSizeFor(1));
        }
    }
}
