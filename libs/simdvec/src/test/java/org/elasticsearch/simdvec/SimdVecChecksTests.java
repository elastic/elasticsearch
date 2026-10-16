/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.elasticsearch.test.ESTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class SimdVecChecksTests extends ESTestCase {

    /**
     * Offsets are scaled by the pitch and checked against {@code byteSize()} as {@code long}s, so rows
     * beyond {@code Integer.MAX_VALUE} bytes into a large dataset are accepted, and rows past its end are
     * still rejected (see #143232). The dataset is a small real allocation reinterpreted to a larger size;
     * the check only reads its size.
     */
    public void testBulkOffsetsDoNotOverflowBeyondIntRange() {
        final int pitch = 1024;
        final long datasetBytes = 3L << 30;
        final int lastRow = (int) (datasetBytes / pitch) - 1;
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment dataset = arena.allocate(pitch).reinterpret(datasetBytes);
            MemorySegment offsets = arena.allocate(3L * Integer.BYTES);
            offsets.setAtIndex(ValueLayout.JAVA_INT, 0, 0);
            offsets.setAtIndex(ValueLayout.JAVA_INT, 1, 1);
            offsets.setAtIndex(ValueLayout.JAVA_INT, 2, lastRow);
            assertTrue(SimdVecChecks.validateBulkOffsets(dataset, offsets, 3, pitch, pitch));

            offsets.setAtIndex(ValueLayout.JAVA_INT, 2, lastRow + 1);
            expectThrows(IndexOutOfBoundsException.class, () -> SimdVecChecks.validateBulkOffsets(dataset, offsets, 3, pitch, pitch));
        }
    }
}
