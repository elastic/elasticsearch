/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.adapter;

import org.elasticsearch.test.ESTestCase;

/**
 * Expectations specific to the JDK 22+ variant of {@link MemorySegmentUtils} in {@code src/main22}.
 */
public class MemorySegmentUtilsJdk22Tests extends ESTestCase {

    /**
     * From JDK 22 a heap segment is a legal downcall argument, so
     * the array is wrapped in place and the copy the JDK 21 variant has to make is avoided.
     */
    public void testArrayIsWrappedWithoutCopying() throws Exception {
        byte[] data = randomByteArrayOfLength(64);
        MemorySegmentUtils.withDowncallSegment(data, data.length, segment -> {
            assertFalse("JDK 22+ should not allocate off-heap", segment.isNative());
            assertSame("the caller's array should back the segment", data, segment.heapBase().orElseThrow());
            return null;
        });
    }
}
