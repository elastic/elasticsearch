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

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;

/**
 * Tests the contract of {@link MemorySegmentUtils#withDowncallSegment} that holds on every JDK.
 *
 * <p>This task runs on the runtime JDK, so it exercises whichever implementation the JVM selects
 * from the multi-release jar. Expectations specific to the JDK 22+ variant live in
 * {@code src/test22}, which runs on JDK 22.
 */
public class MemorySegmentUtilsTests extends ESTestCase {

    public void testSegmentHoldsRequestedBytes() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        byte[] copied = MemorySegmentUtils.withDowncallSegment(data, data.length, segment -> {
            assertEquals(data.length, segment.byteSize());
            return toByteArray(segment);
        });
        assertArrayEquals(data, copied);
    }

    public void testExposesOnlyLeadingBytesOfOversizedArray() throws Exception {
        byte[] scratch = randomByteArrayOfLength(256);
        int length = randomIntBetween(1, scratch.length - 1);
        byte[] copied = MemorySegmentUtils.withDowncallSegment(scratch, length, segment -> {
            assertEquals(length, segment.byteSize());
            return toByteArray(segment);
        });
        assertArrayEquals(Arrays.copyOfRange(scratch, 0, length), copied);
    }

    /**
     * On JDK 21 a heap segment cannot be passed to a native function handle, so the variant in
     * {@code src/main} must hand out an off-heap segment. Only reachable when the tests run on a
     * JDK 21 runtime; on later JDKs the JVM selects the {@code src/main22} variant instead, whose
     * expectations are asserted in {@code src/test22}.
     */
    public void testSegmentIsOffHeapOnJdk21() throws Exception {
        assumeTrue("only the JDK 21 variant must avoid heap segments", Runtime.version().feature() < 22);
        byte[] data = randomByteArrayOfLength(64);
        MemorySegmentUtils.withDowncallSegment(data, data.length, segment -> {
            assertTrue("JDK 21 cannot pass heap segments to a downcall", segment.isNative());
            assertTrue("The segment must have a real address", segment.address() > 0);
            return null;
        });
    }

    public void testResultIsPropagated() throws Exception {
        byte[] data = randomByteArrayOfLength(32);
        String expected = randomAlphaOfLength(8);
        assertEquals(expected, MemorySegmentUtils.withDowncallSegment(data, data.length, segment -> expected));
    }

    public void testCheckedExceptionIsPropagated() {
        byte[] data = randomByteArrayOfLength(32);
        IOException thrown = new IOException("boom");
        IOException caught = expectThrows(IOException.class, () -> MemorySegmentUtils.withDowncallSegment(data, data.length, segment -> {
            throw thrown;
        }));
        assertSame(thrown, caught);
    }

    public void testLengthBeyondArrayIsRejected() {
        byte[] data = randomByteArrayOfLength(16);
        expectThrows(IndexOutOfBoundsException.class, () -> MemorySegmentUtils.withDowncallSegment(data, data.length + 1, segment -> null));
    }

    private static byte[] toByteArray(MemorySegment segment) {
        byte[] buf = new byte[Math.toIntExact(segment.byteSize())];
        MemorySegment.ofArray(buf).copyFrom(segment);
        return buf;
    }
}
