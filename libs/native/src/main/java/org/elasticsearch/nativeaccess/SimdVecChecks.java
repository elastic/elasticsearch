/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

final class SimdVecChecks {

    private SimdVecChecks() {}

    /** Checks that every {@code offsets[i]} addresses a whole {@code rowBytes} row within {@code a}. */
    static boolean validateBulkOffsets(MemorySegment a, MemorySegment offsets, int count, int pitch, long rowBytes) {
        Objects.checkFromIndexSize(0L, (long) count * Integer.BYTES, offsets.byteSize());
        long aSize = a.byteSize();
        for (int i = 0; i < count; i++) {
            int offset = offsets.getAtIndex(JAVA_INT, i);
            Objects.checkFromIndexSize((long) offset * pitch, rowBytes, aSize);
        }
        return true;
    }

    /** Checks that every address in {@code addresses} is non-null. */
    static boolean validateBulkSparse(MemorySegment addresses, int count) {
        Objects.checkFromIndexSize(0L, (long) count * Long.BYTES, addresses.byteSize());
        for (int i = 0; i < count; i++) {
            if (addresses.getAtIndex(JAVA_LONG, i) == 0) {
                throw new IllegalArgumentException("address at index " + i + " is null");
            }
        }
        return true;
    }
}
