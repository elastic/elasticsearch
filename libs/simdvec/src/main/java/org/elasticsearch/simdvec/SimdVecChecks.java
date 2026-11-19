/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

import static java.lang.foreign.ValueLayout.JAVA_LONG;

final class SimdVecChecks {

    private SimdVecChecks() {}

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
