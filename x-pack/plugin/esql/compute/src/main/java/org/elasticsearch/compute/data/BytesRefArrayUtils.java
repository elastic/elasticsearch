/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefArray;

/**
 * Helpers for filling a {@link BytesRefArray} from packed (prefix-free) payloads.
 */
public final class BytesRefArrayUtils {

    private BytesRefArrayUtils() {}

    /**
     * Appends {@code count} packed payloads by slicing each value and calling
     * {@link BytesRefArray#append(BytesRef)}. {@code exclusiveEnds[i]} is the exclusive offset of
     * value {@code i} relative to {@code packedOff}.
     */
    public static void appendPacked(BytesRefArray array, byte[] packed, int packedOff, int packedLen, int[] exclusiveEnds, int count) {
        if (count == 0) {
            return;
        }
        assert exclusiveEnds[count - 1] == packedLen : exclusiveEnds[count - 1] + " != " + packedLen;
        BytesRef scratch = new BytesRef();
        scratch.bytes = packed;
        int start = packedOff;
        for (int i = 0; i < count; i++) {
            int end = packedOff + exclusiveEnds[i];
            scratch.offset = start;
            scratch.length = end - start;
            array.append(scratch);
            start = end;
        }
    }
}
