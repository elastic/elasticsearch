/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;

import java.io.IOException;

/**
 * Reusable wrapper for turning a {@link BytesReference} into a {@link BytesRef} without
 * per-call allocations. The returned {@link BytesRef} is a reused view and is only valid
 * until the next call to {@link #wrap(BytesReference)}. Not thread-safe.
 */
public final class BytesRefViewScratch {
    private final BytesRef ref = new BytesRef();
    private byte[] copyBuffer;

    public BytesRef wrap(BytesReference bytes) {
        if (bytes.hasArray()) {
            ref.bytes = bytes.array();
            ref.offset = bytes.arrayOffset();
            ref.length = bytes.length();
            return ref;
        }
        int length = bytes.length();
        if (copyBuffer == null || copyBuffer.length < length) {
            copyBuffer = new byte[length];
        }
        copyToArray(bytes, copyBuffer, 0);
        ref.bytes = copyBuffer;
        ref.offset = 0;
        ref.length = length;
        return ref;
    }

    /**
     * Returns a copy of the bytes as a new {@link BytesRef}. Safe to retain.
     */
    public BytesRef copy(BytesReference bytes) {
        int length = bytes.length();
        if (length == 0) {
            return new BytesRef();
        }
        byte[] arr = new byte[length];
        if (bytes.hasArray()) {
            System.arraycopy(bytes.array(), bytes.arrayOffset(), arr, 0, length);
        } else {
            copyToArray(bytes, arr, 0);
        }
        return new BytesRef(arr, 0, length);
    }

    private static void copyToArray(BytesReference bytesReference, byte[] arr, int offset) {
        final BytesRefIterator iterator = bytesReference.iterator();
        try {
            BytesRef slice;
            while ((slice = iterator.next()) != null) {
                System.arraycopy(slice.bytes, slice.offset, arr, offset, slice.length);
                offset += slice.length;
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
