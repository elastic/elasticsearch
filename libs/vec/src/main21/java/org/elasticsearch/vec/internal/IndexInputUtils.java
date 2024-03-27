/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.vec.internal;

import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Objects;

public final class IndexInputUtils {

    static final Class<?> MSINDEX_CLS;
    static final VarHandle SEGMENTS_ARRAY, CHUNK_SIZE_POWER, CHUNK_SIZE_MASK;

    static {
        try {
            MSINDEX_CLS = Class.forName("org.apache.lucene.store.MemorySegmentIndexInput");
            var lookup = privilegedPrivateLookupIn(MSINDEX_CLS, MethodHandles.lookup());
            SEGMENTS_ARRAY = lookup.findVarHandle(MSINDEX_CLS, "segments", MemorySegment[].class);
            CHUNK_SIZE_POWER = lookup.findVarHandle(MSINDEX_CLS, "chunkSizePower", int.class);
            CHUNK_SIZE_MASK = lookup.findVarHandle(MSINDEX_CLS, "chunkSizeMask", long.class);

        } catch (ClassNotFoundException e) {
            throw new AssertionError(e);
        } catch (IllegalAccessException e) {
            throw new AssertionError("should not happen, check opens", e);
        } catch (NoSuchFieldException e) {
            throw new AssertionError("should not happen", e);
        }
    }

    private IndexInputUtils() {}

    static MemorySegment segmentSlice(IndexInput input, long pos, int length) {
        final int si = (int) (pos >> chunkSizePower(input));
        final MemorySegment seg = segmentArray(input)[si];
        try {
            long offset = pos & chunkSizeMask(input);
            Objects.checkIndex(offset + length, seg.byteSize() + 1);
            return seg.asSlice(offset, length);
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    /** Unwraps and returns the input if it's a MemorySegment backed input. Otherwise, null. */
    public static IndexInput unwrapAndCheckInputOrNull(IndexInput input) {
        input = FilterIndexInput.unwrap(input);
        if (MSINDEX_CLS.isAssignableFrom(input.getClass())) {
            return input;
        }
        return null;
    }

    static MemorySegment[] segmentArray(IndexInput input) {
        return (MemorySegment[]) SEGMENTS_ARRAY.get(input);
    }

    static long chunkSizeMask(IndexInput input) {
        return (long) CHUNK_SIZE_MASK.get(input);
    }

    static int chunkSizePower(IndexInput input) {
        return (int) CHUNK_SIZE_POWER.get(input);
    }

    @SuppressWarnings("removal")
    static MethodHandles.Lookup privilegedPrivateLookupIn(Class<?> cls, MethodHandles.Lookup lookup) throws IllegalAccessException {
        PrivilegedAction<MethodHandles.Lookup> pa = () -> {
            try {
                return MethodHandles.privateLookupIn(cls, lookup);
            } catch (IllegalAccessException e) {
                throw new AssertionError("should not happen, check opens", e);
            }
        };
        return AccessController.doPrivileged(pa);
    }
}
