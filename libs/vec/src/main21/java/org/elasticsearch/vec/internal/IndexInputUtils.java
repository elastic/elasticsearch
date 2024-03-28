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

public final class IndexInputUtils {

    static final Class<?> MSINDEX_CLS, MS_MSINDEX_CLS;
    static final VarHandle SEGMENTS_ARRAY, CHUNK_SIZE_POWER, CHUNK_SIZE_MASK, MULTI_OFFSET;

    static {
        try {
            MSINDEX_CLS = Class.forName("org.apache.lucene.store.MemorySegmentIndexInput");
            MS_MSINDEX_CLS = Class.forName("org.apache.lucene.store.MemorySegmentIndexInput$MultiSegmentImpl");
            var lookup = privilegedPrivateLookupIn(MSINDEX_CLS, MethodHandles.lookup());
            SEGMENTS_ARRAY = lookup.findVarHandle(MSINDEX_CLS, "segments", MemorySegment[].class);
            CHUNK_SIZE_POWER = lookup.findVarHandle(MSINDEX_CLS, "chunkSizePower", int.class);
            CHUNK_SIZE_MASK = lookup.findVarHandle(MSINDEX_CLS, "chunkSizeMask", long.class);
            MULTI_OFFSET = lookup.findVarHandle(MS_MSINDEX_CLS, "offset", long.class);
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
        if (MS_MSINDEX_CLS.isAssignableFrom(input.getClass())) {
            pos += offset(input);
        }
        final int si = (int) (pos >> chunkSizePower(input));
        final MemorySegment seg = segmentArray(input)[si];
        long offset = pos & chunkSizeMask(input);
        if (checkIndex(offset + length, seg.byteSize() + 1)) {
            return seg.asSlice(offset, length);
        }
        return null;
    }

    static boolean checkIndex(long index, long length) {
        return index >= 0 && index < length;
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

    static long offset(IndexInput input) {
        return (long) MULTI_OFFSET.get(input);
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
