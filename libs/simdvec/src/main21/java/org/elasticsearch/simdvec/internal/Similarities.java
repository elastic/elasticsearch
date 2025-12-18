/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;

public class Similarities {

    static final VectorSimilarityFunctions DISTANCE_FUNCS = NativeAccess.instance()
        .getVectorSimilarityFunctions()
        .orElseThrow(AssertionError::new);

    static final MethodHandle DOT_PRODUCT_7U = DISTANCE_FUNCS.dotProductHandle7u();
    static final MethodHandle DOT_PRODUCT_7U_BULK = DISTANCE_FUNCS.dotProductHandle7uBulk();
    static final MethodHandle DOT_HANDLE_7U_BULK_WITH_OFFSETS = DISTANCE_FUNCS.dotProductHandle7uBulkWithOffsets();

    static final MethodHandle DOT_PRODUCT_I4B1 = DISTANCE_FUNCS.dotProductHandleI4B1();
    static final MethodHandle DOT_PRODUCT_I4B1_BULK = DISTANCE_FUNCS.dotProductHandleI4B1Bulk();
    static final MethodHandle DOT_HANDLE_I4B1_BULK_WITH_OFFSETS = DISTANCE_FUNCS.dotProductHandleI4B1BulkWithOffsets();

    static final MethodHandle SQUARE_DISTANCE_7U = DISTANCE_FUNCS.squareDistanceHandle7u();

    static int dotProduct7u(MemorySegment a, MemorySegment b, int length) {
        try {
            return (int) DOT_PRODUCT_7U.invokeExact(a, b, length);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    static void dotProduct7uBulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
        try {
            DOT_PRODUCT_7U_BULK.invokeExact(a, b, length, count, scores);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    static void dotProduct7uBulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        try {
            DOT_HANDLE_7U_BULK_WITH_OFFSETS.invokeExact(a, b, length, pitch, offsets, count, scores);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public static int dotProductI4B1(MemorySegment a, MemorySegment b, int length) {
        try {
            return (int) DOT_PRODUCT_I4B1.invokeExact(a, b, length);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public static void dotProductI4B1Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
        try {
            DOT_PRODUCT_I4B1_BULK.invokeExact(a, b, length, count, scores);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    static void dotProductI4B1BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        try {
            DOT_HANDLE_I4B1_BULK_WITH_OFFSETS.invokeExact(a, b, length, pitch, offsets, count, scores);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    static int squareDistance7u(MemorySegment a, MemorySegment b, int length) {
        try {
            return (int) SQUARE_DISTANCE_7U.invokeExact(a, b, length);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }
}
