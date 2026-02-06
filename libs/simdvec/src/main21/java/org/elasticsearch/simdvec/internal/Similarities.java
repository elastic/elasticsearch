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
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.BBQType;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.DataType;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.Function;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.Operation;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;

public class Similarities {

    static final VectorSimilarityFunctions DISTANCE_FUNCS = NativeAccess.instance()
        .getVectorSimilarityFunctions()
        .orElseThrow(AssertionError::new);

    static final MethodHandle DOT_PRODUCT_7U = DISTANCE_FUNCS.getHandle(Function.DOT_PRODUCT, DataType.INT7, Operation.SINGLE);
    static final MethodHandle DOT_PRODUCT_7U_BULK = DISTANCE_FUNCS.getHandle(Function.DOT_PRODUCT, DataType.INT7, Operation.BULK);
    static final MethodHandle DOT_PRODUCT_7U_BULK_WITH_OFFSETS = DISTANCE_FUNCS.getHandle(
        Function.DOT_PRODUCT,
        DataType.INT7,
        Operation.BULK_OFFSETS
    );
    static final MethodHandle SQUARE_DISTANCE_7U = DISTANCE_FUNCS.getHandle(Function.SQUARE_DISTANCE, DataType.INT7, Operation.SINGLE);
    static final MethodHandle SQUARE_DISTANCE_7U_BULK = DISTANCE_FUNCS.getHandle(Function.SQUARE_DISTANCE, DataType.INT7, Operation.BULK);
    static final MethodHandle SQUARE_DISTANCE_7U_BULK_WITH_OFFSETS = DISTANCE_FUNCS.getHandle(
        Function.SQUARE_DISTANCE,
        DataType.INT7,
        Operation.BULK_OFFSETS
    );

    static final MethodHandle DOT_PRODUCT_I1I4 = DISTANCE_FUNCS.getHandle(Function.DOT_PRODUCT, BBQType.I1I4, Operation.SINGLE);
    static final MethodHandle DOT_PRODUCT_I1I4_BULK = DISTANCE_FUNCS.getHandle(Function.DOT_PRODUCT, BBQType.I1I4, Operation.BULK);
    static final MethodHandle DOT_PRODUCT_I1I4_BULK_WITH_OFFSETS = DISTANCE_FUNCS.getHandle(
        Function.DOT_PRODUCT,
        BBQType.I1I4,
        Operation.BULK_OFFSETS
    );

    static final MethodHandle DOT_PRODUCT_I2I4 = DISTANCE_FUNCS.getHandle(Function.DOT_PRODUCT, BBQType.I2I4, Operation.SINGLE);
    static final MethodHandle DOT_PRODUCT_I2I4_BULK = DISTANCE_FUNCS.getHandle(Function.DOT_PRODUCT, BBQType.I2I4, Operation.BULK);
    static final MethodHandle DOT_PRODUCT_I2I4_BULK_WITH_OFFSETS = DISTANCE_FUNCS.getHandle(
        Function.DOT_PRODUCT,
        BBQType.I2I4,
        Operation.BULK_OFFSETS
    );

    static final MethodHandle DOT_PRODUCT_F32 = DISTANCE_FUNCS.getHandle(Function.DOT_PRODUCT, DataType.FLOAT32, Operation.SINGLE);
    static final MethodHandle DOT_PRODUCT_F32_BULK = DISTANCE_FUNCS.getHandle(Function.DOT_PRODUCT, DataType.FLOAT32, Operation.BULK);
    static final MethodHandle DOT_PRODUCT_F32_BULK_WITH_OFFSETS = DISTANCE_FUNCS.getHandle(
        Function.DOT_PRODUCT,
        DataType.FLOAT32,
        Operation.BULK_OFFSETS
    );
    static final MethodHandle SQUARE_DISTANCE_F32 = DISTANCE_FUNCS.getHandle(Function.SQUARE_DISTANCE, DataType.FLOAT32, Operation.SINGLE);
    static final MethodHandle SQUARE_DISTANCE_F32_BULK = DISTANCE_FUNCS.getHandle(
        Function.SQUARE_DISTANCE,
        DataType.FLOAT32,
        Operation.BULK
    );
    static final MethodHandle SQUARE_DISTANCE_F32_BULK_WITH_OFFSETS = DISTANCE_FUNCS.getHandle(
        Function.SQUARE_DISTANCE,
        DataType.FLOAT32,
        Operation.BULK_OFFSETS
    );

    private static RuntimeException rethrow(Throwable t) {
        if (t instanceof Error err) {
            throw err;
        }
        return t instanceof RuntimeException re ? re : new RuntimeException(t);
    }

    static int dotProduct7u(MemorySegment a, MemorySegment b, int length) {
        try {
            return (int) DOT_PRODUCT_7U.invokeExact(a, b, length);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void dotProduct7uBulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
        try {
            DOT_PRODUCT_7U_BULK.invokeExact(a, b, length, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
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
            DOT_PRODUCT_7U_BULK_WITH_OFFSETS.invokeExact(a, b, length, pitch, offsets, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    public static long dotProductI1I4(MemorySegment a, MemorySegment query, int length) {
        try {
            return (long) DOT_PRODUCT_I1I4.invokeExact(a, query, length);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    public static void dotProductI1I4Bulk(MemorySegment a, MemorySegment query, int length, int count, MemorySegment scores) {
        try {
            DOT_PRODUCT_I1I4_BULK.invokeExact(a, query, length, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void dotProductI1I4BulkWithOffsets(
        MemorySegment a,
        MemorySegment query,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        try {
            DOT_PRODUCT_I1I4_BULK_WITH_OFFSETS.invokeExact(a, query, length, pitch, offsets, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    public static long dotProductI2I4(MemorySegment a, MemorySegment query, int length) {
        try {
            return (long) DOT_PRODUCT_I2I4.invokeExact(a, query, length);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    public static void dotProductI2I4Bulk(MemorySegment a, MemorySegment query, int length, int count, MemorySegment scores) {
        try {
            DOT_PRODUCT_I2I4_BULK.invokeExact(a, query, length, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void dotProductI2I4BulkWithOffsets(
        MemorySegment a,
        MemorySegment query,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        try {
            DOT_PRODUCT_I2I4_BULK_WITH_OFFSETS.invokeExact(a, query, length, pitch, offsets, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static int squareDistance7u(MemorySegment a, MemorySegment b, int length) {
        try {
            return (int) SQUARE_DISTANCE_7U.invokeExact(a, b, length);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void squareDistance7uBulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
        try {
            SQUARE_DISTANCE_7U_BULK.invokeExact(a, b, length, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void squareDistance7uBulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        try {
            SQUARE_DISTANCE_7U_BULK_WITH_OFFSETS.invokeExact(a, b, length, pitch, offsets, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    public static float dotProductF32(MemorySegment a, MemorySegment b, int length) {
        try {
            return (float) DOT_PRODUCT_F32.invokeExact(a, b, length);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void dotProductF32Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
        try {
            DOT_PRODUCT_F32_BULK.invokeExact(a, b, length, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void dotProductF32BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        try {
            DOT_PRODUCT_F32_BULK_WITH_OFFSETS.invokeExact(a, b, length, pitch, offsets, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    public static float squareDistanceF32(MemorySegment a, MemorySegment b, int length) {
        try {
            return (float) SQUARE_DISTANCE_F32.invokeExact(a, b, length);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void squareDistanceF32Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
        try {
            SQUARE_DISTANCE_F32_BULK.invokeExact(a, b, length, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void squareDistanceF32BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        try {
            SQUARE_DISTANCE_F32_BULK_WITH_OFFSETS.invokeExact(a, b, length, pitch, offsets, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }
}
