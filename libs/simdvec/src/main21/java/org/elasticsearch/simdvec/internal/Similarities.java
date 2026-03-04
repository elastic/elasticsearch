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

    static final MethodHandle DOT_PRODUCT_I7U = DISTANCE_FUNCS.getHandle(Function.DOT_PRODUCT, DataType.INT7U, Operation.SINGLE);
    static final MethodHandle DOT_PRODUCT_I7U_BULK = DISTANCE_FUNCS.getHandle(Function.DOT_PRODUCT, DataType.INT7U, Operation.BULK);
    static final MethodHandle DOT_PRODUCT_I7U_BULK_WITH_OFFSETS = DISTANCE_FUNCS.getHandle(
        Function.DOT_PRODUCT,
        DataType.INT7U,
        Operation.BULK_OFFSETS
    );
    static final MethodHandle SQUARE_DISTANCE_I7U = DISTANCE_FUNCS.getHandle(Function.SQUARE_DISTANCE, DataType.INT7U, Operation.SINGLE);
    static final MethodHandle SQUARE_DISTANCE_I7U_BULK = DISTANCE_FUNCS.getHandle(Function.SQUARE_DISTANCE, DataType.INT7U, Operation.BULK);
    static final MethodHandle SQUARE_DISTANCE_I7U_BULK_WITH_OFFSETS = DISTANCE_FUNCS.getHandle(
        Function.SQUARE_DISTANCE,
        DataType.INT7U,
        Operation.BULK_OFFSETS
    );

    static final MethodHandle COSINE_I8 = DISTANCE_FUNCS.getHandle(Function.COSINE, DataType.INT8, Operation.SINGLE);
    static final MethodHandle COSINE_I8_BULK = DISTANCE_FUNCS.getHandle(Function.COSINE, DataType.INT8, Operation.BULK);
    static final MethodHandle COSINE_I8_BULK_WITH_OFFSETS = DISTANCE_FUNCS.getHandle(
        Function.COSINE,
        DataType.INT8,
        Operation.BULK_OFFSETS
    );
    static final MethodHandle DOT_PRODUCT_I8 = DISTANCE_FUNCS.getHandle(Function.DOT_PRODUCT, DataType.INT8, Operation.SINGLE);
    static final MethodHandle DOT_PRODUCT_I8_BULK = DISTANCE_FUNCS.getHandle(Function.DOT_PRODUCT, DataType.INT8, Operation.BULK);
    static final MethodHandle DOT_PRODUCT_I8_BULK_WITH_OFFSETS = DISTANCE_FUNCS.getHandle(
        Function.DOT_PRODUCT,
        DataType.INT8,
        Operation.BULK_OFFSETS
    );
    static final MethodHandle SQUARE_DISTANCE_I8 = DISTANCE_FUNCS.getHandle(Function.SQUARE_DISTANCE, DataType.INT8, Operation.SINGLE);
    static final MethodHandle SQUARE_DISTANCE_I8_BULK = DISTANCE_FUNCS.getHandle(Function.SQUARE_DISTANCE, DataType.INT8, Operation.BULK);
    static final MethodHandle SQUARE_DISTANCE_I8_BULK_WITH_OFFSETS = DISTANCE_FUNCS.getHandle(
        Function.SQUARE_DISTANCE,
        DataType.INT8,
        Operation.BULK_OFFSETS
    );

    static final MethodHandle DOT_PRODUCT_D1Q4 = DISTANCE_FUNCS.getHandle(Function.DOT_PRODUCT, BBQType.D1Q4, Operation.SINGLE);
    static final MethodHandle DOT_PRODUCT_D1Q4_BULK = DISTANCE_FUNCS.getHandle(Function.DOT_PRODUCT, BBQType.D1Q4, Operation.BULK);
    static final MethodHandle DOT_PRODUCT_D1Q4_BULK_WITH_OFFSETS = DISTANCE_FUNCS.getHandle(
        Function.DOT_PRODUCT,
        BBQType.D1Q4,
        Operation.BULK_OFFSETS
    );

    static final MethodHandle DOT_PRODUCT_D2Q4 = DISTANCE_FUNCS.getHandle(Function.DOT_PRODUCT, BBQType.D2Q4, Operation.SINGLE);
    static final MethodHandle DOT_PRODUCT_D2Q4_BULK = DISTANCE_FUNCS.getHandle(Function.DOT_PRODUCT, BBQType.D2Q4, Operation.BULK);
    static final MethodHandle DOT_PRODUCT_D2Q4_BULK_WITH_OFFSETS = DISTANCE_FUNCS.getHandle(
        Function.DOT_PRODUCT,
        BBQType.D2Q4,
        Operation.BULK_OFFSETS
    );

    static final MethodHandle DOT_PRODUCT_D4Q4 = DISTANCE_FUNCS.getHandle(Function.DOT_PRODUCT, BBQType.D4Q4, Operation.SINGLE);
    static final MethodHandle DOT_PRODUCT_D4Q4_BULK = DISTANCE_FUNCS.getHandle(Function.DOT_PRODUCT, BBQType.D4Q4, Operation.BULK);
    static final MethodHandle DOT_PRODUCT_D4Q4_BULK_WITH_OFFSETS = DISTANCE_FUNCS.getHandle(
        Function.DOT_PRODUCT,
        BBQType.D4Q4,
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

    static int dotProductI7u(MemorySegment a, MemorySegment b, int length) {
        try {
            return (int) DOT_PRODUCT_I7U.invokeExact(a, b, length);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void dotProductI7uBulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
        try {
            DOT_PRODUCT_I7U_BULK.invokeExact(a, b, length, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void dotProductI7uBulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        try {
            DOT_PRODUCT_I7U_BULK_WITH_OFFSETS.invokeExact(a, b, length, pitch, offsets, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static int squareDistanceI7u(MemorySegment a, MemorySegment b, int length) {
        try {
            return (int) SQUARE_DISTANCE_I7U.invokeExact(a, b, length);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void squareDistanceI7uBulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
        try {
            SQUARE_DISTANCE_I7U_BULK.invokeExact(a, b, length, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void squareDistanceI7uBulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        try {
            SQUARE_DISTANCE_I7U_BULK_WITH_OFFSETS.invokeExact(a, b, length, pitch, offsets, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    public static float cosineI8(MemorySegment a, MemorySegment b, int length) {
        try {
            return (float) COSINE_I8.invokeExact(a, b, length);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void cosineI8Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
        try {
            COSINE_I8_BULK.invokeExact(a, b, length, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void cosineI8BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        try {
            COSINE_I8_BULK_WITH_OFFSETS.invokeExact(a, b, length, pitch, offsets, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    public static float dotProductI8(MemorySegment a, MemorySegment b, int length) {
        try {
            return (float) DOT_PRODUCT_I8.invokeExact(a, b, length);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void dotProductI8Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
        try {
            DOT_PRODUCT_I8_BULK.invokeExact(a, b, length, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void dotProductI8BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        try {
            DOT_PRODUCT_I8_BULK_WITH_OFFSETS.invokeExact(a, b, length, pitch, offsets, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    public static float squareDistanceI8(MemorySegment a, MemorySegment b, int length) {
        try {
            return (float) SQUARE_DISTANCE_I8.invokeExact(a, b, length);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void squareDistanceI8Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
        try {
            SQUARE_DISTANCE_I8_BULK.invokeExact(a, b, length, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void squareDistanceI8BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        try {
            SQUARE_DISTANCE_I8_BULK_WITH_OFFSETS.invokeExact(a, b, length, pitch, offsets, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    public static long dotProductD1Q4(MemorySegment a, MemorySegment query, int length) {
        try {
            return (long) DOT_PRODUCT_D1Q4.invokeExact(a, query, length);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    public static void dotProductD1Q4Bulk(MemorySegment a, MemorySegment query, int length, int count, MemorySegment scores) {
        try {
            DOT_PRODUCT_D1Q4_BULK.invokeExact(a, query, length, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void dotProductD1Q4BulkWithOffsets(
        MemorySegment a,
        MemorySegment query,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        try {
            DOT_PRODUCT_D1Q4_BULK_WITH_OFFSETS.invokeExact(a, query, length, pitch, offsets, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    public static long dotProductD2Q4(MemorySegment a, MemorySegment query, int length) {
        try {
            return (long) DOT_PRODUCT_D2Q4.invokeExact(a, query, length);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    public static void dotProductD2Q4Bulk(MemorySegment a, MemorySegment query, int length, int count, MemorySegment scores) {
        try {
            DOT_PRODUCT_D2Q4_BULK.invokeExact(a, query, length, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void dotProductD2Q4BulkWithOffsets(
        MemorySegment a,
        MemorySegment query,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        try {
            DOT_PRODUCT_D2Q4_BULK_WITH_OFFSETS.invokeExact(a, query, length, pitch, offsets, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    public static long dotProductD4Q4(MemorySegment a, MemorySegment query, int length) {
        try {
            return (long) DOT_PRODUCT_D4Q4.invokeExact(a, query, length);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    public static void dotProductD4Q4Bulk(MemorySegment a, MemorySegment query, int length, int count, MemorySegment scores) {
        try {
            DOT_PRODUCT_D4Q4_BULK.invokeExact(a, query, length, count, scores);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static void dotProductD4Q4BulkWithOffsets(
        MemorySegment a,
        MemorySegment query,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        try {
            DOT_PRODUCT_D4Q4_BULK_WITH_OFFSETS.invokeExact(a, query, length, pitch, offsets, count, scores);
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
