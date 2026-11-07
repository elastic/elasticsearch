/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.simdvec.MultiBFloat16VectorsSource;
import org.elasticsearch.simdvec.MultiByteVectorsSource;
import org.elasticsearch.simdvec.MultiFloatVectorsSource;
import org.elasticsearch.simdvec.MultiVectorsSource;
import org.elasticsearch.simdvec.SimdVecLibrary;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public final class Native22ESVectorUtilSupport extends PanamaESVectorUtilSupport {

    private static final SimdVecLibrary SIMDVEC = SimdVecLibrary.instance().orElseThrow(AssertionError::new);

    /**
     * Byte budget per heap bulk call in the {@code maxSimDotProduct} paths.
     */
    private static final int HEAP_BULK_BYTES = 512 * 1024;

    /**
     * Columns of the result computed per task. This is a multiple of the native panel width, and wide enough
     * that the native code re-reads the rows of A only once per task, not once per panel.
     */
    private static final int MATRIX_TASK_COLUMNS = 64;

    /*
     * This is technically separate to the Panama22 implementation, but there's
     * only a few things which are native-only, it's easiest to inherit implementations
     * and override the ones we want, rather than delegating all methods in the interface.
     */

    @Override
    public float dotProduct(float[] a, float[] b) {
        return SIMDVEC.dotProductF32(MemorySegment.ofArray(a), MemorySegment.ofArray(b), a.length);
    }

    @Override
    public void matrixVectorMultiply(byte[] a, int rows, int cols, byte[] v, int[] result) {
        SIMDVEC.sgemvI8(MemorySegment.ofArray(a), MemorySegment.ofArray(v), rows, cols, MemorySegment.ofArray(result));
    }

    @Override
    public float dotProduct(float[] a, int aOffset, float[] b, int bOffset, int length) {
        return SIMDVEC.dotProductF32(
            MemorySegment.ofArray(a).asSlice((long) aOffset * Float.BYTES, (long) length * Float.BYTES),
            MemorySegment.ofArray(b).asSlice((long) bOffset * Float.BYTES, (long) length * Float.BYTES),
            length
        );
    }

    @Override
    public float squareDistance(float[] a, float[] b) {
        return SIMDVEC.squareDistanceF32(MemorySegment.ofArray(a), MemorySegment.ofArray(b), a.length);
    }

    @Override
    public float squareDistance(float[] a, float[] b, int offset, int length) {
        return SIMDVEC.squareDistanceF32(
            MemorySegment.ofArray(a).asSlice((long) offset * Float.BYTES, (long) length * Float.BYTES),
            MemorySegment.ofArray(b).asSlice((long) offset * Float.BYTES, (long) length * Float.BYTES),
            length
        );
    }

    @Override
    public float cosine(byte[] a, byte[] b) {
        return SIMDVEC.cosineI8(MemorySegment.ofArray(a), MemorySegment.ofArray(b), a.length);
    }

    @Override
    public float dotProduct(byte[] a, byte[] b) {
        return SIMDVEC.dotProductI8(MemorySegment.ofArray(a), MemorySegment.ofArray(b), a.length);
    }

    @Override
    public float dotProduct(byte[] a, byte[] b, int offset, int length) {
        return SIMDVEC.dotProductI8(
            MemorySegment.ofArray(a).asSlice(offset, length),
            MemorySegment.ofArray(b).asSlice(offset, length),
            length
        );
    }

    @Override
    public float squareDistance(byte[] a, byte[] b) {
        return SIMDVEC.squareDistanceI8(MemorySegment.ofArray(a), MemorySegment.ofArray(b), a.length);
    }

    @Override
    public float squareDistance(byte[] a, byte[] b, int offset, int length) {
        return SIMDVEC.squareDistanceI8(
            MemorySegment.ofArray(a).asSlice(offset, length),
            MemorySegment.ofArray(b).asSlice(offset, length),
            length
        );
    }

    @Override
    public void matrixMultiplyI8(MemorySegment a, MemorySegment b, int m, int k, int n, MemorySegment result, TaskExecutor executor) {
        // the native code adds to the result
        result.fill((byte) 0);

        final int numTasks = (n + MATRIX_TASK_COLUMNS - 1) / MATRIX_TASK_COLUMNS;
        if (executor == null || numTasks < 2) {
            SIMDVEC.sgemmI8(a, b, m, k, n, result, 0, n);
            return;
        }

        List<Callable<Void>> tasks = new ArrayList<>(numTasks);
        for (int colStart = 0; colStart < n; colStart += MATRIX_TASK_COLUMNS) {
            final int start = colStart;
            final int end = Math.min(start + MATRIX_TASK_COLUMNS, n);
            tasks.add(() -> {
                SIMDVEC.sgemmI8(a, b, m, k, n, result, start, end);
                return null;
            });
        }

        try {
            executor.invokeAll(tasks);
        } catch (IOException e) {
            // can't happen
            throw new AssertionError(e);
        }
    }

    @Override
    public float maxSimDotProduct(MultiFloatVectorsSource source, float[][] query, float[] scoresScratch) {
        if (canUseF32BulkPath(source)) {
            final BytesRef vectors = source.vectorBytes();
            final int count = source.vectorCount();
            final int vectorByteSize = source.vectorByteSize();
            final int rowsPerChunk = getRowsPerChunk(vectorByteSize);
            float sum = 0f;
            for (float[] floats : query) {
                for (int row = 0; row < count; row += rowsPerChunk) {
                    SIMDVEC.dotProductF32Bulk(
                        vectors.bytes,
                        vectors.offset + row * vectorByteSize,
                        floats,
                        source.vectorDims(),
                        Math.min(rowsPerChunk, count - row),
                        scoresScratch,
                        row
                    );
                }
                sum += ESVectorUtil.max(scoresScratch, count);
            }
            return sum;
        }
        return super.maxSimDotProduct(source, query, scoresScratch);
    }

    /**
     * Returns the rows (number of vectors) that fit in a chunk.
     * <p>
     * Functions that call into {@link SIMDVEC} {@code critical} downcalls need to be quick
     * ("comparable to a function with an empty body" according to the JDK docs); to avoid
     * unlimited/unknown time, we divide the data these downcalls need to process into fixed
     * chunks of at most {@value #HEAP_BULK_BYTES} bytes; the caller will then perform multiple calls.
     */
    private static int getRowsPerChunk(int vectorByteSize) {
        return Math.max(1, HEAP_BULK_BYTES / vectorByteSize);
    }

    @Override
    public float maxSimDotProduct(MultiBFloat16VectorsSource source, float[][] query, float[] scoresScratch) {
        if (canUseBFloat16Path(source)) {
            final BytesRef vectors = source.vectorBytes();
            final int count = source.vectorCount();
            final int vectorByteSize = source.vectorByteSize();
            final int rowsPerChunk = getRowsPerChunk(vectorByteSize);
            float sum = 0f;
            for (float[] floats : query) {
                for (int row = 0; row < count; row += rowsPerChunk) {
                    SIMDVEC.dotProductDBF16QF32Bulk(
                        vectors.bytes,
                        vectors.offset + row * vectorByteSize,
                        floats,
                        source.vectorDims(),
                        Math.min(rowsPerChunk, count - row),
                        scoresScratch,
                        row
                    );
                }
                sum += ESVectorUtil.max(scoresScratch, count);
            }
            return sum;
        }
        return super.maxSimDotProduct(source, query, scoresScratch);
    }

    @Override
    public float maxSimDotProduct(MultiByteVectorsSource source, byte[][] query, float[] scoresScratch) {
        if (canUseI8BulkPath(source)) {
            final BytesRef vectors = source.vectorBytes();
            final int count = source.vectorCount();
            final int vectorByteSize = source.vectorByteSize();
            final int rowsPerChunk = getRowsPerChunk(vectorByteSize);
            float sum = 0f;
            for (byte[] bytes : query) {
                for (int row = 0; row < count; row += rowsPerChunk) {
                    SIMDVEC.dotProductI8Bulk(
                        vectors.bytes,
                        vectors.offset + row * vectorByteSize,
                        bytes,
                        source.vectorDims(),
                        Math.min(rowsPerChunk, count - row),
                        scoresScratch,
                        row
                    );
                }
                sum += ESVectorUtil.max(scoresScratch, count);
            }
            return sum;
        }
        return super.maxSimDotProduct(source, query, scoresScratch);
    }

    private static boolean canUseBulkPath(MultiVectorsSource<?> source) {
        return source.vectorBytes() != null
            && source.vectorCount() > 0
            && source.vectorBytes().length == source.vectorCount() * source.vectorByteSize();
    }

    private static boolean canUseF32BulkPath(MultiFloatVectorsSource source) {
        return canUseBulkPath(source) && source.vectorByteSize() == source.vectorDims() * Float.BYTES;
    }

    private static boolean canUseBFloat16Path(MultiBFloat16VectorsSource source) {
        return canUseBulkPath(source) && source.vectorByteSize() == source.vectorDims() * Short.BYTES;
    }

    private static boolean canUseI8BulkPath(MultiByteVectorsSource source) {
        return canUseBulkPath(source) && source.vectorByteSize() == source.vectorDims();
    }
}
