/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.simdvec.MultiBFloat16VectorsSource;
import org.elasticsearch.simdvec.MultiByteVectorsSource;
import org.elasticsearch.simdvec.MultiFloatVectorsSource;
import org.elasticsearch.simdvec.MultiVectorsSource;

import java.lang.foreign.MemorySegment;

public final class Native22ESVectorUtilSupport extends PanamaESVectorUtilSupport {

    private static final VectorSimilarityFunctions DISTANCE_FUNCS = NativeAccess.instance()
        .getVectorSimilarityFunctions()
        .orElseThrow(AssertionError::new);

    /*
     * This is technically separate to the Panama22 implementation, but there's
     * only a few things which are native-only, it's easiest to inherit implementations
     * and override the ones we want, rather than delegating all methods in the interface.
     */

    @Override
    public float dotProduct(float[] a, float[] b) {
        return DISTANCE_FUNCS.dotProductF32(MemorySegment.ofArray(a), MemorySegment.ofArray(b), a.length);
    }

    @Override
    public float dotProduct(float[] a, float[] b, int offset, int length) {
        return DISTANCE_FUNCS.dotProductF32(
            MemorySegment.ofArray(a).asSlice((long) offset * Float.BYTES, (long) length * Float.BYTES),
            MemorySegment.ofArray(b).asSlice((long) offset * Float.BYTES, (long) length * Float.BYTES),
            length
        );
    }

    @Override
    public float squareDistance(float[] a, float[] b) {
        return DISTANCE_FUNCS.squareDistanceF32(MemorySegment.ofArray(a), MemorySegment.ofArray(b), a.length);
    }

    @Override
    public float squareDistance(float[] a, float[] b, int offset, int length) {
        return DISTANCE_FUNCS.squareDistanceF32(
            MemorySegment.ofArray(a).asSlice((long) offset * Float.BYTES, (long) length * Float.BYTES),
            MemorySegment.ofArray(b).asSlice((long) offset * Float.BYTES, (long) length * Float.BYTES),
            length
        );
    }

    @Override
    public float cosine(byte[] a, byte[] b) {
        return DISTANCE_FUNCS.cosineI8(MemorySegment.ofArray(a), MemorySegment.ofArray(b), a.length);
    }

    @Override
    public float dotProduct(byte[] a, byte[] b) {
        return DISTANCE_FUNCS.dotProductI8(MemorySegment.ofArray(a), MemorySegment.ofArray(b), a.length);
    }

    @Override
    public float dotProduct(byte[] a, byte[] b, int offset, int length) {
        return DISTANCE_FUNCS.dotProductI8(
            MemorySegment.ofArray(a).asSlice(offset, length),
            MemorySegment.ofArray(b).asSlice(offset, length),
            length
        );
    }

    @Override
    public float squareDistance(byte[] a, byte[] b) {
        return DISTANCE_FUNCS.squareDistanceI8(MemorySegment.ofArray(a), MemorySegment.ofArray(b), a.length);
    }

    @Override
    public float squareDistance(byte[] a, byte[] b, int offset, int length) {
        return DISTANCE_FUNCS.squareDistanceI8(
            MemorySegment.ofArray(a).asSlice(offset, length),
            MemorySegment.ofArray(b).asSlice(offset, length),
            length
        );
    }

    @Override
    public float maxSimDotProduct(MultiFloatVectorsSource source, float[][] query, float[] scoresScratch) {
        if (canUseF32BulkPath(source)) {
            final BytesRef vectors = source.vectorBytes();
            final MemorySegment vectorsSegment = MemorySegment.ofArray(vectors.bytes)
                .asSlice(vectors.offset, (long) source.vectorByteSize() * source.vectorCount());
            final MemorySegment scoresSegment = MemorySegment.ofArray(scoresScratch);
            float sum = 0f;
            for (float[] floats : query) {
                DISTANCE_FUNCS.dotProductF32Bulk(
                    vectorsSegment,
                    MemorySegment.ofArray(floats),
                    source.vectorDims(),
                    source.vectorCount(),
                    scoresSegment
                );
                sum += ESVectorUtil.max(scoresScratch, source.vectorCount());
            }
            return sum;
        }
        return super.maxSimDotProduct(source, query, scoresScratch);
    }

    @Override
    public float maxSimDotProduct(MultiBFloat16VectorsSource source, float[][] query, float[] scoresScratch) {
        if (canUseBFloat16Path(source)) {
            final BytesRef vectors = source.vectorBytes();
            final MemorySegment vectorsSegment = MemorySegment.ofArray(vectors.bytes)
                .asSlice(vectors.offset, (long) source.vectorByteSize() * source.vectorCount());
            final MemorySegment scoresSegment = MemorySegment.ofArray(scoresScratch);
            float sum = 0f;
            for (float[] floats : query) {
                DISTANCE_FUNCS.dotProductDBF16QF32Bulk(
                    vectorsSegment,
                    MemorySegment.ofArray(floats),
                    source.vectorDims(),
                    source.vectorCount(),
                    scoresSegment
                );
                sum += ESVectorUtil.max(scoresScratch, source.vectorCount());
            }
            return sum;
        }
        return super.maxSimDotProduct(source, query, scoresScratch);
    }

    @Override
    public float maxSimDotProduct(MultiByteVectorsSource source, byte[][] query, float[] scoresScratch) {
        if (canUseI8BulkPath(source)) {
            final BytesRef vectors = source.vectorBytes();
            final MemorySegment vectorsSegment = MemorySegment.ofArray(vectors.bytes)
                .asSlice(vectors.offset, (long) source.vectorByteSize() * source.vectorCount());
            final MemorySegment scoresSegment = MemorySegment.ofArray(scoresScratch);
            float sum = 0f;
            for (byte[] bytes : query) {
                DISTANCE_FUNCS.dotProductI8Bulk(
                    vectorsSegment,
                    MemorySegment.ofArray(bytes),
                    source.vectorDims(),
                    source.vectorCount(),
                    scoresSegment
                );
                sum += ESVectorUtil.max(scoresScratch, source.vectorCount());
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
