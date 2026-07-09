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

public class Similarities {

    static final VectorSimilarityFunctions DISTANCE_FUNCS = NativeAccess.instance()
        .getVectorSimilarityFunctions()
        .orElseThrow(AssertionError::new);

    public static int dotProductI7u(MemorySegment a, MemorySegment b, int length) {
        return DISTANCE_FUNCS.dotProductI7u(a, b, length);
    }

    public static void dotProductI7uBulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.dotProductI7uBulk(a, b, length, count, scores);
    }

    public static void dotProductI7uBulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        DISTANCE_FUNCS.dotProductI7uBulkWithOffsets(a, b, length, pitch, offsets, count, scores);
    }

    static int squareDistanceI7u(MemorySegment a, MemorySegment b, int length) {
        return DISTANCE_FUNCS.squareDistanceI7u(a, b, length);
    }

    static void squareDistanceI7uBulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.squareDistanceI7uBulk(a, b, length, count, scores);
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
        DISTANCE_FUNCS.squareDistanceI7uBulkWithOffsets(a, b, length, pitch, offsets, count, scores);
    }

    public static int dotProductI4(MemorySegment unpacked, MemorySegment packed, int packedLen) {
        return DISTANCE_FUNCS.dotProductI4(unpacked, packed, packedLen);
    }

    public static void dotProductI4Bulk(MemorySegment a, MemorySegment b, int packedLen, int count, MemorySegment scores) {
        DISTANCE_FUNCS.dotProductI4Bulk(a, b, packedLen, count, scores);
    }

    public static void dotProductI4BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int packedLen,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        DISTANCE_FUNCS.dotProductI4BulkWithOffsets(a, b, packedLen, pitch, offsets, count, scores);
    }

    static void dotProductI4BulkSparse(MemorySegment addresses, MemorySegment query, int packedLen, int count, MemorySegment scores) {
        DISTANCE_FUNCS.dotProductI4BulkSparse(addresses, query, packedLen, count, scores);
    }

    public static float cosineI8(MemorySegment a, MemorySegment b, int length) {
        return DISTANCE_FUNCS.cosineI8(a, b, length);
    }

    static void cosineI8Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.cosineI8Bulk(a, b, length, count, scores);
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
        DISTANCE_FUNCS.cosineI8BulkWithOffsets(a, b, length, pitch, offsets, count, scores);
    }

    public static float dotProductI8(MemorySegment a, MemorySegment b, int length) {
        return DISTANCE_FUNCS.dotProductI8(a, b, length);
    }

    public static void dotProductI8Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.dotProductI8Bulk(a, b, length, count, scores);
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
        DISTANCE_FUNCS.dotProductI8BulkWithOffsets(a, b, length, pitch, offsets, count, scores);
    }

    public static float squareDistanceI8(MemorySegment a, MemorySegment b, int length) {
        return DISTANCE_FUNCS.squareDistanceI8(a, b, length);
    }

    static void squareDistanceI8Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.squareDistanceI8Bulk(a, b, length, count, scores);
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
        DISTANCE_FUNCS.squareDistanceI8BulkWithOffsets(a, b, length, pitch, offsets, count, scores);
    }

    static void cosineI8BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.cosineI8BulkSparse(addresses, b, length, count, scores);
    }

    static void dotProductI8BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.dotProductI8BulkSparse(addresses, b, length, count, scores);
    }

    static void squareDistanceI8BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.squareDistanceI8BulkSparse(addresses, b, length, count, scores);
    }

    static void dotProductI7uBulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.dotProductI7uBulkSparse(addresses, b, length, count, scores);
    }

    static void squareDistanceI7uBulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.squareDistanceI7uBulkSparse(addresses, b, length, count, scores);
    }

    public static long dotProductD1Q4(MemorySegment a, MemorySegment query, int length) {
        return DISTANCE_FUNCS.dotProductD1Q4(a, query, length);
    }

    public static void dotProductD1Q4Bulk(MemorySegment a, MemorySegment query, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.dotProductD1Q4Bulk(a, query, length, count, scores);
    }

    public static void dotProductD1Q4BulkWithOffsets(
        MemorySegment a,
        MemorySegment query,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        DISTANCE_FUNCS.dotProductD1Q4BulkWithOffsets(a, query, length, pitch, offsets, count, scores);
    }

    public static void dotProductD1Q4BulkSparse(MemorySegment addresses, MemorySegment query, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.dotProductD1Q4BulkSparse(addresses, query, length, count, scores);
    }

    public static long dotProductD1Q1(MemorySegment a, MemorySegment query, int length) {
        return DISTANCE_FUNCS.dotProductD1Q1(a, query, length);
    }

    public static void dotProductD1Q1Bulk(MemorySegment a, MemorySegment query, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.dotProductD1Q1Bulk(a, query, length, count, scores);
    }

    public static void dotProductD1Q1BulkWithOffsets(
        MemorySegment a,
        MemorySegment query,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        DISTANCE_FUNCS.dotProductD1Q1BulkWithOffsets(a, query, length, pitch, offsets, count, scores);
    }

    public static long dotProductD2Q4(MemorySegment a, MemorySegment query, int length) {
        return DISTANCE_FUNCS.dotProductD2Q4(a, query, length);
    }

    public static void dotProductD2Q4Bulk(MemorySegment a, MemorySegment query, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.dotProductD2Q4Bulk(a, query, length, count, scores);
    }

    public static void dotProductD2Q4BulkWithOffsets(
        MemorySegment a,
        MemorySegment query,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        DISTANCE_FUNCS.dotProductD2Q4BulkWithOffsets(a, query, length, pitch, offsets, count, scores);
    }

    public static long dotProductD2Q4Packed(MemorySegment a, MemorySegment query, int length) {
        return DISTANCE_FUNCS.dotProductD2Q4Packed(a, query, length);
    }

    public static void dotProductD2Q4PackedBulk(MemorySegment a, MemorySegment query, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.dotProductD2Q4PackedBulk(a, query, length, count, scores);
    }

    public static void dotProductD2Q4PackedBulkWithOffsets(
        MemorySegment a,
        MemorySegment query,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        DISTANCE_FUNCS.dotProductD2Q4PackedBulkWithOffsets(a, query, length, pitch, offsets, count, scores);
    }

    public static long dotProductD4Q4(MemorySegment a, MemorySegment query, int length) {
        return DISTANCE_FUNCS.dotProductD4Q4(a, query, length);
    }

    public static void dotProductD4Q4Bulk(MemorySegment a, MemorySegment query, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.dotProductD4Q4Bulk(a, query, length, count, scores);
    }

    public static void dotProductD4Q4BulkWithOffsets(
        MemorySegment a,
        MemorySegment query,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        DISTANCE_FUNCS.dotProductD4Q4BulkWithOffsets(a, query, length, pitch, offsets, count, scores);
    }

    public static float dotProductDBF16QF32(MemorySegment a, MemorySegment b, int length) {
        return DISTANCE_FUNCS.dotProductDBF16QF32(a, b, length);
    }

    public static void dotProductDBF16QF32Bulk(
        MemorySegment vectors,
        MemorySegment query,
        int dimensions,
        int count,
        MemorySegment scores
    ) {
        DISTANCE_FUNCS.dotProductDBF16QF32Bulk(vectors, query, dimensions, count, scores);
    }

    static void dotProductDBF16QF32BulkSparse(
        MemorySegment addresses,
        MemorySegment query,
        int dimensions,
        int count,
        MemorySegment scores
    ) {
        DISTANCE_FUNCS.dotProductDBF16QF32BulkSparse(addresses, query, dimensions, count, scores);
    }

    public static float squareDistanceDBF16QF32(MemorySegment a, MemorySegment b, int length) {
        return DISTANCE_FUNCS.squareDistanceDBF16QF32(a, b, length);
    }

    static void squareDistanceDBF16QF32BulkSparse(
        MemorySegment addresses,
        MemorySegment query,
        int dimensions,
        int count,
        MemorySegment scores
    ) {
        DISTANCE_FUNCS.squareDistanceDBF16QF32BulkSparse(addresses, query, dimensions, count, scores);
    }

    public static float dotProductDBF16QBF16(MemorySegment a, MemorySegment b, int length) {
        return DISTANCE_FUNCS.dotProductDBF16QBF16(a, b, length);
    }

    static void dotProductDBF16QBF16BulkSparse(
        MemorySegment addresses,
        MemorySegment query,
        int dimensions,
        int count,
        MemorySegment scores
    ) {
        DISTANCE_FUNCS.dotProductDBF16QBF16BulkSparse(addresses, query, dimensions, count, scores);
    }

    public static float squareDistanceDBF16QBF16(MemorySegment a, MemorySegment b, int length) {
        return DISTANCE_FUNCS.squareDistanceDBF16QBF16(a, b, length);
    }

    static void squareDistanceDBF16QBF16BulkSparse(
        MemorySegment addresses,
        MemorySegment query,
        int dimensions,
        int count,
        MemorySegment scores
    ) {
        DISTANCE_FUNCS.squareDistanceDBF16QBF16BulkSparse(addresses, query, dimensions, count, scores);
    }

    public static float dotProductF32(MemorySegment a, MemorySegment b, int length) {
        return DISTANCE_FUNCS.dotProductF32(a, b, length);
    }

    public static void dotProductF32Bulk(MemorySegment vectors, MemorySegment query, int dimensions, int count, MemorySegment scores) {
        DISTANCE_FUNCS.dotProductF32Bulk(vectors, query, dimensions, count, scores);
    }

    static void dotProductF32BulkSparse(MemorySegment addresses, MemorySegment query, int dimensions, int count, MemorySegment scores) {
        DISTANCE_FUNCS.dotProductF32BulkSparse(addresses, query, dimensions, count, scores);
    }

    public static float squareDistanceF32(MemorySegment a, MemorySegment b, int length) {
        return DISTANCE_FUNCS.squareDistanceF32(a, b, length);
    }

    static void squareDistanceF32BulkSparse(MemorySegment addresses, MemorySegment query, int dimensions, int count, MemorySegment scores) {
        DISTANCE_FUNCS.squareDistanceF32BulkSparse(addresses, query, dimensions, count, scores);
    }
}
