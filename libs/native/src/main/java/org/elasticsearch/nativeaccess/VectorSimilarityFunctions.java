/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import java.lang.foreign.MemorySegment;

/**
 * Utility interface providing vector similarity functions.
 */
public interface VectorSimilarityFunctions {

    enum Function {
        /**
         * Cosine distance (byte vectors only)
         */
        COSINE,
        /**
         * Dot product distance
         */
        DOT_PRODUCT,
        /**
         * Squared Euclidean distance
         */
        SQUARE_DISTANCE
    }

    enum DataType {
        /**
         * Unsigned int7. Single vector score returns results as an int.
         */
        INT7U(Byte.SIZE),
        /**
         * 4-bit packed nibble. Two values per byte; single vector score returns results as an int.
         */
        INT4(4),
        /**
         * 1-byte int. Single vector score returns results as an int.
         */
        INT8(Byte.SIZE),
        /**
         * 4-byte float. Single vector score returns results as a float.
         */
        FLOAT32(Float.SIZE);

        private final int bits;

        DataType(int bits) {
            this.bits = bits;
        }

        public int bits() {
            return bits;
        }
    }

    enum BFloat16QueryType {
        BFLOAT16(Short.BYTES),
        FLOAT32(Float.BYTES);

        private final int bytes;

        BFloat16QueryType(int bytes) {
            this.bytes = bytes;
        }

        public int bytes() {
            return bytes;
        }
    }

    /**
     * Doc-side data layout for BBQ kernels. {@link #STRIPED} (bit-plane transposition)
     * is the original layout used by all {@code vec_dotdNqM} kernels; {@link #PACKED} groups
     * multiple values into a single byte (K x N-bit doc values per byte, where K x N = 8);
     * The native symbol suffix is empty for STRIPED to preserve existing names and
     * {@code _packed} for PACKED, so the two appear as e.g. {@code vec_dotd2q4} and
     * {@code vec_dotd2q4_packed}.
     */
    enum Layout {
        STRIPED(""),
        PACKED("_packed");

        private final String suffix;

        Layout(String suffix) {
            this.suffix = suffix;
        }

        public String suffix() {
            return suffix;
        }
    }

    /**
     * The various flavors of BBQ indices. Single vector score returns results as a long.
     */
    enum BBQType {
        /**
         * 1-bit data, 1-bit queries, bit-plane striped layout.
         */
        D1Q1((byte) 1, (byte) 1, Layout.STRIPED),
        /**
         * 2-bit data, 2-bit queries, bit-plane striped layout.
         */
        D2Q2((byte) 2, (byte) 2, Layout.STRIPED),
        /**
         * 1-bit data, 4-bit queries, bit-plane striped layout.
         */
        D1Q4((byte) 1, (byte) 4, Layout.STRIPED),
        /**
         * 2-bit data, 4-bit queries, bit-plane striped layout.
         */
        D2Q4((byte) 2, (byte) 4, Layout.STRIPED),
        /**
         * 4-bit data, 4-bit queries, bit-plane striped layout.
         */
        D4Q4((byte) 4, (byte) 4, Layout.STRIPED),
        /**
         * 2-bit data, 4-bit queries, packed-quad layout.
         */
        D2Q4_PACKED((byte) 2, (byte) 4, Layout.PACKED);

        private final byte dataBits;
        private final byte queryBits;
        private final Layout layout;

        BBQType(byte dataBits, byte queryBits, Layout layout) {
            this.dataBits = dataBits;
            this.queryBits = queryBits;
            this.layout = layout;
        }

        public byte dataBits() {
            return dataBits;
        }

        public byte queryBits() {
            return queryBits;
        }

        public Layout layout() {
            return layout;
        }

        /**
         * Number of query bytes per doc byte, for buffer-size bounds checks.
         * <ul>
         *   <li>STRIPED: query is bit-plane transposed at {@code queryBits} planes; total query bytes
         *       = {@code dims * queryBits / 8} = {@code docBytes * queryBits / dataBits}
         *       (e.g. D1Q4: x4, D2Q4: x2, D4Q4: x1).</li>
         *   <li>PACKED: query is a flat one-byte-per-value buffer; total query bytes = {@code dims}
         *       = {@code docBytes * 8 / dataBits} (the doc packs {@code 8/dataBits} values per byte --
         *       e.g. D2Q4_PACKED: x4, flat one byte per value).</li>
         * </ul>
         */
        public int queryBytesPerDocByte() {
            return layout == Layout.PACKED ? 8 / dataBits : queryBits() / dataBits;
        }
    }

    enum Operation {
        /**
         * Scores a single vector against another.
         * <p>
         * Method handle takes arguments {@code (MemorySegment, MemorySegment, int)}:
         * <ol>
         *     <li>First vector</li>
         *     <li>Second vector</li>
         *     <li>Number of dimensions, or for bbq, the number of index bytes</li>
         * </ol>
         * Return value type is determined by the {@link DataType}.
         */
        SINGLE,
        /**
         * Scores multiple vectors against a single vector.
         * <p>
         * Method handle takes arguments {@code (MemorySegment, MemorySegment, int, int, MemorySegment}:
         * <ol>
         *     <li>Multiple vectors to score {@code a}</li>
         *     <li>Single vector to score against</li>
         *     <li>Number of dimensions, or for bbq, the number of index bytes</li>
         *     <li>Number of vectors in {@code a}</li>
         *     <li>Score results, as 4-byte floats</li>
         * </ol>
         */
        BULK,
        /**
         * Scores multiple vectors against a single vector, with an offset array to determine the vectors to score.
         * <p>
         * Method handle takes arguments {@code (MemorySegment, MemorySegment, int, int, MemorySegment, int, MemorySegment}:
         * <ol>
         *     <li>Multiple vectors to score</li>
         *     <li>Single vector to score against</li>
         *     <li>Number of dimensions, or for bbq, the number of index bytes</li>
         *     <li>Number of bytes between the start of one vector and the start of the next vector in {@code a}</li>
         *     <li>Array of 4-byte ints containing indices of vectors to score in {@code a}</li>
         *     <li>Number of vectors to score</li>
         *     <li>Score results, as 4-byte floats, in order of iteration through the offset array</li>
         * </ol>
         */
        BULK_OFFSETS,
        /**
         * Scores multiple vectors against a single vector, using an array of direct memory addresses
         * to locate each vector.
         * <p>
         * Method handle takes arguments {@code (MemorySegment, MemorySegment, int, int, MemorySegment)}:
         * <ol>
         *     <li>Array of 8-byte longs containing the native memory address of each vector</li>
         *     <li>Single vector to score against</li>
         *     <li>Number of dimensions, or for bbq, the number of index bytes</li>
         *     <li>Number of vectors to score</li>
         *     <li>Score results, as 4-byte floats</li>
         * </ol>
         */
        BULK_SPARSE
    }

    // --- INT7U: dot product and square distance ---

    int dotProductI7u(MemorySegment a, MemorySegment b, int length);

    void dotProductI7uBulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores);

    void dotProductI7uBulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    );

    void dotProductI7uBulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores);

    int squareDistanceI7u(MemorySegment a, MemorySegment b, int length);

    void squareDistanceI7uBulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores);

    void squareDistanceI7uBulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    );

    void squareDistanceI7uBulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores);

    // --- INT4: dot product only ---

    int dotProductI4(MemorySegment a, MemorySegment b, int length);

    void dotProductI4Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores);

    void dotProductI4BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    );

    void dotProductI4BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores);

    // --- INT8: cosine, dot product, square distance ---

    float cosineI8(MemorySegment a, MemorySegment b, int length);

    void cosineI8Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores);

    void cosineI8BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    );

    void cosineI8BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores);

    float dotProductI8(MemorySegment a, MemorySegment b, int length);

    void dotProductI8Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores);

    void dotProductI8BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    );

    void dotProductI8BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores);

    float squareDistanceI8(MemorySegment a, MemorySegment b, int length);

    void squareDistanceI8Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores);

    void squareDistanceI8BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    );

    void squareDistanceI8BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores);

    // --- FLOAT32: dot product and square distance ---

    float dotProductF32(MemorySegment a, MemorySegment b, int length);

    void dotProductF32Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores);

    void dotProductF32BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores);

    void dotProductF32BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    );

    float squareDistanceF32(MemorySegment a, MemorySegment b, int length);

    void squareDistanceF32Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores);

    void squareDistanceF32BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    );

    void squareDistanceF32BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores);

    // --- BFloat16 ---

    float dotProductDBF16QF32(MemorySegment a, MemorySegment b, int length);

    void dotProductDBF16QF32Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores);

    void dotProductDBF16QF32BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores);

    void dotProductDBF16QF32BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    );

    float squareDistanceDBF16QF32(MemorySegment a, MemorySegment b, int length);

    void squareDistanceDBF16QF32Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores);

    void squareDistanceDBF16QF32BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores);

    void squareDistanceDBF16QF32BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    );

    float dotProductDBF16QBF16(MemorySegment a, MemorySegment b, int length);

    void dotProductDBF16QBF16Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores);

    void dotProductDBF16QBF16BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores);

    void dotProductDBF16QBF16BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    );

    float squareDistanceDBF16QBF16(MemorySegment a, MemorySegment b, int length);

    void squareDistanceDBF16QBF16Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores);

    void squareDistanceDBF16QBF16BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores);

    void squareDistanceDBF16QBF16BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    );

    // --- BBQ: dot product for all BBQ types ---

    long dotProductD1Q1(MemorySegment a, MemorySegment b, int length);

    void dotProductD1Q1Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores);

    void dotProductD1Q1BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    );

    long dotProductD1Q4(MemorySegment a, MemorySegment b, int length);

    void dotProductD1Q4Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores);

    void dotProductD1Q4BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    );

    void dotProductD1Q4BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores);

    long dotProductD2Q2(MemorySegment a, MemorySegment b, int length);

    void dotProductD2Q2Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores);

    void dotProductD2Q2BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    );

    long dotProductD2Q4(MemorySegment a, MemorySegment b, int length);

    void dotProductD2Q4Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores);

    void dotProductD2Q4BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    );

    long dotProductD2Q4Packed(MemorySegment a, MemorySegment b, int length);

    void dotProductD2Q4PackedBulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores);

    void dotProductD2Q4PackedBulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    );

    long dotProductD4Q4(MemorySegment a, MemorySegment b, int length);

    void dotProductD4Q4Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores);

    void dotProductD4Q4BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    );

    // --- Corrections (DiskBBQ) ---

    float applyCorrectionsEuclideanBulk(
        MemorySegment corrections,
        int bulkSize,
        int dimensions,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        float queryBitScale,
        float indexBitScale,
        float centroidDp,
        MemorySegment scores
    );

    float applyCorrectionsMaxInnerProductBulk(
        MemorySegment corrections,
        int bulkSize,
        int dimensions,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        float queryBitScale,
        float indexBitScale,
        float centroidDp,
        MemorySegment scores
    );

    float applyCorrectionsDotProductBulk(
        MemorySegment corrections,
        int bulkSize,
        int dimensions,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        float queryBitScale,
        float indexBitScale,
        float centroidDp,
        MemorySegment scores
    );

    // --- Corrections (BBQ inline layout) ---

    float bbqApplyCorrectionsEuclideanBulk(
        MemorySegment data,
        int bulkSize,
        int vectorSizeInBytes,
        int pitchInBytes,
        int dimensions,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        float queryBitScale,
        float indexBitScale,
        float centroidDp,
        byte readComponentSumAsInt,
        MemorySegment scores
    );

    float bbqApplyCorrectionsMaxInnerProductBulk(
        MemorySegment data,
        int bulkSize,
        int vectorSizeInBytes,
        int pitchInBytes,
        int dimensions,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        float queryBitScale,
        float indexBitScale,
        float centroidDp,
        byte readComponentSumAsInt,
        MemorySegment scores
    );

    float bbqApplyCorrectionsDotProductBulk(
        MemorySegment data,
        int bulkSize,
        int vectorSizeInBytes,
        int pitchInBytes,
        int dimensions,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        float queryBitScale,
        float indexBitScale,
        float centroidDp,
        byte readComponentSumAsInt,
        MemorySegment scores
    );
}
