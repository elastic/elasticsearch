/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.foreign.Critical;
import org.elasticsearch.foreign.Function;
import org.elasticsearch.foreign.LibraryProvider;
import org.elasticsearch.foreign.LibrarySpecification;
import org.elasticsearch.foreign.MatrixSegment;
import org.elasticsearch.foreign.Platform;
import org.elasticsearch.foreign.VectorSegment;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.lang.foreign.MemorySegment;
import java.util.Optional;

import static org.elasticsearch.nativeaccess.SimdVecChecks.validateBulkOffsets;
import static org.elasticsearch.nativeaccess.SimdVecChecks.validateBulkSparse;

/**
 * Class providing vector similarity functions.
 */
@LibrarySpecification(
    name = "vec",
    unavailableOn = { Platform.WINDOWS_X64, Platform.DARWIN_X64 },
    symbolResolver = VecCapsSymbolResolver.class
)
public abstract class VectorSimilarityFunctions {

    private static final Logger logger = LogManager.getLogger(VectorSimilarityFunctions.class);

    public enum SimilarityFunction {
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

    public enum DataType {
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

    public enum BFloat16QueryType {
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
    public enum Layout {
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
    public enum BBQType {
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

    public enum Operation {
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

    static Optional<VectorSimilarityFunctions> tryLoad() {
        int capability = VecCaps.caps();
        if (capability < 0) {
            logger.warn("""
                Your CPU supports vector capabilities, but they are disabled at OS level. For optimal performance, \
                enable them in your OS/Hypervisor/VM/container""");
            return Optional.empty();
        }

        if (capability == 0) {
            return Optional.empty();
        }
        return Optional.ofNullable(LibraryProvider.lookupLibrary(VectorSimilarityFunctions.class));
    }

    // --- INT7U: dot product and square distance ---
    //
    // Both the dataset and the query hold one unsigned 7-bit value per byte, so every segment is
    // `length` bytes wide and every `elementBits` below is Byte.SIZE.

    @Function("vec_doti7u")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract int dotProductI7u(
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_doti7u_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract void dotProductI7uBulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_doti7u_bulk_offsets")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductI7uBulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductI7uBulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        long rowBytes = length;
        if (pitch < rowBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
        }
        assert validateBulkOffsets(a, offsets, count, pitch, rowBytes);
        dotProductI7uBulkWithOffsets_raw(a, b, length, pitch, offsets, count, scores);
    }

    @Function("vec_doti7u_bulk_sparse")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductI7uBulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductI7uBulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        dotProductI7uBulkSparse_raw(addresses, b, length, count, scores);
    }

    @Function("vec_sqri7u")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract int squareDistanceI7u(
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_sqri7u_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract void squareDistanceI7uBulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_sqri7u_bulk_offsets")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void squareDistanceI7uBulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceI7uBulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        long rowBytes = length;
        if (pitch < rowBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
        }
        assert validateBulkOffsets(a, offsets, count, pitch, rowBytes);
        squareDistanceI7uBulkWithOffsets_raw(a, b, length, pitch, offsets, count, scores);
    }

    @Function("vec_sqri7u_bulk_sparse")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void squareDistanceI7uBulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceI7uBulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        squareDistanceI7uBulkSparse_raw(addresses, b, length, count, scores);
    }

    // --- INT4: dot product only ---
    //
    // Int4 is asymmetric: the query holds one value per byte (`2 * length` bytes) while the dataset
    // packs two nibbles per byte (`length` bytes). `length` is the *packed* byte count, not the
    // logical dimension count. Note the single-vector form takes the
    // query as `a` and the packed dataset as `b`; the bulk forms take the dataset as `a`.

    @Function("vec_doti4")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract int dotProductI4(
        @VectorSegment(countParam = "length", elementBits = 2 * Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_doti4_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract void dotProductI4Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = 2 * Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_doti4_bulk_offsets")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductI4BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = 2 * Byte.SIZE) MemorySegment b,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductI4BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        long rowBytes = length;
        if (pitch < rowBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
        }
        assert validateBulkOffsets(a, offsets, count, pitch, rowBytes);
        dotProductI4BulkWithOffsets_raw(a, b, length, pitch, offsets, count, scores);
    }

    @Function("vec_doti4_bulk_sparse")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductI4BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = 2 * Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductI4BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        dotProductI4BulkSparse_raw(addresses, b, length, count, scores);
    }

    // --- INT8: cosine, dot product, square distance ---
    //
    // One signed byte per value on both sides, so every segment is `length` bytes wide.

    @Function("vec_cosi8")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract float cosineI8(
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_cosi8_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract void cosineI8Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_cosi8_bulk_offsets")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void cosineI8BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void cosineI8BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        long rowBytes = length;
        if (pitch < rowBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
        }
        assert validateBulkOffsets(a, offsets, count, pitch, rowBytes);
        cosineI8BulkWithOffsets_raw(a, b, length, pitch, offsets, count, scores);
    }

    @Function("vec_cosi8_bulk_sparse")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void cosineI8BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void cosineI8BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        cosineI8BulkSparse_raw(addresses, b, length, count, scores);
    }

    @Function("vec_doti8")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract float dotProductI8(
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_doti8_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract void dotProductI8Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_doti8_bulk_offsets")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductI8BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductI8BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        long rowBytes = length;
        if (pitch < rowBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
        }
        assert validateBulkOffsets(a, offsets, count, pitch, rowBytes);
        dotProductI8BulkWithOffsets_raw(a, b, length, pitch, offsets, count, scores);
    }

    @Function("vec_doti8_bulk_sparse")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductI8BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductI8BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        dotProductI8BulkSparse_raw(addresses, b, length, count, scores);
    }

    @Function("vec_sqri8")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract float squareDistanceI8(
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_sqri8_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract void squareDistanceI8Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_sqri8_bulk_offsets")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void squareDistanceI8BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceI8BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        long rowBytes = length;
        if (pitch < rowBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
        }
        assert validateBulkOffsets(a, offsets, count, pitch, rowBytes);
        squareDistanceI8BulkWithOffsets_raw(a, b, length, pitch, offsets, count, scores);
    }

    @Function("vec_sqri8_bulk_sparse")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void squareDistanceI8BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceI8BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        squareDistanceI8BulkSparse_raw(addresses, b, length, count, scores);
    }

    // --- FLOAT32: dot product and square distance ---
    //
    // Four bytes per value on both sides.

    @Function("vec_dotf32")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract float dotProductF32(
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_dotf32_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract void dotProductF32Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Float.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_dotf32_bulk_sparse")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductF32BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductF32BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        dotProductF32BulkSparse_raw(addresses, b, length, count, scores);
    }

    @Function("vec_dotf32_bulk_offsets")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductF32BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment b,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductF32BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        long rowBytes = (long) length * Float.BYTES;
        if (pitch < rowBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
        }
        assert validateBulkOffsets(a, offsets, count, pitch, rowBytes);
        dotProductF32BulkWithOffsets_raw(a, b, length, pitch, offsets, count, scores);
    }

    @Function("vec_sqrf32")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract float squareDistanceF32(
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_sqrf32_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract void squareDistanceF32Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Float.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_sqrf32_bulk_offsets")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void squareDistanceF32BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment b,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceF32BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        long rowBytes = (long) length * Float.BYTES;
        if (pitch < rowBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
        }
        assert validateBulkOffsets(a, offsets, count, pitch, rowBytes);
        squareDistanceF32BulkWithOffsets_raw(a, b, length, pitch, offsets, count, scores);
    }

    @Function("vec_sqrf32_bulk_sparse")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void squareDistanceF32BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceF32BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        squareDistanceF32BulkSparse_raw(addresses, b, length, count, scores);
    }

    // --- BFloat16 ---
    //
    // The dataset always holds two bytes per value; the query holds either four bytes (QF32) or two
    // (QBF16), per BFloat16QueryType#bytes().

    @Function("vec_dotDbf16Qf32")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract float dotProductDBF16QF32(
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_dotDbf16Qf32_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract void dotProductDBF16QF32Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Short.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_dotDbf16Qf32_bulk_sparse")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductDBF16QF32BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductDBF16QF32BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        dotProductDBF16QF32BulkSparse_raw(addresses, b, length, count, scores);
    }

    @Function("vec_dotDbf16Qf32_bulk_offsets")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductDBF16QF32BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment b,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductDBF16QF32BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        long rowBytes = (long) length * Short.BYTES;
        if (pitch < rowBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
        }
        assert validateBulkOffsets(a, offsets, count, pitch, rowBytes);
        dotProductDBF16QF32BulkWithOffsets_raw(a, b, length, pitch, offsets, count, scores);
    }

    @Function("vec_sqrDbf16Qf32")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract float squareDistanceDBF16QF32(
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_sqrDbf16Qf32_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract void squareDistanceDBF16QF32Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Short.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_sqrDbf16Qf32_bulk_sparse")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void squareDistanceDBF16QF32BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceDBF16QF32BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        squareDistanceDBF16QF32BulkSparse_raw(addresses, b, length, count, scores);
    }

    @Function("vec_sqrDbf16Qf32_bulk_offsets")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void squareDistanceDBF16QF32BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment b,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceDBF16QF32BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        long rowBytes = (long) length * Short.BYTES;
        if (pitch < rowBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
        }
        assert validateBulkOffsets(a, offsets, count, pitch, rowBytes);
        squareDistanceDBF16QF32BulkWithOffsets_raw(a, b, length, pitch, offsets, count, scores);
    }

    @Function("vec_dotDbf16Qbf16")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract float dotProductDBF16QBF16(
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_dotDbf16Qbf16_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract void dotProductDBF16QBF16Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Short.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_dotDbf16Qbf16_bulk_sparse")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductDBF16QBF16BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductDBF16QBF16BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        dotProductDBF16QBF16BulkSparse_raw(addresses, b, length, count, scores);
    }

    @Function("vec_dotDbf16Qbf16_bulk_offsets")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductDBF16QBF16BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment b,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductDBF16QBF16BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        long rowBytes = (long) length * Short.BYTES;
        if (pitch < rowBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
        }
        assert validateBulkOffsets(a, offsets, count, pitch, rowBytes);
        dotProductDBF16QBF16BulkWithOffsets_raw(a, b, length, pitch, offsets, count, scores);
    }

    @Function("vec_sqrDbf16Qbf16")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract float squareDistanceDBF16QBF16(
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_sqrDbf16Qbf16_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract void squareDistanceDBF16QBF16Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Short.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_sqrDbf16Qbf16_bulk_sparse")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void squareDistanceDBF16QBF16BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceDBF16QBF16BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        squareDistanceDBF16QBF16BulkSparse_raw(addresses, b, length, count, scores);
    }

    @Function("vec_sqrDbf16Qbf16_bulk_offsets")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void squareDistanceDBF16QBF16BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment b,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceDBF16QBF16BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        long rowBytes = (long) length * Short.BYTES;
        if (pitch < rowBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
        }
        assert validateBulkOffsets(a, offsets, count, pitch, rowBytes);
        squareDistanceDBF16QBF16BulkWithOffsets_raw(a, b, length, pitch, offsets, count, scores);
    }

    // --- BBQ: dot product for all BBQ types ---
    //
    // `length` is the dataset vector size in bytes. The query width is a fixed multiple of it, given
    // by BBQType#queryBytesPerDocByte(): x1 for D1Q1/D2Q2/D4Q4, x2 for D2Q4, x4 for D1Q4/D2Q4_PACKED.

    @Function("vec_dotd1q1")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract long dotProductD1Q1(
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_dotd1q1_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract void dotProductD1Q1Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_dotd1q1_bulk_offsets")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductD1Q1BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductD1Q1BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        long rowBytes = length;
        if (pitch < rowBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
        }
        assert validateBulkOffsets(a, offsets, count, pitch, rowBytes);
        dotProductD1Q1BulkWithOffsets_raw(a, b, length, pitch, offsets, count, scores);
    }

    @Function("vec_dotd1q4")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract long dotProductD1Q4(
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = 4 * Byte.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_dotd1q4_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract void dotProductD1Q4Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = 4 * Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_dotd1q4_bulk_offsets")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductD1Q4BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = 4 * Byte.SIZE) MemorySegment b,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductD1Q4BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        long rowBytes = length;
        if (pitch < rowBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
        }
        assert validateBulkOffsets(a, offsets, count, pitch, rowBytes);
        dotProductD1Q4BulkWithOffsets_raw(a, b, length, pitch, offsets, count, scores);
    }

    @Function("vec_dotd1q4_bulk_sparse")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductD1Q4BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = 4 * Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductD1Q4BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        dotProductD1Q4BulkSparse_raw(addresses, b, length, count, scores);
    }

    @Function("vec_dotd2q2")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract long dotProductD2Q2(
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_dotd2q2_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract void dotProductD2Q2Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_dotd2q2_bulk_offsets")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductD2Q2BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductD2Q2BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        long rowBytes = length;
        if (pitch < rowBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
        }
        assert validateBulkOffsets(a, offsets, count, pitch, rowBytes);
        dotProductD2Q2BulkWithOffsets_raw(a, b, length, pitch, offsets, count, scores);
    }

    @Function("vec_dotd2q4")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract long dotProductD2Q4(
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = 2 * Byte.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_dotd2q4_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract void dotProductD2Q4Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = 2 * Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_dotd2q4_bulk_offsets")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductD2Q4BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = 2 * Byte.SIZE) MemorySegment b,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductD2Q4BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        long rowBytes = length;
        if (pitch < rowBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
        }
        assert validateBulkOffsets(a, offsets, count, pitch, rowBytes);
        dotProductD2Q4BulkWithOffsets_raw(a, b, length, pitch, offsets, count, scores);
    }

    @Function("vec_dotd2q4_packed")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract long dotProductD2Q4Packed(
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = 4 * Byte.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_dotd2q4_packed_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract void dotProductD2Q4PackedBulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = 4 * Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_dotd2q4_packed_bulk_offsets")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductD2Q4PackedBulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = 4 * Byte.SIZE) MemorySegment b,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductD2Q4PackedBulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        long rowBytes = length;
        if (pitch < rowBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
        }
        assert validateBulkOffsets(a, offsets, count, pitch, rowBytes);
        dotProductD2Q4PackedBulkWithOffsets_raw(a, b, length, pitch, offsets, count, scores);
    }

    @Function("vec_dotd4q4")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract long dotProductD4Q4(
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_dotd4q4_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract void dotProductD4Q4Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_dotd4q4_bulk_offsets")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    protected abstract void dotProductD4Q4BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductD4Q4BulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        long rowBytes = length;
        if (pitch < rowBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
        }
        assert validateBulkOffsets(a, offsets, count, pitch, rowBytes);
        dotProductD4Q4BulkWithOffsets_raw(a, b, length, pitch, offsets, count, scores);
    }

    // --- Corrections (DiskBBQ) ---
    //
    // The corrections kernels read a caller-described layout rather than a plain vector, so they carry
    // no segment bounds annotations -- matching the hand-written bindings they replace.

    /**
     * Applies euclidean correction terms to a bulk of raw DiskBBQ dot products.
     *
     * @param corrections the packed per-vector correction terms
     * @param bulkSize    number of vectors in the bulk
     * @param dimensions  logical dimension count
     * @param scores      in/out raw dot products, overwritten with corrected scores
     */
    @Function("diskbbq_apply_corrections_euclidean_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract float applyCorrectionsEuclideanBulk(
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

    /** Maximum-inner-product variant of {@link #applyCorrectionsEuclideanBulk}. */
    @Function("diskbbq_apply_corrections_maximum_inner_product_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract float applyCorrectionsMaxInnerProductBulk(
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

    /** Dot-product variant of {@link #applyCorrectionsEuclideanBulk}. */
    @Function("diskbbq_apply_corrections_dot_product_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract float applyCorrectionsDotProductBulk(
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

    /**
     * Applies euclidean correction terms where the correction terms are stored inline after each
     * vector, rather than in a separate array as in the DiskBBQ layout.
     *
     * @param data                  the entire vector data segment
     * @param vectorSizeInBytes     quantized vector size, excluding the inline corrections
     * @param pitchInBytes          stride from one vector to the next, including inline corrections
     * @param readComponentSumAsInt 0 for the 2-byte component-sum format, 1 for the 4-byte format
     * @param scores                in/out raw dot products, overwritten with corrected scores
     */
    @Function("bbq_apply_corrections_euclidean_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract float bbqApplyCorrectionsEuclideanBulk(
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

    /** Maximum-inner-product variant of {@link #bbqApplyCorrectionsEuclideanBulk}. */
    @Function("bbq_apply_corrections_maximum_inner_product_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract float bbqApplyCorrectionsMaxInnerProductBulk(
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

    /** Dot-product variant of {@link #bbqApplyCorrectionsEuclideanBulk}. */
    @Function("bbq_apply_corrections_dot_product_bulk")
    @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
    public abstract float bbqApplyCorrectionsDotProductBulk(
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
