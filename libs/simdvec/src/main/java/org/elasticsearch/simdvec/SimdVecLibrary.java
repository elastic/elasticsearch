/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.elasticsearch.core.SuppressForbidden;
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

import static org.elasticsearch.simdvec.SimdVecChecks.validateBulkOffsets;
import static org.elasticsearch.simdvec.SimdVecChecks.validateBulkSparse;

/**
 * Class providing vector similarity functions.
 */
@LibrarySpecification(
    name = "vec",
    unavailableOn = { Platform.WINDOWS_X64, Platform.DARWIN_X64 },
    symbolResolver = VecCapsSymbolResolver.class
)
public abstract class SimdVecLibrary {

    private static final Logger logger = LogManager.getLogger(SimdVecLibrary.class);

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

    /** Query-side element format for the BFloat16 kernels; the document side is always bfloat16. */
    public enum BFloat16QueryType {
        BFLOAT16,
        FLOAT32
    }

    /**
     * Doc-side data layout for BBQ kernels. {@link #STRIPED} (bit-plane transposition)
     * is the original layout used by all {@code vec_dotdNqM} kernels; {@link #PACKED} groups
     * multiple values into a single byte (K x N-bit doc values per byte, where K x N = 8);
     * The distinction shows up in the native symbol names, which carry no suffix for STRIPED and
     * {@code _packed} for PACKED -- e.g. {@code vec_dotd2q4} versus {@code vec_dotd2q4_packed}.
     */
    public enum Layout {
        STRIPED,
        PACKED
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
    }

    /**
     * System property that opts out of loading the native vector library. Kept under the original
     * {@code nativeaccess} name for backward compatibility with existing deployments.
     */
    static final String ENABLE_JDK_VECTOR_LIBRARY = "org.elasticsearch.nativeaccess.enableVectorLibrary";

    private static final SimdVecLibrary INSTANCE = load();

    /**
     * Returns the native vector library, or an empty {@code Optional} if this host CPU/OS does not
     * support it, or if the user has explicitly disabled it via
     * {@code -D} {@value #ENABLE_JDK_VECTOR_LIBRARY} {@code =false}.
     */
    public static Optional<SimdVecLibrary> instance() {
        return Optional.ofNullable(INSTANCE);
    }

    /** Whether the host CPU/OS/JDK combination can run the native vector library. */
    public static boolean isNativeVectorLibSupported() {
        var supportedPlatform = Platform.current().equals(Platform.DARWIN_AARCH64)
            || Platform.current().equals(Platform.LINUX_AARCH64)
            || Platform.current().equals(Platform.LINUX_X64);
        return Runtime.version().feature() >= 22 && supportedPlatform && checkEnableSystemProperty();
    }

    @SuppressForbidden(
        reason = "TODO Deprecate any lenient usage of Boolean#parseBoolean https://github.com/elastic/elasticsearch/issues/128993"
    )
    private static boolean checkEnableSystemProperty() {
        return Optional.ofNullable(System.getProperty(ENABLE_JDK_VECTOR_LIBRARY)).map(Boolean::valueOf).orElse(Boolean.TRUE);
    }

    private static SimdVecLibrary load() {
        if (isNativeVectorLibSupported() == false) {
            return null;
        }

        int capability = VecCaps.caps();
        if (capability < 0) {
            logger.warn("""
                Your CPU supports vector capabilities, but they are disabled at OS level. For optimal performance, \
                enable them in your OS/Hypervisor/VM/container""");
            return null;
        }

        if (capability == 0) {
            return null;
        }
        var lib = LibraryProvider.lookupLibrary(SimdVecLibrary.class);
        // lookupLibrary must succeed here, we already checked requirements
        // (and loaded the native library to call vec_caps)
        assert lib != null;
        logger.info("Using native vector library; to disable start with -D" + ENABLE_JDK_VECTOR_LIBRARY + "=false");
        return lib;
    }

    // --- INT7U: dot product and square distance ---
    //
    // Both the dataset and the query hold one unsigned 7-bit value per byte, so every segment is
    // `length` bytes wide and every `elementBits` below is Byte.SIZE.

    @Function("vec_doti7u")
    @Critical
    public abstract int dotProductI7u(
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_doti7u_bulk")
    @Critical
    public abstract void dotProductI7uBulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_doti7u_bulk_offsets")
    @Critical
    protected abstract void dotProductI7uBulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment query,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductI7uBulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        if (pitch < length) {
            throw new IllegalArgumentException("Pitch needs to be at least " + length);
        }
        assert validateBulkOffsets(dataset, offsets, count, pitch, length);
        dotProductI7uBulkWithOffsets_raw(dataset, query, length, pitch, offsets, count, scores);
    }

    @Function("vec_doti7u_bulk_sparse")
    @Critical
    protected abstract void dotProductI7uBulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductI7uBulkSparse(MemorySegment addresses, MemorySegment query, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        dotProductI7uBulkSparse_raw(addresses, query, length, count, scores);
    }

    @Function("vec_sqri7u")
    @Critical
    public abstract int squareDistanceI7u(
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_sqri7u_bulk")
    @Critical
    public abstract void squareDistanceI7uBulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_sqri7u_bulk_offsets")
    @Critical
    protected abstract void squareDistanceI7uBulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment query,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceI7uBulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        if (pitch < length) {
            throw new IllegalArgumentException("Pitch needs to be at least " + length);
        }
        assert validateBulkOffsets(dataset, offsets, count, pitch, length);
        squareDistanceI7uBulkWithOffsets_raw(dataset, query, length, pitch, offsets, count, scores);
    }

    @Function("vec_sqri7u_bulk_sparse")
    @Critical
    protected abstract void squareDistanceI7uBulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceI7uBulkSparse(MemorySegment addresses, MemorySegment query, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        squareDistanceI7uBulkSparse_raw(addresses, query, length, count, scores);
    }

    // --- INT4: dot product only ---
    //
    // Int4 is asymmetric: the query holds one value per byte (`2 * documentBytes`) while the document
    // packs two nibbles per byte (`documentBytes`). `documentBytes` is the *packed* byte count, not the
    // logical dimension count. Note the single-vector form takes the query first and the packed document
    // second.

    @Function("vec_doti4")
    @Critical
    public abstract int dotProductI4(
        @VectorSegment(countParam = "documentBytes", elementBits = 2 * Byte.SIZE) MemorySegment query,
        @VectorSegment(countParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment document,
        int documentBytes
    );

    @Function("vec_doti4_bulk")
    @Critical
    public abstract void dotProductI4Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "documentBytes", elementBits = 2 * Byte.SIZE) MemorySegment query,
        int documentBytes,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_doti4_bulk_offsets")
    @Critical
    protected abstract void dotProductI4BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "documentBytes", elementBits = 2 * Byte.SIZE) MemorySegment query,
        int documentBytes,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductI4BulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
        int documentBytes,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        if (pitch < documentBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + documentBytes);
        }
        assert validateBulkOffsets(dataset, offsets, count, pitch, documentBytes);
        dotProductI4BulkWithOffsets_raw(dataset, query, documentBytes, pitch, offsets, count, scores);
    }

    @Function("vec_doti4_bulk_sparse")
    @Critical
    protected abstract void dotProductI4BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "documentBytes", elementBits = 2 * Byte.SIZE) MemorySegment query,
        int documentBytes,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductI4BulkSparse(MemorySegment addresses, MemorySegment query, int documentBytes, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        dotProductI4BulkSparse_raw(addresses, query, documentBytes, count, scores);
    }

    // --- INT8: cosine, dot product, square distance ---
    //
    // One signed byte per value on both sides, so every segment is `length` bytes wide.

    @Function("vec_cosi8")
    @Critical
    public abstract float cosineI8(
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_cosi8_bulk")
    @Critical
    public abstract void cosineI8Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_cosi8_bulk_offsets")
    @Critical
    protected abstract void cosineI8BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment query,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void cosineI8BulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        if (pitch < length) {
            throw new IllegalArgumentException("Pitch needs to be at least " + length);
        }
        assert validateBulkOffsets(dataset, offsets, count, pitch, length);
        cosineI8BulkWithOffsets_raw(dataset, query, length, pitch, offsets, count, scores);
    }

    @Function("vec_cosi8_bulk_sparse")
    @Critical
    protected abstract void cosineI8BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void cosineI8BulkSparse(MemorySegment addresses, MemorySegment query, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        cosineI8BulkSparse_raw(addresses, query, length, count, scores);
    }

    @Function("vec_doti8")
    @Critical
    public abstract float dotProductI8(
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_doti8_bulk")
    @Critical
    public abstract void dotProductI8Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_doti8_bulk_offsets")
    @Critical
    protected abstract void dotProductI8BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment query,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductI8BulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        if (pitch < length) {
            throw new IllegalArgumentException("Pitch needs to be at least " + length);
        }
        assert validateBulkOffsets(dataset, offsets, count, pitch, length);
        dotProductI8BulkWithOffsets_raw(dataset, query, length, pitch, offsets, count, scores);
    }

    @Function("vec_doti8_bulk_sparse")
    @Critical
    protected abstract void dotProductI8BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductI8BulkSparse(MemorySegment addresses, MemorySegment query, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        dotProductI8BulkSparse_raw(addresses, query, length, count, scores);
    }

    @Function("vec_sqri8")
    @Critical
    public abstract float squareDistanceI8(
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_sqri8_bulk")
    @Critical
    public abstract void squareDistanceI8Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_sqri8_bulk_offsets")
    @Critical
    protected abstract void squareDistanceI8BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment query,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceI8BulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
        int length,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        if (pitch < length) {
            throw new IllegalArgumentException("Pitch needs to be at least " + length);
        }
        assert validateBulkOffsets(dataset, offsets, count, pitch, length);
        squareDistanceI8BulkWithOffsets_raw(dataset, query, length, pitch, offsets, count, scores);
    }

    @Function("vec_sqri8_bulk_sparse")
    @Critical
    protected abstract void squareDistanceI8BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Byte.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceI8BulkSparse(MemorySegment addresses, MemorySegment query, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        squareDistanceI8BulkSparse_raw(addresses, query, length, count, scores);
    }

    // --- FLOAT32: dot product and square distance ---
    //
    // Four bytes per value on both sides.

    @Function("vec_dotf32")
    @Critical
    public abstract float dotProductF32(
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_dotf32_bulk")
    @Critical
    public abstract void dotProductF32Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Float.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_dotf32_bulk_sparse")
    @Critical
    protected abstract void dotProductF32BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductF32BulkSparse(MemorySegment addresses, MemorySegment query, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        dotProductF32BulkSparse_raw(addresses, query, length, count, scores);
    }

    @Function("vec_dotf32_bulk_offsets")
    @Critical
    protected abstract void dotProductF32BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment query,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductF32BulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
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
        assert validateBulkOffsets(dataset, offsets, count, pitch, rowBytes);
        dotProductF32BulkWithOffsets_raw(dataset, query, length, pitch, offsets, count, scores);
    }

    @Function("vec_sqrf32")
    @Critical
    public abstract float squareDistanceF32(
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment a,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment b,
        int length
    );

    @Function("vec_sqrf32_bulk")
    @Critical
    public abstract void squareDistanceF32Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Float.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_sqrf32_bulk_offsets")
    @Critical
    protected abstract void squareDistanceF32BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment query,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceF32BulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
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
        assert validateBulkOffsets(dataset, offsets, count, pitch, rowBytes);
        squareDistanceF32BulkWithOffsets_raw(dataset, query, length, pitch, offsets, count, scores);
    }

    @Function("vec_sqrf32_bulk_sparse")
    @Critical
    protected abstract void squareDistanceF32BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceF32BulkSparse(MemorySegment addresses, MemorySegment query, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        squareDistanceF32BulkSparse_raw(addresses, query, length, count, scores);
    }

    // --- BFloat16 ---
    //
    // The document always holds two bytes per value; the query holds either four bytes (QF32) or two
    // (QBF16), as spelled out in the `elementBits` of each kernel's query parameter below.

    @Function("vec_dotDbf16Qf32")
    @Critical
    public abstract float dotProductDBF16QF32(
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment document,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment query,
        int length
    );

    @Function("vec_dotDbf16Qf32_bulk")
    @Critical
    public abstract void dotProductDBF16QF32Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Short.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_dotDbf16Qf32_bulk_sparse")
    @Critical
    protected abstract void dotProductDBF16QF32BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductDBF16QF32BulkSparse(MemorySegment addresses, MemorySegment query, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        dotProductDBF16QF32BulkSparse_raw(addresses, query, length, count, scores);
    }

    @Function("vec_dotDbf16Qf32_bulk_offsets")
    @Critical
    protected abstract void dotProductDBF16QF32BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment query,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductDBF16QF32BulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
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
        assert validateBulkOffsets(dataset, offsets, count, pitch, rowBytes);
        dotProductDBF16QF32BulkWithOffsets_raw(dataset, query, length, pitch, offsets, count, scores);
    }

    @Function("vec_sqrDbf16Qf32")
    @Critical
    public abstract float squareDistanceDBF16QF32(
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment document,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment query,
        int length
    );

    @Function("vec_sqrDbf16Qf32_bulk")
    @Critical
    public abstract void squareDistanceDBF16QF32Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Short.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_sqrDbf16Qf32_bulk_sparse")
    @Critical
    protected abstract void squareDistanceDBF16QF32BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceDBF16QF32BulkSparse(
        MemorySegment addresses,
        MemorySegment query,
        int length,
        int count,
        MemorySegment scores
    ) {
        assert validateBulkSparse(addresses, count);
        squareDistanceDBF16QF32BulkSparse_raw(addresses, query, length, count, scores);
    }

    @Function("vec_sqrDbf16Qf32_bulk_offsets")
    @Critical
    protected abstract void squareDistanceDBF16QF32BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Float.SIZE) MemorySegment query,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceDBF16QF32BulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
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
        assert validateBulkOffsets(dataset, offsets, count, pitch, rowBytes);
        squareDistanceDBF16QF32BulkWithOffsets_raw(dataset, query, length, pitch, offsets, count, scores);
    }

    @Function("vec_dotDbf16Qbf16")
    @Critical
    public abstract float dotProductDBF16QBF16(
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment document,
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment query,
        int length
    );

    @Function("vec_dotDbf16Qbf16_bulk")
    @Critical
    public abstract void dotProductDBF16QBF16Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Short.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_dotDbf16Qbf16_bulk_sparse")
    @Critical
    protected abstract void dotProductDBF16QBF16BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductDBF16QBF16BulkSparse(MemorySegment addresses, MemorySegment query, int length, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        dotProductDBF16QBF16BulkSparse_raw(addresses, query, length, count, scores);
    }

    @Function("vec_dotDbf16Qbf16_bulk_offsets")
    @Critical
    protected abstract void dotProductDBF16QBF16BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment query,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductDBF16QBF16BulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
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
        assert validateBulkOffsets(dataset, offsets, count, pitch, rowBytes);
        dotProductDBF16QBF16BulkWithOffsets_raw(dataset, query, length, pitch, offsets, count, scores);
    }

    @Function("vec_sqrDbf16Qbf16")
    @Critical
    public abstract float squareDistanceDBF16QBF16(
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment document,
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment query,
        int length
    );

    @Function("vec_sqrDbf16Qbf16_bulk")
    @Critical
    public abstract void squareDistanceDBF16QBF16Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = Short.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_sqrDbf16Qbf16_bulk_sparse")
    @Critical
    protected abstract void squareDistanceDBF16QBF16BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment query,
        int length,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceDBF16QBF16BulkSparse(
        MemorySegment addresses,
        MemorySegment query,
        int length,
        int count,
        MemorySegment scores
    ) {
        assert validateBulkSparse(addresses, count);
        squareDistanceDBF16QBF16BulkSparse_raw(addresses, query, length, count, scores);
    }

    @Function("vec_sqrDbf16Qbf16_bulk_offsets")
    @Critical
    protected abstract void squareDistanceDBF16QBF16BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "length", elementBits = Short.SIZE) MemorySegment query,
        int length,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void squareDistanceDBF16QBF16BulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
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
        assert validateBulkOffsets(dataset, offsets, count, pitch, rowBytes);
        squareDistanceDBF16QBF16BulkWithOffsets_raw(dataset, query, length, pitch, offsets, count, scores);
    }

    // --- BBQ: dot product for all BBQ types ---
    //
    // `documentBytes` is the document vector size in bytes. The query width is a fixed multiple of it:
    // x1 for D1Q1/D2Q2/D4Q4, x2 for D2Q4, x4 for D1Q4/D2Q4_PACKED. Each multiple is spelled out in the
    // `elementBits` of that kernel's query parameter below.

    @Function("vec_dotd1q1")
    @Critical
    public abstract long dotProductD1Q1(
        @VectorSegment(countParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment document,
        @VectorSegment(countParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment query,
        int documentBytes
    );

    @Function("vec_dotd1q1_bulk")
    @Critical
    public abstract void dotProductD1Q1Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment query,
        int documentBytes,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_dotd1q1_bulk_offsets")
    @Critical
    protected abstract void dotProductD1Q1BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment query,
        int documentBytes,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductD1Q1BulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
        int documentBytes,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        if (pitch < documentBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + documentBytes);
        }
        assert validateBulkOffsets(dataset, offsets, count, pitch, documentBytes);
        dotProductD1Q1BulkWithOffsets_raw(dataset, query, documentBytes, pitch, offsets, count, scores);
    }

    @Function("vec_dotd1q4")
    @Critical
    public abstract long dotProductD1Q4(
        @VectorSegment(countParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment document,
        @VectorSegment(countParam = "documentBytes", elementBits = 4 * Byte.SIZE) MemorySegment query,
        int documentBytes
    );

    @Function("vec_dotd1q4_bulk")
    @Critical
    public abstract void dotProductD1Q4Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "documentBytes", elementBits = 4 * Byte.SIZE) MemorySegment query,
        int documentBytes,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_dotd1q4_bulk_offsets")
    @Critical
    protected abstract void dotProductD1Q4BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "documentBytes", elementBits = 4 * Byte.SIZE) MemorySegment query,
        int documentBytes,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductD1Q4BulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
        int documentBytes,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        if (pitch < documentBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + documentBytes);
        }
        assert validateBulkOffsets(dataset, offsets, count, pitch, documentBytes);
        dotProductD1Q4BulkWithOffsets_raw(dataset, query, documentBytes, pitch, offsets, count, scores);
    }

    @Function("vec_dotd1q4_bulk_sparse")
    @Critical
    protected abstract void dotProductD1Q4BulkSparse_raw(
        @VectorSegment(countParam = "count", elementBits = Long.SIZE, aligned = true) MemorySegment addresses,
        @VectorSegment(countParam = "documentBytes", elementBits = 4 * Byte.SIZE) MemorySegment query,
        int documentBytes,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductD1Q4BulkSparse(MemorySegment addresses, MemorySegment query, int documentBytes, int count, MemorySegment scores) {
        assert validateBulkSparse(addresses, count);
        dotProductD1Q4BulkSparse_raw(addresses, query, documentBytes, count, scores);
    }

    @Function("vec_dotd2q2")
    @Critical
    public abstract long dotProductD2Q2(
        @VectorSegment(countParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment document,
        @VectorSegment(countParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment query,
        int documentBytes
    );

    @Function("vec_dotd2q2_bulk")
    @Critical
    public abstract void dotProductD2Q2Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment query,
        int documentBytes,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_dotd2q2_bulk_offsets")
    @Critical
    protected abstract void dotProductD2Q2BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment query,
        int documentBytes,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductD2Q2BulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
        int documentBytes,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        if (pitch < documentBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + documentBytes);
        }
        assert validateBulkOffsets(dataset, offsets, count, pitch, documentBytes);
        dotProductD2Q2BulkWithOffsets_raw(dataset, query, documentBytes, pitch, offsets, count, scores);
    }

    @Function("vec_dotd2q4")
    @Critical
    public abstract long dotProductD2Q4(
        @VectorSegment(countParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment document,
        @VectorSegment(countParam = "documentBytes", elementBits = 2 * Byte.SIZE) MemorySegment query,
        int documentBytes
    );

    @Function("vec_dotd2q4_bulk")
    @Critical
    public abstract void dotProductD2Q4Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "documentBytes", elementBits = 2 * Byte.SIZE) MemorySegment query,
        int documentBytes,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_dotd2q4_bulk_offsets")
    @Critical
    protected abstract void dotProductD2Q4BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "documentBytes", elementBits = 2 * Byte.SIZE) MemorySegment query,
        int documentBytes,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductD2Q4BulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
        int documentBytes,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        if (pitch < documentBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + documentBytes);
        }
        assert validateBulkOffsets(dataset, offsets, count, pitch, documentBytes);
        dotProductD2Q4BulkWithOffsets_raw(dataset, query, documentBytes, pitch, offsets, count, scores);
    }

    @Function("vec_dotd2q4_packed")
    @Critical
    public abstract long dotProductD2Q4Packed(
        @VectorSegment(countParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment document,
        @VectorSegment(countParam = "documentBytes", elementBits = 4 * Byte.SIZE) MemorySegment query,
        int documentBytes
    );

    @Function("vec_dotd2q4_packed_bulk")
    @Critical
    public abstract void dotProductD2Q4PackedBulk(
        @MatrixSegment(rowsParam = "count", colsParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "documentBytes", elementBits = 4 * Byte.SIZE) MemorySegment query,
        int documentBytes,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_dotd2q4_packed_bulk_offsets")
    @Critical
    protected abstract void dotProductD2Q4PackedBulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "documentBytes", elementBits = 4 * Byte.SIZE) MemorySegment query,
        int documentBytes,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductD2Q4PackedBulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
        int documentBytes,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        if (pitch < documentBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + documentBytes);
        }
        assert validateBulkOffsets(dataset, offsets, count, pitch, documentBytes);
        dotProductD2Q4PackedBulkWithOffsets_raw(dataset, query, documentBytes, pitch, offsets, count, scores);
    }

    @Function("vec_dotd4q4")
    @Critical
    public abstract long dotProductD4Q4(
        @VectorSegment(countParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment document,
        @VectorSegment(countParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment query,
        int documentBytes
    );

    @Function("vec_dotd4q4_bulk")
    @Critical
    public abstract void dotProductD4Q4Bulk(
        @MatrixSegment(rowsParam = "count", colsParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment query,
        int documentBytes,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE) MemorySegment scores
    );

    @Function("vec_dotd4q4_bulk_offsets")
    @Critical
    protected abstract void dotProductD4Q4BulkWithOffsets_raw(
        @MatrixSegment(rowsParam = "count", colsParam = "pitch", elementBits = Byte.SIZE) MemorySegment dataset,
        @VectorSegment(countParam = "documentBytes", elementBits = Byte.SIZE) MemorySegment query,
        int documentBytes,
        int pitch,
        @VectorSegment(countParam = "count", elementBits = Integer.SIZE, aligned = true) MemorySegment offsets,
        int count,
        @VectorSegment(countParam = "count", elementBits = Float.SIZE, aligned = true) MemorySegment scores
    );

    public void dotProductD4Q4BulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
        int documentBytes,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment scores
    ) {
        if (pitch < documentBytes) {
            throw new IllegalArgumentException("Pitch needs to be at least " + documentBytes);
        }
        assert validateBulkOffsets(dataset, offsets, count, pitch, documentBytes);
        dotProductD4Q4BulkWithOffsets_raw(dataset, query, documentBytes, pitch, offsets, count, scores);
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
    @Critical
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
    @Critical
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
    @Critical
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
    @Critical
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
    @Critical
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
    @Critical
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
