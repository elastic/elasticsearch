/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

/**
 * Sealed hierarchy capturing pipeline stage specifications.
 *
 * <p>Each record represents a stage type and can hold stage-specific parameters
 * (e.g., {@code maxError} for lossy stages). References {@link StageId} as the
 * source of truth for persisted byte identifiers.
 */
public sealed interface StageSpec {

    /**
     * Returns the persisted stage identifier for this specification.
     *
     * @return the {@link StageId} for this stage
     */
    StageId stageId();

    /** Delta encoding: stores differences between consecutive values. */
    record DeltaStage() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.DELTA_STAGE;
        }
    }

    /** Offset removal: subtracts the minimum value from all entries. */
    record OffsetStage() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.OFFSET_STAGE;
        }
    }

    /** GCD factoring: divides all values by their greatest common divisor. */
    record GcdStage() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.GCD_STAGE;
        }
    }

    /** Patched PFor: frame-of-reference with exception patching for outliers. */
    record PatchedPForStage() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.PATCHED_PFOR_STAGE;
        }
    }

    /** XOR transform: stores XOR of consecutive values (for floating-point). */
    record XorStage() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.XOR_STAGE;
        }
    }

    /** ALP transform for double values with optional lossy quantization. */
    record AlpDoubleStage(double maxError) implements StageSpec {
        public AlpDoubleStage {
            validateMaxError(maxError);
        }

        public AlpDoubleStage() {
            this(0);
        }

        @Override
        public StageId stageId() {
            return StageId.ALP_DOUBLE_STAGE;
        }
    }

    /** ALP transform for float values with optional lossy quantization. */
    record AlpFloatStage(double maxError) implements StageSpec {
        public AlpFloatStage {
            validateMaxError(maxError);
        }

        public AlpFloatStage() {
            this(0);
        }

        @Override
        public StageId stageId() {
            return StageId.ALP_FLOAT_STAGE;
        }
    }

    /** FPC (Fast Predictive Coding) transform for double values. */
    record FpcDoubleStage(int tableSize, double maxError) implements StageSpec {
        public FpcDoubleStage {
            validateTableSize(tableSize);
            validateMaxError(maxError);
        }

        public FpcDoubleStage() {
            this(0, 0);
        }

        public FpcDoubleStage(int tableSize) {
            this(tableSize, 0);
        }

        @Override
        public StageId stageId() {
            return StageId.FPC_DOUBLE_STAGE;
        }
    }

    /** FPC (Fast Predictive Coding) transform for float values. */
    record FpcFloatStage(int tableSize, double maxError) implements StageSpec {
        public FpcFloatStage {
            validateTableSize(tableSize);
            validateMaxError(maxError);
        }

        public FpcFloatStage() {
            this(0, 0);
        }

        public FpcFloatStage(int tableSize) {
            this(tableSize, 0);
        }

        @Override
        public StageId stageId() {
            return StageId.FPC_FLOAT_STAGE;
        }
    }

    /** Bit-packing payload: packs values using the minimum number of bits. */
    record BitPackPayload() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.BITPACK_PAYLOAD;
        }
    }

    /** Zstandard block compression payload. */
    record ZstdPayload(int compressionLevel) implements StageSpec {
        static final int DEFAULT_COMPRESSION_LEVEL = 3;
        static final int MIN_COMPRESSION_LEVEL = 1;
        static final int MAX_COMPRESSION_LEVEL = 22;

        public ZstdPayload {
            if (compressionLevel < MIN_COMPRESSION_LEVEL || compressionLevel > MAX_COMPRESSION_LEVEL) {
                throw new IllegalArgumentException(
                    "compressionLevel must be between " + MIN_COMPRESSION_LEVEL + " and " + MAX_COMPRESSION_LEVEL + ", got: "
                        + compressionLevel
                );
            }
        }

        public ZstdPayload() {
            this(DEFAULT_COMPRESSION_LEVEL);
        }

        @Override
        public StageId stageId() {
            return StageId.ZSTD_PAYLOAD;
        }
    }

    /** LZ4 block compression payload. */
    record Lz4Payload(boolean highCompression) implements StageSpec {
        public Lz4Payload() {
            this(false);
        }

        @Override
        public StageId stageId() {
            return StageId.LZ4_PAYLOAD;
        }
    }

    /** Gorilla payload for double values (XOR-based bit-level encoding). */
    record GorillaDoublePayload(double maxError) implements StageSpec {
        public GorillaDoublePayload {
            validateMaxError(maxError);
        }

        public GorillaDoublePayload() {
            this(0);
        }

        @Override
        public StageId stageId() {
            return StageId.GORILLA_DOUBLE_PAYLOAD;
        }
    }

    /** Gorilla payload for float values. */
    record GorillaFloatPayload() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.GORILLA_FLOAT_PAYLOAD;
        }
    }

    /** Chimp payload for double values (improved Gorilla variant). */
    record ChimpDoublePayload(double maxError) implements StageSpec {
        public ChimpDoublePayload {
            validateMaxError(maxError);
        }

        public ChimpDoublePayload() {
            this(0);
        }

        @Override
        public StageId stageId() {
            return StageId.CHIMP_DOUBLE_PAYLOAD;
        }
    }

    /** Chimp payload for float values. */
    record ChimpFloatPayload() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.CHIMP_FLOAT_PAYLOAD;
        }
    }

    /** Chimp128 payload for double values (128-bit window variant). */
    record Chimp128DoublePayload(double maxError) implements StageSpec {
        public Chimp128DoublePayload {
            validateMaxError(maxError);
        }

        public Chimp128DoublePayload() {
            this(0);
        }

        @Override
        public StageId stageId() {
            return StageId.CHIMP128_DOUBLE_PAYLOAD;
        }
    }

    /** Chimp128 payload for float values. */
    record Chimp128FloatPayload() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.CHIMP128_FLOAT_PAYLOAD;
        }
    }

    private static void validateMaxError(double maxError) {
        if (maxError < 0 || Double.isFinite(maxError) == false) {
            throw new IllegalArgumentException("maxError must be non-negative and finite, got: " + maxError);
        }
    }

    private static void validateTableSize(int tableSize) {
        if (tableSize < 0) {
            throw new IllegalArgumentException("tableSize must be non-negative, got: " + tableSize);
        }
    }
}
