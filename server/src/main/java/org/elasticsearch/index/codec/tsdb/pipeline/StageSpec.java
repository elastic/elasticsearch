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
 * <p>Each record represents a stage type. References {@link StageId} as the
 * source of truth for persisted byte identifiers.
 */
public sealed interface StageSpec {

    /**
     * Returns the persisted stage identifier for this specification.
     *
     * @return the {@link StageId} for this stage
     */
    StageId stageId();

    /** Marker for transform stages that can be chained in the pipeline. */
    sealed interface TransformSpec extends StageSpec {}

    /** Marker for terminal payload stages that serialize values to bytes. */
    sealed interface PayloadSpec extends StageSpec {}

    /** Delta encoding: stores differences between consecutive values. */
    record DeltaStage() implements TransformSpec {
        @Override
        public StageId stageId() {
            return StageId.DELTA_STAGE;
        }
    }

    /** Offset removal: subtracts the minimum value from all entries. */
    record OffsetStage() implements TransformSpec {
        @Override
        public StageId stageId() {
            return StageId.OFFSET_STAGE;
        }
    }

    /** GCD factoring: divides all values by their greatest common divisor. */
    record GcdStage() implements TransformSpec {
        @Override
        public StageId stageId() {
            return StageId.GCD_STAGE;
        }
    }

    /**
     * Segmented delta encoding for piecewise-monotonic sequences: delta-encodes each
     * monotonic sub-run separated by direction flips, accepting up to {@code kMax}
     * flips per block.
     *
     * <p>{@code kMax} is an encode-time threshold and is not persisted in the wire format:
     * the decoder reads the actual per-block flip count from stage metadata. The decoder is
     * always reconstructed with {@link #MAX_K_MAX} so its scratch buffers fit any flip count
     * an encoder can emit, regardless of the encoder's own (smaller) {@code kMax}.
     *
     * @param kMax the maximum number of direction flips accepted per block; must be at least
     *             one and at most {@link #MAX_K_MAX}
     */
    record SplitDeltaStage(int kMax) implements TransformSpec {

        /** Default cap on direction flips per block. */
        public static final int DEFAULT_K_MAX = 16;

        /**
         * Largest {@code kMax} an encoder may pick, and the cap the decoder is always built
         * with so its scratch holds the most splits a block can contain.
         *
         * <p>An encoder sizes {@code kMax} from the block size as {@code clamp(blockSize / 32,
         * 4, 64)}: about one split per 32 values, so each sub-run stays long enough for its
         * anchor metadata to amortize. 64 is that ceiling (reached at {@code blockSize = 2048})
         * and bounds split metadata on large blocks. The decoder reads each block's actual split
         * count, but sizes its scratch to this fixed maximum once instead of growing per block,
         * keeping the hot decode path free of reallocation.
         */
        public static final int MAX_K_MAX = 64;

        public SplitDeltaStage {
            if (kMax < 1 || kMax > MAX_K_MAX) {
                throw new IllegalArgumentException("kMax must be in [1, " + MAX_K_MAX + "], got: " + kMax);
            }
        }

        @Override
        public StageId stageId() {
            return StageId.SPLIT_DELTA_STAGE;
        }
    }

    /** ALP transform: converts doubles to integer mantissas via a shared per-block (e, f). */
    record AlpDoubleStage() implements TransformSpec {
        @Override
        public StageId stageId() {
            return StageId.ALP_DOUBLE_STAGE;
        }
    }

    /** Bit-packing payload: packs values using the minimum number of bits. */
    record BitPackPayload() implements PayloadSpec {
        @Override
        public StageId stageId() {
            return StageId.BITPACK_PAYLOAD;
        }
    }
}
