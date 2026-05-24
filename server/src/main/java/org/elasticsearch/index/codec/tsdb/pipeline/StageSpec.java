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
     * <p>{@code kMax} is an encode-time threshold only and is not persisted in the wire
     * format. The decoder reads the actual per-block flip count from stage metadata, so
     * it does not need to know the encoder's cap.
     *
     * @param kMax the maximum number of direction flips accepted per block; must be at least one
     */
    record SplitDeltaStage(int kMax) implements TransformSpec {

        /** Default cap on direction flips per block. */
        public static final int DEFAULT_K_MAX = 16;

        public SplitDeltaStage {
            if (kMax < 1) {
                throw new IllegalArgumentException("kMax must be at least 1, got: " + kMax);
            }
        }

        @Override
        public StageId stageId() {
            return StageId.SPLIT_DELTA_STAGE;
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
