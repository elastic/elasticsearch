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
 * Each record represents a stage type and can hold stage-specific parameters.
 * References {@link StageId} as the source of truth for byte identifiers.
 */
public sealed interface StageSpec {

    StageId stageId();

    // Transform stages
    record Delta() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.DELTA;
        }
    }

    record DeltaDelta() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.DELTA_DELTA;
        }
    }

    record Offset() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.OFFSET;
        }
    }

    record Gcd() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.GCD;
        }
    }

    record PatchedPFor() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.PATCHED_PFOR;
        }
    }

    record Xor() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.XOR;
        }
    }

    record QuantizeDouble(double maxError) implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.QUANTIZE_DOUBLE;
        }
    }

    record Rle() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.RLE;
        }
    }

    record AlpDoubleStage(double maxError) implements StageSpec {
        public AlpDoubleStage() {
            this(-1);
        }

        @Override
        public StageId stageId() {
            return StageId.ALP_DOUBLE_STAGE;
        }
    }

    record AlpRdDoubleStage(double maxError) implements StageSpec {
        public AlpRdDoubleStage() {
            this(-1);
        }

        @Override
        public StageId stageId() {
            return StageId.ALP_RD_DOUBLE_STAGE;
        }
    }

    record AlpFloatStage(double maxError) implements StageSpec {
        public AlpFloatStage() {
            this(-1);
        }

        @Override
        public StageId stageId() {
            return StageId.ALP_FLOAT_STAGE;
        }
    }

    record AlpRdFloatStage(double maxError) implements StageSpec {
        public AlpRdFloatStage() {
            this(-1);
        }

        @Override
        public StageId stageId() {
            return StageId.ALP_RD_FLOAT_STAGE;
        }
    }

    record FpcDoubleStage(int tableSize, double maxError) implements StageSpec {
        public FpcDoubleStage() {
            this(0, -1);
        }

        public FpcDoubleStage(int tableSize) {
            this(tableSize, -1);
        }

        @Override
        public StageId stageId() {
            return StageId.FPC_DOUBLE_STAGE;
        }
    }

    record FpcFloatStage(int tableSize, double maxError) implements StageSpec {
        public FpcFloatStage() {
            this(0, -1);
        }

        public FpcFloatStage(int tableSize) {
            this(tableSize, -1);
        }

        @Override
        public StageId stageId() {
            return StageId.FPC_FLOAT_STAGE;
        }
    }

    // Payload stages
    record BitPack() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.BIT_PACK;
        }
    }

    record Zstd() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.ZSTD;
        }
    }

    record Lz4(boolean highCompression) implements StageSpec {
        public Lz4() {
            this(false);
        }

        @Override
        public StageId stageId() {
            return StageId.LZ4;
        }
    }

    record Gorilla(double maxError) implements StageSpec {
        public Gorilla() {
            this(-1);
        }

        @Override
        public StageId stageId() {
            return StageId.GORILLA_PAYLOAD;
        }
    }

    record RlePayload() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.RLE_PAYLOAD;
        }
    }

    record AlpDouble(double maxError) implements StageSpec {
        public AlpDouble() {
            this(-1);
        }

        @Override
        public StageId stageId() {
            return StageId.ALP_DOUBLE;
        }
    }

    record AlpRdDouble(double maxError) implements StageSpec {
        public AlpRdDouble() {
            this(-1);
        }

        @Override
        public StageId stageId() {
            return StageId.ALP_RD_DOUBLE;
        }
    }

    record GorillaFloat() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.GORILLA_FLOAT_PAYLOAD;
        }
    }

    record AlpFloat(double maxError) implements StageSpec {
        public AlpFloat() {
            this(-1);
        }

        @Override
        public StageId stageId() {
            return StageId.ALP_FLOAT;
        }
    }

    record AlpRdFloat(double maxError) implements StageSpec {
        public AlpRdFloat() {
            this(-1);
        }

        @Override
        public StageId stageId() {
            return StageId.ALP_RD_FLOAT;
        }
    }

    record ChimpDoublePayload(double maxError) implements StageSpec {
        public ChimpDoublePayload() {
            this(-1);
        }

        @Override
        public StageId stageId() {
            return StageId.CHIMP_DOUBLE_PAYLOAD;
        }
    }

    record ChimpFloatPayload() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.CHIMP_FLOAT_PAYLOAD;
        }
    }

    record Chimp128DoublePayload(double maxError) implements StageSpec {
        public Chimp128DoublePayload() {
            this(-1);
        }

        @Override
        public StageId stageId() {
            return StageId.CHIMP128_DOUBLE_PAYLOAD;
        }
    }

    record Chimp128FloatPayload() implements StageSpec {
        @Override
        public StageId stageId() {
            return StageId.CHIMP128_FLOAT_PAYLOAD;
        }
    }
}
