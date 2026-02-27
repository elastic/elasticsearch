/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.StageSpec;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpDoubleDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpDoubleEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpDoubleTransformDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpDoubleTransformEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpFloatDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpFloatEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpFloatTransformDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpFloatTransformEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpRdDoubleDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpRdDoubleEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpRdDoubleTransformDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpRdDoubleTransformEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpRdFloatDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpRdFloatEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpRdFloatTransformDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpRdFloatTransformEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.BitPackCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.Chimp128DoubleDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.Chimp128DoubleEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.Chimp128FloatDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.Chimp128FloatEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.ChimpDoubleDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.ChimpDoubleEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.ChimpFloatDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.ChimpFloatEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.DeltaCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.DeltaDeltaCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.FpcDoubleTransformDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.FpcDoubleTransformEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.FpcFloatTransformDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.FpcFloatTransformEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.GcdCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.GorillaDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.GorillaEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.GorillaFloatDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.GorillaFloatEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.Lz4DecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.Lz4EncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.OffsetCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.PatchedPForDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.PatchedPForEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.QuantizeDoubleCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.RleDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.RleEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.RlePayloadCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.XorCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.ZstdDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.ZstdEncodeStage;

public final class StageFactory {

    private StageFactory() {}

    static TransformEncoder newTransformEncoder(final StageSpec spec, int blockSize) {
        return newTransformEncoder(spec, blockSize, false);
    }

    static TransformEncoder newTransformEncoder(final StageSpec spec, int blockSize, boolean isFloat) {
        return switch (spec) {
            case StageSpec.Delta() -> DeltaCodecStage.INSTANCE;
            case StageSpec.DeltaDelta() -> DeltaDeltaCodecStage.INSTANCE;
            case StageSpec.Offset() -> OffsetCodecStage.INSTANCE;
            case StageSpec.Gcd() -> GcdCodecStage.INSTANCE;
            case StageSpec.PatchedPFor() -> new PatchedPForEncodeStage();
            case StageSpec.Xor() -> XorCodecStage.INSTANCE;
            case StageSpec.QuantizeDouble(double maxError) -> new QuantizeDoubleCodecStage(maxError);
            case StageSpec.Rle() -> new RleEncodeStage(blockSize);
            case StageSpec.AlpDoubleStage(double maxError) -> maxError > 0
                ? new AlpDoubleTransformEncodeStage(blockSize, maxError)
                : new AlpDoubleTransformEncodeStage(blockSize);
            case StageSpec.AlpRdDoubleStage(double maxError) -> maxError > 0
                ? new AlpRdDoubleTransformEncodeStage(blockSize, maxError)
                : new AlpRdDoubleTransformEncodeStage(blockSize);
            case StageSpec.AlpFloatStage(double maxError) -> maxError > 0
                ? new AlpFloatTransformEncodeStage(blockSize, maxError)
                : new AlpFloatTransformEncodeStage(blockSize);
            case StageSpec.AlpRdFloatStage(double maxError) -> maxError > 0
                ? new AlpRdFloatTransformEncodeStage(blockSize, maxError)
                : new AlpRdFloatTransformEncodeStage(blockSize);
            case StageSpec.FpcDoubleStage(int ts, double me) -> me > 0
                ? new FpcDoubleTransformEncodeStage(blockSize, ts > 0 ? ts : FpcDoubleTransformEncodeStage.DEFAULT_TABLE_SIZE, me)
                : new FpcDoubleTransformEncodeStage(blockSize, ts > 0 ? ts : FpcDoubleTransformEncodeStage.DEFAULT_TABLE_SIZE);
            case StageSpec.FpcFloatStage(int ts, double me) -> me > 0
                ? new FpcFloatTransformEncodeStage(blockSize, ts > 0 ? ts : FpcFloatTransformEncodeStage.DEFAULT_TABLE_SIZE, me)
                : new FpcFloatTransformEncodeStage(blockSize, ts > 0 ? ts : FpcFloatTransformEncodeStage.DEFAULT_TABLE_SIZE);
            default -> throw new IllegalArgumentException("Not a transform stage: " + spec);
        };
    }

    static TransformDecoder newTransformDecoder(final StageSpec spec, int blockSize) {
        return newTransformDecoder(spec, blockSize, false);
    }

    static TransformDecoder newTransformDecoder(final StageSpec spec, int blockSize, boolean isFloat) {
        return switch (spec) {
            case StageSpec.Delta() -> DeltaCodecStage.INSTANCE;
            case StageSpec.DeltaDelta() -> DeltaDeltaCodecStage.INSTANCE;
            case StageSpec.Offset() -> OffsetCodecStage.INSTANCE;
            case StageSpec.Gcd() -> GcdCodecStage.INSTANCE;
            case StageSpec.PatchedPFor() -> new PatchedPForDecodeStage();
            case StageSpec.Xor() -> XorCodecStage.INSTANCE;
            case StageSpec.QuantizeDouble(double maxError) -> new QuantizeDoubleCodecStage(maxError);
            case StageSpec.Rle() -> new RleDecodeStage();
            case StageSpec.AlpDoubleStage alpDoubleStage -> new AlpDoubleTransformDecodeStage();
            case StageSpec.AlpRdDoubleStage alpRdDoubleStage -> new AlpRdDoubleTransformDecodeStage();
            case StageSpec.AlpFloatStage alpFloatStage -> new AlpFloatTransformDecodeStage();
            case StageSpec.AlpRdFloatStage alpRdFloatStage -> new AlpRdFloatTransformDecodeStage();
            case StageSpec.FpcDoubleStage(int ts, double me) -> ts > 0
                ? new FpcDoubleTransformDecodeStage(blockSize, ts)
                : new FpcDoubleTransformDecodeStage(blockSize);
            case StageSpec.FpcFloatStage(int ts, double me) -> ts > 0
                ? new FpcFloatTransformDecodeStage(blockSize, ts)
                : new FpcFloatTransformDecodeStage(blockSize);
            default -> throw new IllegalArgumentException("Not a transform stage: " + spec);
        };
    }

    static PayloadEncoder newPayloadEncoder(final StageSpec spec, int blockSize) {
        return switch (spec) {
            case StageSpec.BitPack() -> new BitPackCodecStage(blockSize);
            case StageSpec.Zstd() -> new ZstdEncodeStage(blockSize, ZstdEncodeStage.DEFAULT_COMPRESSION_LEVEL);
            case StageSpec.Lz4(boolean highCompression) -> new Lz4EncodeStage(blockSize, highCompression);
            case StageSpec.Gorilla() -> new GorillaEncodeStage();
            case StageSpec.GorillaFloat() -> new GorillaFloatEncodeStage();
            case StageSpec.RlePayload() -> RlePayloadCodecStage.INSTANCE;
            case StageSpec.AlpDouble(double maxError) -> maxError > 0
                ? new AlpDoubleEncodeStage(blockSize, maxError)
                : new AlpDoubleEncodeStage(blockSize);
            case StageSpec.AlpRdDouble(double maxError) -> maxError > 0
                ? new AlpRdDoubleEncodeStage(blockSize, maxError)
                : new AlpRdDoubleEncodeStage(blockSize);
            case StageSpec.AlpFloat(double maxError) -> maxError > 0
                ? new AlpFloatEncodeStage(blockSize, maxError)
                : new AlpFloatEncodeStage(blockSize);
            case StageSpec.AlpRdFloat(double maxError) -> maxError > 0
                ? new AlpRdFloatEncodeStage(blockSize, maxError)
                : new AlpRdFloatEncodeStage(blockSize);
            case StageSpec.ChimpDoublePayload() -> new ChimpDoubleEncodeStage();
            case StageSpec.ChimpFloatPayload() -> new ChimpFloatEncodeStage();
            case StageSpec.Chimp128DoublePayload() -> new Chimp128DoubleEncodeStage(blockSize);
            case StageSpec.Chimp128FloatPayload() -> new Chimp128FloatEncodeStage(blockSize);
            default -> throw new IllegalArgumentException("Not a payload stage: " + spec);
        };
    }

    static PayloadDecoder newPayloadDecoder(final StageSpec spec, int blockSize) {
        return switch (spec) {
            case StageSpec.BitPack() -> new BitPackCodecStage(blockSize);
            case StageSpec.Zstd() -> new ZstdDecodeStage(blockSize);
            case StageSpec.Lz4 lz4 -> new Lz4DecodeStage(blockSize);
            case StageSpec.Gorilla() -> new GorillaDecodeStage();
            case StageSpec.GorillaFloat() -> new GorillaFloatDecodeStage();
            case StageSpec.RlePayload() -> RlePayloadCodecStage.INSTANCE;
            case StageSpec.AlpDouble alpDouble -> new AlpDoubleDecodeStage(blockSize);
            case StageSpec.AlpRdDouble alpRdDouble -> new AlpRdDoubleDecodeStage(blockSize);
            case StageSpec.AlpFloat alpFloat -> new AlpFloatDecodeStage(blockSize);
            case StageSpec.AlpRdFloat alpRdFloat -> new AlpRdFloatDecodeStage(blockSize);
            case StageSpec.ChimpDoublePayload() -> new ChimpDoubleDecodeStage();
            case StageSpec.ChimpFloatPayload() -> new ChimpFloatDecodeStage();
            case StageSpec.Chimp128DoublePayload chimp128DoublePayload -> new Chimp128DoubleDecodeStage();
            case StageSpec.Chimp128FloatPayload chimp128FloatPayload -> new Chimp128FloatDecodeStage();
            default -> throw new IllegalArgumentException("Not a payload stage: " + spec);
        };
    }

    // NOTE: Converts a StageId (from the on-disk descriptor) to a StageSpec.
    // For parameterized specs (e.g., QuantizeDouble), uses decode-safe defaults
    // since parameters only affect encoding.
    static StageSpec specFromStageId(final StageId stageId) {
        return switch (stageId) {
            case DELTA -> new StageSpec.Delta();
            case DELTA_DELTA -> new StageSpec.DeltaDelta();
            case OFFSET -> new StageSpec.Offset();
            case GCD -> new StageSpec.Gcd();
            case PATCHED_PFOR -> new StageSpec.PatchedPFor();
            case XOR -> new StageSpec.Xor();
            case QUANTIZE_DOUBLE -> new StageSpec.QuantizeDouble(1.0);
            case RLE -> new StageSpec.Rle();
            case ALP_DOUBLE_STAGE -> new StageSpec.AlpDoubleStage();
            case ALP_RD_DOUBLE_STAGE -> new StageSpec.AlpRdDoubleStage();
            case ALP_FLOAT_STAGE -> new StageSpec.AlpFloatStage();
            case ALP_RD_FLOAT_STAGE -> new StageSpec.AlpRdFloatStage();
            case FPC_DOUBLE_STAGE -> new StageSpec.FpcDoubleStage();
            case FPC_FLOAT_STAGE -> new StageSpec.FpcFloatStage();
            case BIT_PACK -> new StageSpec.BitPack();
            case ZSTD -> new StageSpec.Zstd();
            case LZ4 -> new StageSpec.Lz4();
            case GORILLA_PAYLOAD -> new StageSpec.Gorilla();
            case GORILLA_FLOAT_PAYLOAD -> new StageSpec.GorillaFloat();
            case RLE_PAYLOAD -> new StageSpec.RlePayload();
            case ALP_DOUBLE -> new StageSpec.AlpDouble();
            case ALP_RD_DOUBLE -> new StageSpec.AlpRdDouble();
            case ALP_FLOAT -> new StageSpec.AlpFloat();
            case ALP_RD_FLOAT -> new StageSpec.AlpRdFloat();
            case CHIMP_DOUBLE_PAYLOAD -> new StageSpec.ChimpDoublePayload();
            case CHIMP_FLOAT_PAYLOAD -> new StageSpec.ChimpFloatPayload();
            case CHIMP128_DOUBLE_PAYLOAD -> new StageSpec.Chimp128DoublePayload();
            case CHIMP128_FLOAT_PAYLOAD -> new StageSpec.Chimp128FloatPayload();
        };
    }
}
