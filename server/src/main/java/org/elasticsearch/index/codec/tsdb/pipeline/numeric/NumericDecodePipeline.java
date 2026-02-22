/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.DataInput;
import org.elasticsearch.index.codec.tsdb.pipeline.BlockFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.StageSpec;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpDoubleTransformDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpFloatTransformDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpRdDoubleTransformDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpRdFloatTransformDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.ChimpDoubleTransformDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.ChimpFloatTransformDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.DeltaCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.DeltaDeltaCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.FpcTransformDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.GcdCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.OffsetCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.PatchedPForDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.QuantizeDoubleCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.RleDecodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.XorCodecStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class NumericDecodePipeline {

    private static final Logger logger = LogManager.getLogger(NumericDecodePipeline.class);

    private final TransformDecoder[] stages;
    private final StageId[] stageIds;
    private final PayloadDecoder payloadStage;
    private final int blockSize;
    private final int payloadPosition;

    NumericDecodePipeline(final TransformDecoder[] stages, final PayloadDecoder payloadStage, int blockSize) {
        this.stages = stages;
        this.stageIds = new StageId[stages.length];
        for (int i = 0; i < stages.length; i++) {
            this.stageIds[i] = StageId.fromId(stages[i].id());
        }
        this.payloadStage = payloadStage;
        this.blockSize = blockSize;
        this.payloadPosition = stages.length;
    }

    static NumericDecodePipeline fromDescriptor(final PipelineDescriptor descriptor) {
        final int blockSize = descriptor.blockSize();
        final int stageCount = descriptor.pipelineLength();
        final boolean isFloat = descriptor.dataType() == PipelineDescriptor.DataType.FLOAT;
        final List<TransformDecoder> stages = new ArrayList<>();

        for (int i = 0; i < stageCount - 1; i++) {
            final StageSpec spec = StageFactory.specFromStageId(StageId.fromId(descriptor.stageIdAt(i)));
            stages.add(StageFactory.newTransformDecoder(spec, blockSize, isFloat));
        }
        final StageSpec payloadSpec = StageFactory.specFromStageId(StageId.fromId(descriptor.stageIdAt(stageCount - 1)));
        final PayloadDecoder payloadStage = StageFactory.newPayloadDecoder(payloadSpec, blockSize);

        logger.trace("fromDescriptor blockSize=[{}] stages=[{}] payload=[{}]", blockSize, stages, payloadSpec);
        return new NumericDecodePipeline(stages.toArray(TransformDecoder[]::new), payloadStage, blockSize);
    }

    // NOTE: HotSpot's JIT has three inline cache states for virtual/interface calls:
    // monomorphic (1 type, inlines directly), bimorphic (2 types, inlines both with a
    // type guard), and megamorphic (3+ types, gives up inlining entirely and falls back to
    // vtable/itable lookup). A single stages[pos].decode() call site sees 13+ concrete
    // TransformDecoder types, so it is always megamorphic and never inlined. The switch on
    // stageIds[] with static methods splits this into N monomorphic call sites, each with a
    // known concrete type that the JIT can inline. This runs per block on the decode hot
    // path, so the inlining matters.
    // See "Three Types and Beyond" in https://shipilev.net/blog/2015/black-magic-method-dispatch/
    public int decode(final long[] values, final DataInput in, final DecodingContext context) throws IOException {
        context.setDataInput(in);

        int valueCount = readBlock(in, values, context);

        for (int pos = stages.length - 1; pos >= 0; pos--) {
            if (context.isStageApplied(pos)) {
                valueCount = switch (stageIds[pos]) {
                    case DELTA -> DeltaCodecStage.decodeStatic((DeltaCodecStage) stages[pos], values, valueCount, context);
                    case DELTA_DELTA -> DeltaDeltaCodecStage.decodeStatic((DeltaDeltaCodecStage) stages[pos], values, valueCount, context);
                    case OFFSET -> OffsetCodecStage.decodeStatic((OffsetCodecStage) stages[pos], values, valueCount, context);
                    case GCD -> GcdCodecStage.decodeStatic((GcdCodecStage) stages[pos], values, valueCount, context);
                    case PATCHED_PFOR -> PatchedPForDecodeStage.decodeStatic(
                        (PatchedPForDecodeStage) stages[pos],
                        values,
                        valueCount,
                        context
                    );
                    case XOR -> XorCodecStage.decodeStatic((XorCodecStage) stages[pos], values, valueCount, context);
                    case QUANTIZE_DOUBLE -> QuantizeDoubleCodecStage.decodeStatic(
                        (QuantizeDoubleCodecStage) stages[pos],
                        values,
                        valueCount,
                        context
                    );
                    case RLE -> RleDecodeStage.decodeStatic((RleDecodeStage) stages[pos], values, valueCount, context);
                    case ALP_DOUBLE_STAGE -> AlpDoubleTransformDecodeStage.decodeStatic(
                        (AlpDoubleTransformDecodeStage) stages[pos],
                        values,
                        valueCount,
                        context
                    );
                    case ALP_RD_DOUBLE_STAGE -> AlpRdDoubleTransformDecodeStage.decodeStatic(
                        (AlpRdDoubleTransformDecodeStage) stages[pos],
                        values,
                        valueCount,
                        context
                    );
                    case ALP_FLOAT_STAGE -> AlpFloatTransformDecodeStage.decodeStatic(
                        (AlpFloatTransformDecodeStage) stages[pos],
                        values,
                        valueCount,
                        context
                    );
                    case ALP_RD_FLOAT_STAGE -> AlpRdFloatTransformDecodeStage.decodeStatic(
                        (AlpRdFloatTransformDecodeStage) stages[pos],
                        values,
                        valueCount,
                        context
                    );
                    case FPC_STAGE -> FpcTransformDecodeStage.decodeStatic(
                        (FpcTransformDecodeStage) stages[pos],
                        values,
                        valueCount,
                        context
                    );
                    case CHIMP_DOUBLE_STAGE -> ChimpDoubleTransformDecodeStage.decodeStatic(
                        (ChimpDoubleTransformDecodeStage) stages[pos],
                        values,
                        valueCount,
                        context
                    );
                    case CHIMP_FLOAT_STAGE -> ChimpFloatTransformDecodeStage.decodeStatic(
                        (ChimpFloatTransformDecodeStage) stages[pos],
                        values,
                        valueCount,
                        context
                    );
                    default -> throw new IllegalStateException("Unexpected stage: " + stageIds[pos]);
                };
            }
        }
        return valueCount;
    }

    public int blockSize() {
        return blockSize;
    }

    public int size() {
        return stages.length + 1;
    }

    private int readBlock(final DataInput in, final long[] values, final DecodingContext context) throws IOException {
        return BlockFormat.readBlock(in, values, payloadStage, context, payloadPosition);
    }

    @Override
    public String toString() {
        final String stageChain = Arrays.stream(stages).map(Object::toString).collect(Collectors.joining(" -> "));
        return "NumericDecodePipeline{blockSize=" + blockSize + ", stages=[" + stageChain + " -> " + payloadStage + "]}";
    }
}
