/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

import org.apache.lucene.store.DataOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.BlockFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.MetadataBuffer;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.StageSpec;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpDoubleTransformEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpFloatTransformEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpRdDoubleTransformEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.AlpRdFloatTransformEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.BitPackCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.ChimpDoubleTransformEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.ChimpFloatTransformEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.DeltaCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.DeltaDeltaCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.FpcTransformEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.GcdCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.OffsetCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.PatchedPForEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.QuantizeDoubleCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.RleEncodeStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.XorCodecStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class NumericEncodePipeline {

    private final TransformEncoder[] stages;
    private final StageId[] stageIds;
    private final PayloadEncoder payloadStage;
    private final PipelineDescriptor descriptor;
    private final int blockSize;
    private final int payloadPosition;

    NumericEncodePipeline(
        final TransformEncoder[] stages,
        final PayloadEncoder payloadStage,
        int blockSize,
        final PipelineDescriptor.DataType dataType
    ) {
        this.stages = stages;
        this.stageIds = new StageId[stages.length];
        for (int i = 0; i < stages.length; i++) {
            this.stageIds[i] = StageId.fromId(stages[i].id());
        }
        this.payloadStage = payloadStage;
        this.blockSize = blockSize;
        this.payloadPosition = stages.length;

        final byte[] ids = new byte[stages.length + 1];
        for (int i = 0; i < stages.length; i++) {
            ids[i] = stages[i].id();
        }
        ids[stages.length] = payloadStage.id();
        this.descriptor = new PipelineDescriptor(ids, blockSize, dataType);
    }

    static NumericEncodePipeline withDefault(int blockSize) {
        return new NumericEncodePipeline(
            new TransformEncoder[] { DeltaCodecStage.INSTANCE, OffsetCodecStage.INSTANCE, GcdCodecStage.INSTANCE },
            new BitPackCodecStage(blockSize),
            blockSize,
            PipelineDescriptor.DataType.LONG
        );
    }

    static NumericEncodePipeline fromConfig(final PipelineConfig config) {
        final int blockSize = config.blockSize();
        final List<StageSpec> specs = config.specs();
        final List<TransformEncoder> stages = new ArrayList<>();
        PayloadEncoder payloadStage = null;

        final PipelineConfig.DataType configDataType = config.dataType();
        final boolean isFloat = configDataType == PipelineConfig.DataType.FLOAT;

        for (final StageSpec spec : specs) {
            if (payloadStage != null) {
                throw new IllegalStateException("Payload stage must be last: " + spec);
            }
            if (isPayloadSpec(spec)) {
                payloadStage = StageFactory.newPayloadEncoder(spec, blockSize);
            } else {
                stages.add(StageFactory.newTransformEncoder(spec, blockSize, isFloat));
            }
        }
        for (final StageSpec spec : specs) {
            if (isDoubleOnly(spec) && configDataType == PipelineConfig.DataType.FLOAT) {
                throw new IllegalArgumentException("Double-only stage in FLOAT pipeline: " + spec);
            }
            if (isFloatOnly(spec) && configDataType == PipelineConfig.DataType.DOUBLE) {
                throw new IllegalArgumentException("Float-only stage in DOUBLE pipeline: " + spec);
            }
        }

        if (payloadStage == null) {
            throw new IllegalStateException("Pipeline must end with a payload stage");
        }

        final PipelineDescriptor.DataType dataType = switch (config.dataType()) {
            case DOUBLE -> PipelineDescriptor.DataType.DOUBLE;
            case FLOAT -> PipelineDescriptor.DataType.FLOAT;
            case LONG -> PipelineDescriptor.DataType.LONG;
        };

        return new NumericEncodePipeline(stages.toArray(TransformEncoder[]::new), payloadStage, blockSize, dataType);
    }

    private static boolean isPayloadSpec(final StageSpec spec) {
        return spec instanceof StageSpec.BitPack
            || spec instanceof StageSpec.Zstd
            || spec instanceof StageSpec.Lz4
            || spec instanceof StageSpec.Gorilla
            || spec instanceof StageSpec.GorillaFloat
            || spec instanceof StageSpec.RlePayload
            || spec instanceof StageSpec.AlpDouble
            || spec instanceof StageSpec.AlpRdDouble
            || spec instanceof StageSpec.AlpFloat
            || spec instanceof StageSpec.AlpRdFloat;
    }

    // NOTE: HotSpot's JIT has three inline cache states for virtual/interface calls:
    // monomorphic (1 type, inlines directly), bimorphic (2 types, inlines both with a
    // type guard), and megamorphic (3+ types, gives up inlining entirely and falls back to
    // vtable/itable lookup). A single stages[pos].encode() call site sees 13+ concrete
    // TransformEncoder types, so it is always megamorphic and never inlined. The switch on
    // stageIds[] with static methods splits this into N monomorphic call sites, each with a
    // known concrete type that the JIT can inline.
    // See "Three Types and Beyond" in https://shipilev.net/blog/2015/black-magic-method-dispatch/
    public int encode(final long[] values, int valueCount, final DataOutput out, final EncodingContext context) throws IOException {
        context.setValueCount(valueCount);

        for (int pos = 0; pos < stages.length; pos++) {
            context.setCurrentPosition(pos);
            int newCount = switch (stageIds[pos]) {
                case DELTA -> DeltaCodecStage.encodeStatic((DeltaCodecStage) stages[pos], values, context.valueCount(), context);
                case DELTA_DELTA -> DeltaDeltaCodecStage.encodeStatic(
                    (DeltaDeltaCodecStage) stages[pos],
                    values,
                    context.valueCount(),
                    context
                );
                case OFFSET -> OffsetCodecStage.encodeStatic((OffsetCodecStage) stages[pos], values, context.valueCount(), context);
                case GCD -> GcdCodecStage.encodeStatic((GcdCodecStage) stages[pos], values, context.valueCount(), context);
                case PATCHED_PFOR -> PatchedPForEncodeStage.encodeStatic(
                    (PatchedPForEncodeStage) stages[pos],
                    values,
                    context.valueCount(),
                    context
                );
                case XOR -> XorCodecStage.encodeStatic((XorCodecStage) stages[pos], values, context.valueCount(), context);
                case QUANTIZE_DOUBLE -> QuantizeDoubleCodecStage.encodeStatic(
                    (QuantizeDoubleCodecStage) stages[pos],
                    values,
                    context.valueCount(),
                    context
                );
                case RLE -> RleEncodeStage.encodeStatic((RleEncodeStage) stages[pos], values, context.valueCount(), context);
                case ALP_DOUBLE_STAGE -> AlpDoubleTransformEncodeStage.encodeStatic(
                    (AlpDoubleTransformEncodeStage) stages[pos],
                    values,
                    context.valueCount(),
                    context
                );
                case ALP_RD_DOUBLE_STAGE -> AlpRdDoubleTransformEncodeStage.encodeStatic(
                    (AlpRdDoubleTransformEncodeStage) stages[pos],
                    values,
                    context.valueCount(),
                    context
                );
                case ALP_FLOAT_STAGE -> AlpFloatTransformEncodeStage.encodeStatic(
                    (AlpFloatTransformEncodeStage) stages[pos],
                    values,
                    context.valueCount(),
                    context
                );
                case ALP_RD_FLOAT_STAGE -> AlpRdFloatTransformEncodeStage.encodeStatic(
                    (AlpRdFloatTransformEncodeStage) stages[pos],
                    values,
                    context.valueCount(),
                    context
                );
                case FPC_STAGE -> FpcTransformEncodeStage.encodeStatic(
                    (FpcTransformEncodeStage) stages[pos],
                    values,
                    context.valueCount(),
                    context
                );
                case CHIMP_DOUBLE_STAGE -> ChimpDoubleTransformEncodeStage.encodeStatic(
                    (ChimpDoubleTransformEncodeStage) stages[pos],
                    values,
                    context.valueCount(),
                    context
                );
                case CHIMP_FLOAT_STAGE -> ChimpFloatTransformEncodeStage.encodeStatic(
                    (ChimpFloatTransformEncodeStage) stages[pos],
                    values,
                    context.valueCount(),
                    context
                );
                default -> throw new IllegalStateException("Unexpected stage: " + stageIds[pos]);
            };
            context.setValueCount(newCount);
        }

        context.setCurrentPosition(payloadPosition);
        context.applyStage(payloadPosition);

        writeBlock(out, values, context);

        return context.valueCount();
    }

    public PipelineDescriptor descriptor() {
        return descriptor;
    }

    public int blockSize() {
        return blockSize;
    }

    public int metadataCapacity() {
        int capacity = MetadataBuffer.DEFAULT_CAPACITY;
        for (final TransformEncoder stage : stages) {
            capacity += stage.maxMetadataBytes(blockSize);
        }
        return Integer.highestOneBit(capacity - 1) << 1;
    }

    public int size() {
        return stages.length + 1;
    }

    private static boolean isDoubleOnly(final StageSpec s) {
        return s instanceof StageSpec.AlpDoubleStage
            || s instanceof StageSpec.AlpRdDoubleStage
            || s instanceof StageSpec.ChimpDoubleStage
            || s instanceof StageSpec.AlpDouble
            || s instanceof StageSpec.AlpRdDouble;
    }

    private static boolean isFloatOnly(final StageSpec s) {
        return s instanceof StageSpec.AlpFloatStage
            || s instanceof StageSpec.AlpRdFloatStage
            || s instanceof StageSpec.ChimpFloatStage
            || s instanceof StageSpec.GorillaFloat
            || s instanceof StageSpec.AlpFloat
            || s instanceof StageSpec.AlpRdFloat;
    }

    private void writeBlock(final DataOutput out, final long[] values, final EncodingContext context) throws IOException {
        BlockFormat.writeBlock(out, values, payloadStage, context);
    }

    @Override
    public String toString() {
        final String stageChain = Arrays.stream(stages).map(Object::toString).collect(Collectors.joining(" -> "));
        return "NumericEncodePipeline{blockSize=" + blockSize + ", stages=[" + stageChain + " -> " + payloadStage + "]}";
    }
}
