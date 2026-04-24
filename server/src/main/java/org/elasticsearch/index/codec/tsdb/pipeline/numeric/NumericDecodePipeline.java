/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

import org.apache.lucene.store.DataInput;
import org.elasticsearch.index.codec.tsdb.pipeline.BlockFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.StageSpec;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.DeltaCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.GcdCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.OffsetCodecStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Immutable decoding pipeline: reads payload then reverses transform stages.
 *
 * <p>Reconstructed from a {@link PipelineDescriptor} read from segment metadata,
 * making the format self-describing. The decoder does not need to know the
 * pipeline configuration at compile time.
 *
 * <p>Instances are immutable and thread-safe. Mutable per-block state lives in
 * {@link DecodingContext}, which must be provided by the caller.
 */
public final class NumericDecodePipeline {

    private final NumericCodecStage[] transformStages;
    private final StageId[] stageIds;
    private final PayloadCodecStage payloadStage;
    private final int blockSize;
    private final int payloadPosition;

    NumericDecodePipeline(final NumericCodecStage[] transformStages, final PayloadCodecStage payloadStage, int blockSize) {
        this.transformStages = transformStages;
        this.stageIds = new StageId[transformStages.length];
        for (int i = 0; i < transformStages.length; i++) {
            this.stageIds[i] = StageId.fromId(transformStages[i].id());
        }
        this.payloadStage = payloadStage;
        this.blockSize = blockSize;
        this.payloadPosition = transformStages.length;
    }

    /**
     * Reconstructs a decode pipeline from a persisted descriptor.
     *
     * @param descriptor the pipeline descriptor read from segment metadata
     * @return the decode pipeline
     */
    public static NumericDecodePipeline fromDescriptor(final PipelineDescriptor descriptor) {
        final int blockSize = descriptor.blockSize();
        final int stageCount = descriptor.pipelineLength();
        final List<NumericCodecStage> transforms = new ArrayList<>();

        for (int i = 0; i < stageCount - 1; i++) {
            final StageSpec spec = StageFactory.specFromStageId(StageId.fromId(descriptor.stageIdAt(i)));
            transforms.add(StageFactory.newTransformStage(spec));
        }
        final StageSpec payloadSpec = StageFactory.specFromStageId(StageId.fromId(descriptor.stageIdAt(stageCount - 1)));
        final PayloadCodecStage payloadStage = StageFactory.newPayloadStage(payloadSpec, blockSize);

        return new NumericDecodePipeline(transforms.toArray(NumericCodecStage[]::new), payloadStage, blockSize);
    }

    /**
     * Decodes a block of values by reading the payload and reversing transforms.
     *
     * @param values  the output array to populate
     * @param count   the expected number of values
     * @param in      the data input to read from
     * @param context the mutable per-block decoding context
     * @throws IOException if an I/O error occurs
     */
    // NOTE: a single stages[i].decode() call site sees multiple concrete TransformDecoder
    // types, making it megamorphic and preventing JIT inlining. The switch on stageIds[]
    // with static methods splits this into N monomorphic call sites that the JIT can inline.
    // See https://shipilev.net/blog/2015/black-magic-method-dispatch/#_three_types_and_beyond
    public void decode(final long[] values, int count, final DataInput in, final DecodingContext context) throws IOException {
        context.setDataInput(in);
        BlockFormat.readBlock(in, values, payloadStage, context, payloadPosition);
        for (int i = transformStages.length - 1; i >= 0; i--) {
            if (context.isStageApplied(i)) {
                switch (stageIds[i]) {
                    case DELTA_STAGE -> DeltaCodecStage.decodeStatic((DeltaCodecStage) transformStages[i], values, count, context);
                    case OFFSET_STAGE -> OffsetCodecStage.decodeStatic((OffsetCodecStage) transformStages[i], values, count, context);
                    case GCD_STAGE -> GcdCodecStage.decodeStatic((GcdCodecStage) transformStages[i], values, count, context);
                    default -> throw new IllegalStateException("Unexpected decode stage: " + stageIds[i]);
                }
            }
        }
    }

    /**
     * Returns the number of values per block.
     *
     * @return the number of values per block
     */
    public int blockSize() {
        return blockSize;
    }

    /**
     * Returns the total number of stages (transforms + payload).
     *
     * @return the total number of stages
     */
    public int size() {
        return transformStages.length + 1;
    }
}
