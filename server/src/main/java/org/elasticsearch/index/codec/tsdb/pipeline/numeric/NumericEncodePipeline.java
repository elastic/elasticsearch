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
import org.elasticsearch.index.codec.tsdb.pipeline.FieldDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.StageSpec;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.DeltaCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.GcdCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.OffsetCodecStage;

import java.io.IOException;
import java.util.List;

/**
 * Immutable encoding pipeline: transform stages followed by a terminal payload stage.
 *
 * <p>Transform stages run in forward order, modifying values in-place to reduce
 * dynamic range. The payload stage serializes the result. Each stage decides
 * per-block whether to apply itself, recorded in a position bitmap for decoding.
 *
 * <p>Instances are immutable and thread-safe. Mutable per-block state lives in
 * {@link EncodingContext}, which must be provided by the caller.
 */
public final class NumericEncodePipeline {

    private final NumericCodecStage[] transformStages;
    private final StageId[] stageIds;
    private final PayloadCodecStage payloadStage;
    private final PipelineDescriptor descriptor;
    private final int blockSize;
    private final int payloadPosition;

    NumericEncodePipeline(
        final NumericCodecStage[] transformStages,
        final PayloadCodecStage payloadStage,
        int blockSize,
        final PipelineDescriptor descriptor
    ) {
        this.transformStages = transformStages;
        this.stageIds = new StageId[transformStages.length];
        for (int i = 0; i < transformStages.length; i++) {
            this.stageIds[i] = StageId.fromId(transformStages[i].id());
        }
        this.payloadStage = payloadStage;
        this.blockSize = blockSize;
        this.payloadPosition = transformStages.length;
        this.descriptor = descriptor;
    }

    /**
     * Builds an encode pipeline from a pipeline configuration.
     *
     * @param config the pipeline configuration
     * @return the encode pipeline
     * @throws IllegalStateException if the pipeline has no payload stage
     */
    public static NumericEncodePipeline fromConfig(final PipelineConfig config) {
        final int blockSize = config.blockSize();
        final List<StageSpec.TransformSpec> transformSpecs = config.transforms();
        final NumericCodecStage[] transforms = new NumericCodecStage[transformSpecs.size()];
        for (int i = 0; i < transformSpecs.size(); i++) {
            transforms[i] = StageFactory.newTransformStage(transformSpecs.get(i));
        }
        final PayloadCodecStage payload = StageFactory.newPayloadStage(config.payload(), blockSize);

        final List<StageSpec> specs = config.specs();
        final byte[] ids = new byte[specs.size()];
        for (int i = 0; i < specs.size(); i++) {
            ids[i] = specs.get(i).stageId().id;
        }
        final PipelineDescriptor descriptor = new PipelineDescriptor(ids, blockSize, config.dataType());

        return new NumericEncodePipeline(transforms, payload, blockSize, descriptor);
    }

    /**
     * Encodes a block of values through the pipeline.
     *
     * @param values     the values to encode (modified in-place by transform stages)
     * @param valueCount the number of valid values
     * @param out        the data output to write the encoded block to
     * @param context    the mutable per-block encoding context
     * @throws IOException if an I/O error occurs
     */
    // NOTE: a single stages[i].encode() call site sees multiple concrete TransformEncoder
    // types, making it megamorphic and preventing JIT inlining. The switch on stageIds[]
    // with static methods splits this into N monomorphic call sites that the JIT can inline.
    // See https://shipilev.net/blog/2015/black-magic-method-dispatch/#_three_types_and_beyond
    public void encode(final long[] values, int valueCount, final DataOutput out, final EncodingContext context) throws IOException {
        context.setValueCount(valueCount);

        for (int i = 0; i < transformStages.length; i++) {
            context.setCurrentPosition(i);
            switch (stageIds[i]) {
                case DELTA_STAGE -> DeltaCodecStage.encodeStatic(
                    (DeltaCodecStage) transformStages[i],
                    values,
                    context.valueCount(),
                    context
                );
                case OFFSET_STAGE -> OffsetCodecStage.encodeStatic(
                    (OffsetCodecStage) transformStages[i],
                    values,
                    context.valueCount(),
                    context
                );
                case GCD_STAGE -> GcdCodecStage.encodeStatic((GcdCodecStage) transformStages[i], values, context.valueCount(), context);
                default -> throw new IllegalStateException("Unexpected encode stage: " + stageIds[i]);
            }
        }

        context.setCurrentPosition(payloadPosition);
        context.applyStage(payloadPosition);

        BlockFormat.writeBlock(out, values, payloadStage, context);
    }

    /**
     * Returns the pipeline descriptor for persistence via {@link FieldDescriptor}.
     *
     * @return the pipeline descriptor
     */
    public PipelineDescriptor descriptor() {
        return descriptor;
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
