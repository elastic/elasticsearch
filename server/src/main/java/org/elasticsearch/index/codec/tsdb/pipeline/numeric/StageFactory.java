/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

import org.elasticsearch.index.codec.tsdb.DocValuesForUtil;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.StageSpec;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.BitPackCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.DeltaCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.GcdCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.OffsetCodecStage;

/**
 * Maps between {@link StageSpec} records and concrete stage instances, and between
 * persisted {@link StageId} bytes and {@link StageSpec} records.
 *
 * <p>The encode path uses {@link #newTransformStage} and {@link #newPayloadStage} to
 * create stages from a {@link PipelineConfig}.
 * The decode path uses {@link #specFromStageId} to reconstruct specs from the persisted
 * stage IDs in a {@link PipelineDescriptor}.
 */
public final class StageFactory {

    private StageFactory() {}

    /**
     * Creates a transform stage from a specification.
     *
     * @param spec the stage specification
     * @return the concrete stage instance
     * @throws IllegalArgumentException if the spec is not a transform stage
     */
    static NumericCodecStage newTransformStage(final StageSpec spec) {
        return switch (spec) {
            case StageSpec.DeltaStage ignored -> DeltaCodecStage.INSTANCE;
            case StageSpec.OffsetStage ignored -> OffsetCodecStage.INSTANCE;
            case StageSpec.GcdStage ignored -> GcdCodecStage.INSTANCE;
            default -> throw new IllegalArgumentException("Not a transform stage: " + spec);
        };
    }

    /**
     * Creates a payload stage from a specification.
     *
     * @param spec      the stage specification
     * @param blockSize the number of values per block
     * @return the concrete payload stage instance
     * @throws IllegalArgumentException if the spec is not a payload stage
     */
    static PayloadCodecStage newPayloadStage(final StageSpec spec, int blockSize) {
        return switch (spec) {
            case StageSpec.BitPackPayload ignored -> new BitPackCodecStage(new DocValuesForUtil(blockSize));
            default -> throw new IllegalArgumentException("Not a payload stage: " + spec);
        };
    }

    /**
     * Converts a persisted {@link StageId} back to its corresponding {@link StageSpec}.
     *
     * <p>Used on the decode path to reconstruct the pipeline from a
     * {@link PipelineDescriptor}.
     *
     * @param stageId the persisted stage identifier
     * @return the corresponding stage specification
     */
    static StageSpec specFromStageId(final StageId stageId) {
        return switch (stageId) {
            case DELTA_STAGE -> new StageSpec.DeltaStage();
            case OFFSET_STAGE -> new StageSpec.OffsetStage();
            case GCD_STAGE -> new StageSpec.GcdStage();
            case BITPACK_PAYLOAD -> new StageSpec.BitPackPayload();
        };
    }
}
