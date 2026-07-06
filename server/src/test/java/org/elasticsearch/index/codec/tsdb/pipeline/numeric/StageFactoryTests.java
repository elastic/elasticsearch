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
import org.elasticsearch.test.ESTestCase;

public class StageFactoryTests extends ESTestCase {

    public void testSpecFromStageIdPreservesIdentity() {
        for (StageId id : StageId.values()) {
            final StageSpec spec = StageFactory.specFromStageId(id);
            assertEquals(id, spec.stageId());
        }
    }

    public void testSpecFromStageIdRebuildsSplitDeltaWithMaxKMax() {
        final StageSpec spec = StageFactory.specFromStageId(StageId.SPLIT_DELTA_STAGE);
        assertEquals(
            "decoder must be sized for the largest kMax any encoder can use",
            StageSpec.SplitDeltaStage.MAX_K_MAX,
            ((StageSpec.SplitDeltaStage) spec).kMax()
        );
    }

    public void testTransformStageCreationMatchesSpec() {
        final StageSpec.TransformSpec[] specs = {
            new StageSpec.DeltaStage(),
            new StageSpec.OffsetStage(),
            new StageSpec.GcdStage(),
            new StageSpec.AlpDoubleStage() };
        for (StageSpec.TransformSpec spec : specs) {
            final NumericCodecStage stage = StageFactory.newTransformStage(spec, 128);
            assertEquals(spec.stageId().id, stage.id());
        }
    }

    public void testPayloadStageCreationMatchesSpec() {
        final PayloadCodecStage stage = StageFactory.newPayloadStage(new StageSpec.BitPackPayload(), 128);
        assertEquals(StageId.BITPACK_PAYLOAD.id, stage.id());
    }

    public void testNewTransformStageRejectsPayloadSpec() {
        expectThrows(IllegalArgumentException.class, () -> StageFactory.newTransformStage(new StageSpec.BitPackPayload(), 128));
    }

    public void testNewPayloadStageRejectsTransformSpec() {
        final StageSpec.TransformSpec[] specs = { new StageSpec.DeltaStage(), new StageSpec.OffsetStage(), new StageSpec.GcdStage() };
        for (StageSpec.TransformSpec spec : specs) {
            expectThrows(IllegalArgumentException.class, () -> StageFactory.newPayloadStage(spec, 128));
        }
    }
}
