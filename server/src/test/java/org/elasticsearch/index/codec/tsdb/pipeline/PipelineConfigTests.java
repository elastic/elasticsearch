/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;

public class PipelineConfigTests extends ESTestCase {

    public void testForLongsBuilder() {
        int blockSize = randomBlockSize();
        final PipelineConfig config = PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();

        assertEquals(PipelineDescriptor.DataType.LONG, config.dataType());
        assertEquals(blockSize, config.blockSize());
        assertEquals(3, config.transforms().size());
        assertNotNull(config.payload());
        assertEquals(4, config.specs().size());
    }

    public void testInequalityByDataType() {
        int blockSize = randomBlockSize();
        final PipelineConfig a = new PipelineConfig(
            PipelineDescriptor.DataType.LONG,
            blockSize,
            List.of(new StageSpec.DeltaStage()),
            new StageSpec.BitPackPayload()
        );
        final PipelineConfig b = new PipelineConfig(
            PipelineDescriptor.DataType.DOUBLE,
            blockSize,
            List.of(new StageSpec.DeltaStage()),
            new StageSpec.BitPackPayload()
        );
        assertNotEquals(a, b);
    }

    public void testInequalityByBlockSize() {
        assertNotEquals(PipelineConfig.forLongs(128).delta().bitPack(), PipelineConfig.forLongs(256).delta().bitPack());
    }

    public void testSpecsAreImmutable() {
        final PipelineConfig config = PipelineConfig.forLongs(randomBlockSize()).delta().offset().bitPack();
        expectThrows(UnsupportedOperationException.class, () -> config.transforms().add(new StageSpec.GcdStage()));
    }

    public void testDescribeStages() {
        assertEquals(
            "delta>offset>gcd>bitPack",
            PipelineConfig.forLongs(randomBlockSize()).delta().offset().gcd().bitPack().describeStages()
        );
    }

    public void testDescribeStagesWithSingleStage() {
        assertEquals("bitPack", PipelineConfig.forLongs(randomBlockSize()).bitPack().describeStages());
    }

    public void testAllLongTransformStages() {
        final PipelineConfig config = PipelineConfig.forLongs(randomBlockSize()).delta().offset().gcd().bitPack();

        assertEquals(4, config.specs().size());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.DeltaStage.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.OffsetStage.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.GcdStage.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.BitPackPayload.class)));
    }

    public void testStageSpecsHaveCorrectStageIds() {
        final PipelineConfig config = PipelineConfig.forLongs(randomBlockSize()).delta().offset().gcd().bitPack();
        final List<StageSpec> specs = config.specs();

        assertEquals(StageId.DELTA_STAGE, specs.getFirst().stageId());
        assertEquals(StageId.OFFSET_STAGE, specs.get(1).stageId());
        assertEquals(StageId.GCD_STAGE, specs.get(2).stageId());
        assertEquals(StageId.BITPACK_PAYLOAD, specs.get(3).stageId());
    }

    public void testTransformsAndPayloadSeparation() {
        final PipelineConfig config = PipelineConfig.forLongs(randomBlockSize()).delta().offset().gcd().bitPack();

        assertEquals(3, config.transforms().size());
        assertEquals(StageId.DELTA_STAGE, config.transforms().get(0).stageId());
        assertEquals(StageId.OFFSET_STAGE, config.transforms().get(1).stageId());
        assertEquals(StageId.GCD_STAGE, config.transforms().get(2).stageId());
        assertEquals(StageId.BITPACK_PAYLOAD, config.payload().stageId());
    }

    public void testPayloadOnlyPipeline() {
        final PipelineConfig config = PipelineConfig.forLongs(randomBlockSize()).bitPack();

        assertEquals(0, config.transforms().size());
        assertNotNull(config.payload());
        assertEquals(1, config.specs().size());
    }

    public void testRejectsNullDataType() {
        expectThrows(
            NullPointerException.class,
            () -> new PipelineConfig(null, randomBlockSize(), List.of(), new StageSpec.BitPackPayload())
        );
    }

    public void testRejectsNullTransforms() {
        expectThrows(
            NullPointerException.class,
            () -> new PipelineConfig(PipelineDescriptor.DataType.LONG, randomBlockSize(), null, new StageSpec.BitPackPayload())
        );
    }

    public void testRejectsNullPayload() {
        expectThrows(
            NullPointerException.class,
            () -> new PipelineConfig(PipelineDescriptor.DataType.LONG, randomBlockSize(), List.of(), null)
        );
    }

    public void testRejectsInvalidBlockSize() {
        int invalid = randomFrom(0, -1, -randomIntBetween(1, 1000), 100, 300, 99);
        expectThrows(
            IllegalArgumentException.class,
            () -> new PipelineConfig(PipelineDescriptor.DataType.LONG, invalid, List.of(), new StageSpec.BitPackPayload())
        );
    }

    private static int randomBlockSize() {
        return 1 << randomIntBetween(4, 10);
    }
}
