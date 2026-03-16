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

    private static int randomBlockSize() {
        return 128 << randomIntBetween(0, 7);
    }

    public void testLongBuilderFluency() {
        final int blockSize = randomBlockSize();
        final PipelineConfig config = PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();
        assertEquals(PipelineDescriptor.DataType.LONG, config.dataType());
        assertEquals(blockSize, config.blockSize());
        assertEquals(4, config.specs().size());
    }

    public void testBlockSizePreserved() {
        final int blockSize = randomBlockSize();
        assertEquals(blockSize, PipelineConfig.forLongs(blockSize).delta().bitPack().blockSize());
    }

    public void testEquality() {
        final int blockSize = randomBlockSize();
        final PipelineConfig config1 = PipelineConfig.forLongs(blockSize).delta().bitPack();
        final PipelineConfig config2 = PipelineConfig.forLongs(blockSize).delta().bitPack();

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    public void testInequalityByStages() {
        final int blockSize = randomBlockSize();
        assertNotEquals(PipelineConfig.forLongs(blockSize).delta().bitPack(), PipelineConfig.forLongs(blockSize).offset().bitPack());
    }

    public void testInequalityByDataType() {
        final int blockSize = randomBlockSize();
        final List<StageSpec> stages = List.of(new StageSpec.DeltaStage(), new StageSpec.BitPackPayload());
        assertNotEquals(
            PipelineConfig.of(PipelineDescriptor.DataType.LONG, blockSize, stages),
            PipelineConfig.of(PipelineDescriptor.DataType.DOUBLE, blockSize, stages)
        );
    }

    public void testInequalityByBlockSize() {
        assertNotEquals(PipelineConfig.forLongs(128).delta().bitPack(), PipelineConfig.forLongs(256).delta().bitPack());
    }

    public void testDataTypePreserved() {
        final int blockSize = randomBlockSize();
        assertEquals(PipelineDescriptor.DataType.LONG, PipelineConfig.forLongs(blockSize).delta().bitPack().dataType());
    }

    public void testSpecsPreserved() {
        final PipelineConfig config = PipelineConfig.forLongs(randomBlockSize()).delta().offset().bitPack();

        assertEquals(3, config.specs().size());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.DeltaStage.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.OffsetStage.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.BitPackPayload.class)));
    }

    public void testSpecsAreUnmodifiable() {
        final PipelineConfig config = PipelineConfig.forLongs(randomBlockSize()).delta().bitPack();
        expectThrows(UnsupportedOperationException.class, () -> config.specs().add(new StageSpec.OffsetStage()));
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

    public void testDescribeStagesWithEmptySpecs() {
        assertEquals("empty", PipelineConfig.of(PipelineDescriptor.DataType.LONG, randomBlockSize(), List.of()).describeStages());
    }

    public void testOfFactoryMethod() {
        final int blockSize = randomBlockSize();
        final PipelineConfig config = PipelineConfig.of(
            PipelineDescriptor.DataType.LONG,
            blockSize,
            List.of(new StageSpec.DeltaStage(), new StageSpec.BitPackPayload())
        );
        assertEquals(PipelineDescriptor.DataType.LONG, config.dataType());
        assertEquals(blockSize, config.blockSize());
        assertEquals(2, config.specs().size());
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

    public void testRejectsNullDataType() {
        expectThrows(NullPointerException.class, () -> new PipelineConfig(null, randomBlockSize(), List.of()));
    }

    public void testRejectsNullSpecs() {
        expectThrows(NullPointerException.class, () -> new PipelineConfig(PipelineDescriptor.DataType.LONG, randomBlockSize(), null));
    }

    public void testRejectsInvalidBlockSize() {
        final int invalid = randomFrom(0, -1, -randomIntBetween(1, 1000), 100, 300, 99);
        expectThrows(
            IllegalArgumentException.class,
            () -> new PipelineConfig(PipelineDescriptor.DataType.LONG, invalid, List.of(new StageSpec.BitPackPayload()))
        );
    }
}
