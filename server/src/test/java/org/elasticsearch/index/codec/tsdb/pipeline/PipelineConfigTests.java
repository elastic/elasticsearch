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

    public void testDoubleBuilderFluency() {
        final int blockSize = randomBlockSize();
        final PipelineConfig config = PipelineConfig.forDoubles(blockSize).alpDoubleStage().gorilla();
        assertEquals(PipelineDescriptor.DataType.DOUBLE, config.dataType());
        assertEquals(blockSize, config.blockSize());
        assertEquals(2, config.specs().size());
    }

    public void testFloatBuilderFluency() {
        final int blockSize = randomBlockSize();
        final PipelineConfig config = PipelineConfig.forFloats(blockSize).alpFloatStage().gorilla();
        assertEquals(PipelineDescriptor.DataType.FLOAT, config.dataType());
        assertEquals(blockSize, config.blockSize());
        assertEquals(2, config.specs().size());
    }

    public void testBlockSizePreserved() {
        final int blockSize = randomBlockSize();
        assertEquals(blockSize, PipelineConfig.forLongs(blockSize).delta().bitPack().blockSize());
        assertEquals(blockSize, PipelineConfig.forDoubles(blockSize).alpDoubleStage().gorilla().blockSize());
        assertEquals(blockSize, PipelineConfig.forFloats(blockSize).alpFloatStage().gorilla().blockSize());
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
        assertNotEquals(PipelineConfig.forLongs(blockSize).delta().bitPack(), PipelineConfig.forDoubles(blockSize).delta().bitPack());
    }

    public void testInequalityByBlockSize() {
        assertNotEquals(PipelineConfig.forLongs(128).delta().bitPack(), PipelineConfig.forLongs(256).delta().bitPack());
    }

    public void testDataTypePreserved() {
        final int blockSize = randomBlockSize();
        assertEquals(PipelineDescriptor.DataType.LONG, PipelineConfig.forLongs(blockSize).delta().bitPack().dataType());
        assertEquals(PipelineDescriptor.DataType.DOUBLE, PipelineConfig.forDoubles(blockSize).alpDoubleStage().gorilla().dataType());
        assertEquals(PipelineDescriptor.DataType.FLOAT, PipelineConfig.forFloats(blockSize).alpFloatStage().gorilla().dataType());
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
        final PipelineConfig config = PipelineConfig.forLongs(randomBlockSize()).delta().offset().gcd().patchedPFor().xor().bitPack();

        assertEquals(6, config.specs().size());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.DeltaStage.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.OffsetStage.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.GcdStage.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.PatchedPForStage.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.XorStage.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.BitPackPayload.class)));
    }

    public void testLongTerminalStages() {
        final int blockSize = randomBlockSize();
        assertThat(PipelineConfig.forLongs(blockSize).bitPack().specs(), hasItem(instanceOf(StageSpec.BitPackPayload.class)));
        assertThat(PipelineConfig.forLongs(blockSize).zstd().specs(), hasItem(instanceOf(StageSpec.ZstdPayload.class)));
        assertThat(PipelineConfig.forLongs(blockSize).lz4().specs(), hasItem(instanceOf(StageSpec.Lz4Payload.class)));
    }

    public void testDoubleTerminalStages() {
        final int blockSize = randomBlockSize();
        assertThat(PipelineConfig.forDoubles(blockSize).gorilla().specs(), hasItem(instanceOf(StageSpec.GorillaDoublePayload.class)));
        assertThat(PipelineConfig.forDoubles(blockSize).chimp().specs(), hasItem(instanceOf(StageSpec.ChimpDoublePayload.class)));
        assertThat(PipelineConfig.forDoubles(blockSize).chimp128().specs(), hasItem(instanceOf(StageSpec.Chimp128DoublePayload.class)));
    }

    public void testFloatTerminalStages() {
        final int blockSize = randomBlockSize();
        assertThat(PipelineConfig.forFloats(blockSize).gorilla().specs(), hasItem(instanceOf(StageSpec.GorillaFloatPayload.class)));
        assertThat(PipelineConfig.forFloats(blockSize).chimp().specs(), hasItem(instanceOf(StageSpec.ChimpFloatPayload.class)));
        assertThat(PipelineConfig.forFloats(blockSize).chimp128().specs(), hasItem(instanceOf(StageSpec.Chimp128FloatPayload.class)));
    }

    public void testLz4HighCompression() {
        final int blockSize = randomBlockSize();
        final PipelineConfig config = switch (randomIntBetween(0, 2)) {
            case 0 -> PipelineConfig.forLongs(blockSize).delta().lz4HighCompression();
            case 1 -> PipelineConfig.forDoubles(blockSize).lz4HighCompression();
            default -> PipelineConfig.forFloats(blockSize).lz4HighCompression();
        };
        final StageSpec.Lz4Payload lz4 = config.specs()
            .stream()
            .filter(s -> s instanceof StageSpec.Lz4Payload)
            .map(s -> (StageSpec.Lz4Payload) s)
            .findFirst()
            .orElseThrow();
        assertTrue(lz4.highCompression());
    }

    public void testAlpDoubleStageWithMaxError() {
        final int blockSize = randomBlockSize();
        final double maxError = randomDoubleBetween(0.001, 1.0, true);
        final PipelineConfig config = PipelineConfig.forDoubles(blockSize).alpDoubleStage(maxError).gorilla();
        final StageSpec.AlpDoubleStage alp = (StageSpec.AlpDoubleStage) config.specs().getFirst();
        assertEquals(maxError, alp.maxError(), 0.0);
    }

    public void testAlpFloatStageWithMaxError() {
        final int blockSize = randomBlockSize();
        final double maxError = randomDoubleBetween(0.001, 1.0, true);
        final PipelineConfig config = PipelineConfig.forFloats(blockSize).alpFloatStage(maxError).gorilla();
        final StageSpec.AlpFloatStage alp = (StageSpec.AlpFloatStage) config.specs().getFirst();
        assertEquals(maxError, alp.maxError(), 0.0);
    }

    public void testFpcDoubleStageVariants() {
        final int blockSize = randomBlockSize();
        final int tableSize = randomIntBetween(0, 2048);
        final double maxError = randomDoubleBetween(0.0, 1.0, true);

        assertThat(PipelineConfig.forDoubles(blockSize).fpcStage().bitPack().specs(), hasItem(instanceOf(StageSpec.FpcDoubleStage.class)));
        assertEquals(
            tableSize,
            ((StageSpec.FpcDoubleStage) PipelineConfig.forDoubles(blockSize).fpcStage(tableSize).bitPack().specs().getFirst()).tableSize()
        );
        assertEquals(
            maxError,
            ((StageSpec.FpcDoubleStage) PipelineConfig.forDoubles(blockSize).fpcStage(maxError).bitPack().specs().getFirst()).maxError(),
            0.0
        );
        final StageSpec.FpcDoubleStage fpc = (StageSpec.FpcDoubleStage) PipelineConfig.forDoubles(blockSize)
            .fpcStage(tableSize, maxError)
            .bitPack()
            .specs()
            .getFirst();
        assertEquals(tableSize, fpc.tableSize());
        assertEquals(maxError, fpc.maxError(), 0.0);
    }

    public void testFpcFloatStageVariants() {
        final int blockSize = randomBlockSize();
        final int tableSize = randomIntBetween(0, 2048);
        final double maxError = randomDoubleBetween(0.0, 1.0, true);

        assertThat(PipelineConfig.forFloats(blockSize).fpcStage().bitPack().specs(), hasItem(instanceOf(StageSpec.FpcFloatStage.class)));
        assertEquals(
            tableSize,
            ((StageSpec.FpcFloatStage) PipelineConfig.forFloats(blockSize).fpcStage(tableSize).bitPack().specs().getFirst()).tableSize()
        );
        assertEquals(
            maxError,
            ((StageSpec.FpcFloatStage) PipelineConfig.forFloats(blockSize).fpcStage(maxError).bitPack().specs().getFirst()).maxError(),
            0.0
        );
        final StageSpec.FpcFloatStage fpc = (StageSpec.FpcFloatStage) PipelineConfig.forFloats(blockSize)
            .fpcStage(tableSize, maxError)
            .bitPack()
            .specs()
            .getFirst();
        assertEquals(tableSize, fpc.tableSize());
        assertEquals(maxError, fpc.maxError(), 0.0);
    }

    public void testDoublePayloadMaxError() {
        final int blockSize = randomBlockSize();
        final double maxError = randomDoubleBetween(0.0, 1.0, true);
        assertEquals(
            maxError,
            ((StageSpec.GorillaDoublePayload) PipelineConfig.forDoubles(blockSize).gorilla(maxError).specs().getFirst()).maxError(),
            0.0
        );
        assertEquals(
            maxError,
            ((StageSpec.ChimpDoublePayload) PipelineConfig.forDoubles(blockSize).chimp(maxError).specs().getFirst()).maxError(),
            0.0
        );
        assertEquals(
            maxError,
            ((StageSpec.Chimp128DoublePayload) PipelineConfig.forDoubles(blockSize).chimp128(maxError).specs().getFirst()).maxError(),
            0.0
        );
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
