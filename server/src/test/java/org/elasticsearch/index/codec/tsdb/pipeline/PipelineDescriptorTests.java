/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class PipelineDescriptorTests extends ESTestCase {

    public void testConstructionAndAccessors() {
        final int length = randomIntBetween(1, PipelineDescriptor.MAX_PIPELINE_LENGTH);
        final byte[] stageIds = randomStageIds(length);
        final int blockSize = randomBlockSize();
        final PipelineDescriptor descriptor = new PipelineDescriptor(stageIds, blockSize);

        assertEquals(length, descriptor.pipelineLength());
        assertEquals(Integer.numberOfTrailingZeros(blockSize), descriptor.blockShift());
        assertEquals(blockSize, descriptor.blockSize());
        for (int i = 0; i < length; i++) {
            assertEquals(stageIds[i], descriptor.stageIdAt(i));
        }
    }

    public void testBitmapBytesCalculation() {
        final int blockSize = randomBlockSize();
        assertEquals(1, new PipelineDescriptor(randomStageIds(1), blockSize).bitmapBytes());
        assertEquals(1, new PipelineDescriptor(randomStageIds(8), blockSize).bitmapBytes());
        assertEquals(2, new PipelineDescriptor(randomStageIds(9), blockSize).bitmapBytes());
        assertEquals(2, new PipelineDescriptor(randomStageIds(PipelineDescriptor.MAX_PIPELINE_LENGTH), blockSize).bitmapBytes());
    }

    public void testDefensiveCopyOnConstruction() {
        final byte[] stageIds = { StageId.DELTA.id, StageId.BIT_PACK.id };
        final PipelineDescriptor descriptor = new PipelineDescriptor(stageIds, randomBlockSize());
        stageIds[0] = StageId.OFFSET.id;
        assertEquals(StageId.DELTA.id, descriptor.stageIdAt(0));
    }

    public void testDefensiveCopyOnAccess() {
        final byte[] original = { StageId.DELTA.id, StageId.BIT_PACK.id };
        final PipelineDescriptor descriptor = new PipelineDescriptor(original, randomBlockSize());
        descriptor.stageIds()[0] = StageId.OFFSET.id;
        assertEquals(StageId.DELTA.id, descriptor.stageIdAt(0));
    }

    public void testInvalidConstructionThrows() {
        final int validBlockSize = randomBlockSize();
        expectThrows(IllegalArgumentException.class, () -> new PipelineDescriptor(null, validBlockSize));
        expectThrows(IllegalArgumentException.class, () -> new PipelineDescriptor(new byte[0], validBlockSize));
        expectThrows(
            IllegalArgumentException.class,
            () -> new PipelineDescriptor(new byte[PipelineDescriptor.MAX_PIPELINE_LENGTH + 1], validBlockSize)
        );

        final byte[] validStageIds = randomStageIds(randomIntBetween(1, 5));
        expectThrows(IllegalArgumentException.class, () -> new PipelineDescriptor(validStageIds, 0));
        expectThrows(IllegalArgumentException.class, () -> new PipelineDescriptor(validStageIds, -1));
        expectThrows(IllegalArgumentException.class, () -> new PipelineDescriptor(validStageIds, 100));
    }

    public void testRoundTrip() throws IOException {
        final PipelineDescriptor original = new PipelineDescriptor(
            randomStageIds(randomIntBetween(1, PipelineDescriptor.MAX_PIPELINE_LENGTH)),
            randomBlockSize()
        );

        final byte[] buffer = new byte[128];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        original.writeTo(out);

        final PipelineDescriptor restored = PipelineDescriptor.readFrom(new ByteArrayDataInput(buffer, 0, out.getPosition()));

        assertEquals(original, restored);
        assertEquals(original.pipelineLength(), restored.pipelineLength());
        assertEquals(original.blockSize(), restored.blockSize());
        for (int i = 0; i < original.pipelineLength(); i++) {
            assertEquals(original.stageIdAt(i), restored.stageIdAt(i));
        }
    }

    public void testReadFromInvalidThrows() throws IOException {
        final byte[] buffer = new byte[64];

        final ByteArrayDataOutput out1 = new ByteArrayDataOutput(buffer);
        out1.writeVInt(0);
        expectThrows(
            IllegalArgumentException.class,
            () -> PipelineDescriptor.readFrom(new ByteArrayDataInput(buffer, 0, out1.getPosition()))
        );

        final ByteArrayDataOutput out2 = new ByteArrayDataOutput(buffer);
        out2.writeVInt(PipelineDescriptor.MAX_PIPELINE_LENGTH + 1);
        expectThrows(
            IllegalArgumentException.class,
            () -> PipelineDescriptor.readFrom(new ByteArrayDataInput(buffer, 0, out2.getPosition()))
        );
    }

    public void testEqualsAndHashCode() {
        final byte[] stageIds = randomStageIds(randomIntBetween(1, 5));
        final int blockSize = randomBlockSize();
        final PipelineDescriptor d1 = new PipelineDescriptor(stageIds, blockSize);
        final PipelineDescriptor d2 = new PipelineDescriptor(stageIds.clone(), blockSize);

        assertEquals(d1, d2);
        assertEquals(d1.hashCode(), d2.hashCode());

        final byte[] differentStages = randomStageIds(stageIds.length);
        differentStages[0] = stageIds[0] == StageId.DELTA.id ? StageId.OFFSET.id : StageId.DELTA.id;
        assertNotEquals(d1, new PipelineDescriptor(differentStages, blockSize));
        assertNotEquals(d1, new PipelineDescriptor(stageIds, blockSize == 128 ? 256 : 128));
    }

    public void testDefaultConstructorProducesLongDataType() {
        assertEquals(
            PipelineDescriptor.DataType.LONG,
            new PipelineDescriptor(new byte[] { StageId.DELTA.id, StageId.BIT_PACK.id }, randomBlockSize()).dataType()
        );
    }

    public void testDataTypeRoundTrip() throws IOException {
        for (PipelineDescriptor.DataType dataType : PipelineDescriptor.DataType.values()) {
            final PipelineDescriptor original = new PipelineDescriptor(
                new byte[] { StageId.DELTA.id, StageId.OFFSET.id, StageId.BIT_PACK.id },
                randomBlockSize(),
                dataType
            );

            final byte[] buffer = new byte[128];
            final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
            original.writeTo(out);

            final PipelineDescriptor restored = PipelineDescriptor.readFrom(new ByteArrayDataInput(buffer, 0, out.getPosition()));
            assertEquals(original, restored);
            assertEquals(dataType, restored.dataType());
        }
    }

    public void testDifferentDataTypeNotEqual() {
        final byte[] stageIds = { StageId.DELTA.id, StageId.BIT_PACK.id };
        final int blockSize = randomBlockSize();
        final PipelineDescriptor longDesc = new PipelineDescriptor(stageIds, blockSize, PipelineDescriptor.DataType.LONG);
        final PipelineDescriptor doubleDesc = new PipelineDescriptor(stageIds.clone(), blockSize, PipelineDescriptor.DataType.DOUBLE);

        assertNotEquals(longDesc, doubleDesc);
        assertNotEquals(longDesc.hashCode(), doubleDesc.hashCode());
    }

    public void testFromDescriptorRoundTripVariousPipelines() throws IOException {
        final int blockSize = randomBlockSize();
        final PipelineConfig[] configs = {
            PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack(),
            PipelineConfig.forLongs(blockSize).delta().bitPack(),
            PipelineConfig.forLongs(blockSize).rle().bitPack(),
            PipelineConfig.forLongs(blockSize).offset().gcd().bitPack(),
            PipelineConfig.forLongs(blockSize).xor().bitPack(),
            PipelineConfig.forLongs(blockSize).patchedPFor().bitPack(),
            PipelineConfig.forDoubles(blockSize).alpDoubleStage().alpDouble(),
            PipelineConfig.forDoubles(blockSize).alpRdDoubleStage().alpRdDouble(),
            PipelineConfig.forDoubles(blockSize).alpDoubleStage().offset().gcd().bitPack(),
            PipelineConfig.forDoubles(blockSize).alpRdDoubleStage().offset().gcd().bitPack(),
            PipelineConfig.forFloats(blockSize).alpFloatStage().alpFloat(),
            PipelineConfig.forFloats(blockSize).alpRdFloatStage().alpRdFloat(),
            PipelineConfig.forFloats(blockSize).alpFloatStage().offset().gcd().bitPack(),
            PipelineConfig.forFloats(blockSize).alpRdFloatStage().offset().gcd().bitPack() };

        for (PipelineConfig config : configs) {
            try (NumericEncoder encoder = NumericEncoder.fromConfig(config)) {
                final PipelineDescriptor originalDesc = encoder.descriptor();

                final long[] original = generateTestData(config, blockSize);
                final byte[] buffer = new byte[blockSize * Long.BYTES + 4096];
                final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
                encoder.newBlockEncoder().encode(original.clone(), original.length, out);

                try (NumericDecoder decoder = NumericDecoder.fromDescriptor(originalDesc)) {
                    final long[] decoded = new long[blockSize];
                    decoder.newBlockDecoder().decode(decoded, new ByteArrayDataInput(buffer, 0, out.getPosition()));
                    assertArrayEquals("Data round-trip failed for config: " + config, original, decoded);
                }

                try (NumericEncoder reconstructed = NumericEncoder.fromConfig(config)) {
                    assertEquals("Descriptor round-trip failed for config: " + config, originalDesc, reconstructed.descriptor());
                }
            }
        }
    }

    public void testDoublePipelinesProduceDoubleDataType() throws IOException {
        final int blockSize = randomBlockSize();
        final PipelineConfig[] configs = {
            PipelineConfig.forDoubles(blockSize).alpDoubleStage().offset().gcd().bitPack(),
            PipelineConfig.forDoubles(blockSize).alpDoubleStage().gcd().bitPack(),
            PipelineConfig.forDoubles(blockSize).alpRdDoubleStage().offset().gcd().bitPack(),
            PipelineConfig.forDoubles(blockSize).alpRdDoubleStage().gcd().bitPack() };
        for (PipelineConfig config : configs) {
            try (NumericEncoder encoder = NumericEncoder.fromConfig(config)) {
                assertEquals(
                    "Config " + config + " should produce DOUBLE DataType",
                    PipelineDescriptor.DataType.DOUBLE,
                    encoder.descriptor().dataType()
                );
            }
        }
    }

    public void testLongPipelinesProduceLongDataType() throws IOException {
        final int blockSize = randomBlockSize();
        final PipelineConfig[] configs = {
            PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack(),
            PipelineConfig.forLongs(blockSize).delta().bitPack(),
            PipelineConfig.forLongs(blockSize).offset().gcd().bitPack(),
            PipelineConfig.forLongs(blockSize).rle().bitPack(),
            PipelineConfig.forLongs(blockSize).xor().bitPack() };
        for (PipelineConfig config : configs) {
            try (NumericEncoder encoder = NumericEncoder.fromConfig(config)) {
                assertEquals(
                    "Config " + config + " should produce LONG DataType",
                    PipelineDescriptor.DataType.LONG,
                    encoder.descriptor().dataType()
                );
            }
        }
    }

    public void testFromDescriptorUnknownStageIdThrows() {
        expectThrows(
            IllegalArgumentException.class,
            () -> NumericDecoder.fromDescriptor(new PipelineDescriptor(new byte[] { (byte) 0xFF, StageId.BIT_PACK.id }, randomBlockSize()))
        );
    }

    public void testDataTypeFromIdUnknownThrows() {
        expectThrows(IllegalArgumentException.class, () -> PipelineDescriptor.DataType.fromId((byte) 0x99));
    }

    public void testFloatDataTypeRoundTrip() throws IOException {
        for (PipelineDescriptor.DataType dataType : PipelineDescriptor.DataType.values()) {
            final PipelineDescriptor original = new PipelineDescriptor(
                new byte[] { StageId.DELTA.id, StageId.OFFSET.id, StageId.BIT_PACK.id },
                randomBlockSize(),
                dataType
            );

            final byte[] buffer = new byte[128];
            final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
            original.writeTo(out);

            final PipelineDescriptor restored = PipelineDescriptor.readFrom(new ByteArrayDataInput(buffer, 0, out.getPosition()));
            assertEquals(original, restored);
            assertEquals(dataType, restored.dataType());
        }
    }

    public void testFloatPipelinesProduceFloatDataType() {
        final int blockSize = randomBlockSize();
        final PipelineConfig[] configs = {
            PipelineConfig.forFloats(blockSize).alpFloatStage().offset().gcd().bitPack(),
            PipelineConfig.forFloats(blockSize).alpFloatStage().gcd().bitPack(),
            PipelineConfig.forFloats(blockSize).alpRdFloatStage().offset().gcd().bitPack(),
            PipelineConfig.forFloats(blockSize).alpRdFloatStage().gcd().bitPack() };
        for (PipelineConfig config : configs) {
            assertEquals("Config " + config + " should produce FLOAT DataType", PipelineConfig.DataType.FLOAT, config.dataType());
        }
    }

    public void testFloatDoubleStageTypeMismatchThrows() {
        final int blockSize = randomBlockSize();
        expectThrows(
            IllegalArgumentException.class,
            () -> NumericEncoder.fromConfig(
                new PipelineConfig(
                    PipelineConfig.DataType.FLOAT,
                    blockSize,
                    java.util.List.of(new StageSpec.AlpDoubleStage(), new StageSpec.BitPack())
                )
            )
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> NumericEncoder.fromConfig(
                new PipelineConfig(
                    PipelineConfig.DataType.DOUBLE,
                    blockSize,
                    java.util.List.of(new StageSpec.AlpFloatStage(), new StageSpec.BitPack())
                )
            )
        );
    }

    private long[] generateTestData(PipelineConfig config, int size) {
        final long[] data = new long[size];
        switch (config.dataType()) {
            case LONG -> {
                for (int i = 0; i < size; i++) {
                    data[i] = randomLongBetween(0, 1_000_000L);
                }
            }
            case DOUBLE -> {
                for (int i = 0; i < size; i++) {
                    data[i] = NumericUtils.doubleToSortableLong(randomDoubleBetween(1.0, 1000.0, true));
                }
            }
            case FLOAT -> {
                for (int i = 0; i < size; i++) {
                    data[i] = NumericUtils.floatToSortableInt((float) randomDoubleBetween(1.0, 1000.0, true));
                }
            }
        }
        return data;
    }

    private byte[] randomStageIds(int length) {
        final StageId[] allStages = StageId.values();
        final byte[] stageIds = new byte[length];
        for (int i = 0; i < length; i++) {
            stageIds[i] = allStages[randomIntBetween(0, allStages.length - 1)].id;
        }
        return stageIds;
    }

    private int randomBlockSize() {
        return 1 << randomIntBetween(7, 10);
    }
}
