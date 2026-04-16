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
        final byte[] stageIds = { StageId.DELTA_STAGE.id, StageId.BITPACK_PAYLOAD.id };
        final PipelineDescriptor descriptor = new PipelineDescriptor(stageIds, randomBlockSize());
        stageIds[0] = StageId.OFFSET_STAGE.id;
        assertEquals(StageId.DELTA_STAGE.id, descriptor.stageIdAt(0));
    }

    public void testDefensiveCopyOnAccess() {
        final byte[] original = { StageId.DELTA_STAGE.id, StageId.BITPACK_PAYLOAD.id };
        final PipelineDescriptor descriptor = new PipelineDescriptor(original, randomBlockSize());
        descriptor.stageIds()[0] = StageId.OFFSET_STAGE.id;
        assertEquals(StageId.DELTA_STAGE.id, descriptor.stageIdAt(0));
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
        expectThrows(IOException.class, () -> PipelineDescriptor.readFrom(new ByteArrayDataInput(buffer, 0, out1.getPosition())));

        final ByteArrayDataOutput out2 = new ByteArrayDataOutput(buffer);
        out2.writeVInt(PipelineDescriptor.MAX_PIPELINE_LENGTH + 1);
        expectThrows(IOException.class, () -> PipelineDescriptor.readFrom(new ByteArrayDataInput(buffer, 0, out2.getPosition())));
    }

    public void testReadFromInvalidBlockShiftThrows() throws IOException {
        final byte[] buffer = new byte[64];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        out.writeVInt(1);
        out.writeByte((byte) 63);
        expectThrows(IOException.class, () -> PipelineDescriptor.readFrom(new ByteArrayDataInput(buffer, 0, out.getPosition())));
    }

    public void testEqualsAndHashCode() {
        final byte[] stageIds = randomStageIds(randomIntBetween(1, 5));
        final int blockSize = randomBlockSize();
        final PipelineDescriptor d1 = new PipelineDescriptor(stageIds, blockSize);
        final PipelineDescriptor d2 = new PipelineDescriptor(stageIds.clone(), blockSize);

        assertEquals(d1, d2);
        assertEquals(d1.hashCode(), d2.hashCode());

        final byte[] differentStages = randomStageIds(stageIds.length);
        differentStages[0] = stageIds[0] == StageId.DELTA_STAGE.id ? StageId.OFFSET_STAGE.id : StageId.DELTA_STAGE.id;
        assertNotEquals(d1, new PipelineDescriptor(differentStages, blockSize));
        assertNotEquals(d1, new PipelineDescriptor(stageIds, blockSize == 128 ? 256 : 128));
    }

    public void testDefaultConstructorProducesLongDataType() {
        assertEquals(
            PipelineDescriptor.DataType.LONG,
            new PipelineDescriptor(new byte[] { StageId.DELTA_STAGE.id, StageId.BITPACK_PAYLOAD.id }, randomBlockSize()).dataType()
        );
    }

    public void testDataTypeRoundTrip() throws IOException {
        for (final PipelineDescriptor.DataType dataType : PipelineDescriptor.DataType.values()) {
            final PipelineDescriptor original = new PipelineDescriptor(
                new byte[] { StageId.DELTA_STAGE.id, StageId.OFFSET_STAGE.id, StageId.BITPACK_PAYLOAD.id },
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
        final byte[] stageIds = { StageId.DELTA_STAGE.id, StageId.BITPACK_PAYLOAD.id };
        final int blockSize = randomBlockSize();
        final PipelineDescriptor longDesc = new PipelineDescriptor(stageIds, blockSize, PipelineDescriptor.DataType.LONG);
        final PipelineDescriptor doubleDesc = new PipelineDescriptor(stageIds.clone(), blockSize, PipelineDescriptor.DataType.DOUBLE);

        assertNotEquals(longDesc, doubleDesc);
        assertNotEquals(longDesc.hashCode(), doubleDesc.hashCode());
    }

    public void testDataTypeFromIdUnknownThrows() {
        expectThrows(IOException.class, () -> PipelineDescriptor.DataType.fromId((byte) 0x99));
    }

    public void testWithBlockSize() {
        final PipelineDescriptor original = new PipelineDescriptor(new byte[] { StageId.DELTA_STAGE.id, StageId.BITPACK_PAYLOAD.id }, 128);

        final PipelineDescriptor sameSize = original.withBlockSize(128);
        assertSame(original, sameSize);

        final PipelineDescriptor differentSize = original.withBlockSize(256);
        assertNotSame(original, differentSize);
        assertEquals(256, differentSize.blockSize());
        assertEquals(original.pipelineLength(), differentSize.pipelineLength());
        assertEquals(original.dataType(), differentSize.dataType());
        for (int i = 0; i < original.pipelineLength(); i++) {
            assertEquals(original.stageIdAt(i), differentSize.stageIdAt(i));
        }
    }

    public void testPipelineConfigToDescriptorStageIds() {
        final PipelineConfig config = PipelineConfig.forLongs(128).delta().offset().gcd().bitPack();
        final byte[] stageIds = new byte[config.specs().size()];
        for (int i = 0; i < config.specs().size(); i++) {
            stageIds[i] = config.specs().get(i).stageId().id;
        }
        final PipelineDescriptor descriptor = new PipelineDescriptor(stageIds, config.blockSize());

        assertEquals(config.specs().size(), descriptor.pipelineLength());
        for (int i = 0; i < config.specs().size(); i++) {
            assertEquals(config.specs().get(i).stageId().id, descriptor.stageIdAt(i));
        }
    }

    private byte[] randomStageIds(final int length) {
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
