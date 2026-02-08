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
        final byte[] stageIds = { StageId.DELTA.id, StageId.BIT_PACK.id };
        final PipelineDescriptor descriptor = new PipelineDescriptor(stageIds, randomBlockSize());

        stageIds[0] = StageId.OFFSET.id;

        assertEquals(StageId.DELTA.id, descriptor.stageIdAt(0));
    }

    public void testDefensiveCopyOnAccess() {
        final byte[] original = { StageId.DELTA.id, StageId.BIT_PACK.id };
        final PipelineDescriptor descriptor = new PipelineDescriptor(original, randomBlockSize());

        final byte[] returned = descriptor.stageIds();
        returned[0] = StageId.OFFSET.id;

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
        final int length = randomIntBetween(1, PipelineDescriptor.MAX_PIPELINE_LENGTH);
        final byte[] stageIds = randomStageIds(length);
        final int blockSize = randomBlockSize();
        final PipelineDescriptor original = new PipelineDescriptor(stageIds, blockSize);

        final byte[] buffer = new byte[128];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        original.writeTo(out);

        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());
        final PipelineDescriptor restored = PipelineDescriptor.readFrom(in);

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
        final int blockSize = 128;

        final PipelineDescriptor d1 = new PipelineDescriptor(stageIds, blockSize);
        final PipelineDescriptor d2 = new PipelineDescriptor(stageIds.clone(), blockSize);

        assertEquals(d1, d2);
        assertEquals(d1.hashCode(), d2.hashCode());

        final byte[] differentStages = randomStageIds(stageIds.length);
        differentStages[0] = stageIds[0] == StageId.DELTA.id ? StageId.OFFSET.id : StageId.DELTA.id;
        assertNotEquals(d1, new PipelineDescriptor(differentStages, blockSize));

        assertNotEquals(d1, new PipelineDescriptor(stageIds, 256));
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
        return 1 << randomIntBetween(4, 12);
    }
}
