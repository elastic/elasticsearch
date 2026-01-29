/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.LongStream;

public class BitPackCodecStageTests extends PayloadCodecStageTestCase {

    public void testIdAndName() throws IOException {
        try (BitPackCodecStage stage = new BitPackCodecStage(randomBlockSize())) {
            assertEquals((byte) 0xA1, stage.id());
            assertEquals("bit-pack", stage.name());
        }
    }

    public void testEncodeWritesBitsPerValueAndPackedData() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = LongStream.generate(() -> randomLongBetween(0, 255)).limit(blockSize).toArray();
        final EncodingContext context = createEncodingContext(blockSize);
        final byte[] dataBuffer = new byte[blockSize * Long.BYTES + 64];
        final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(dataBuffer);
        try (BitPackCodecStage stage = new BitPackCodecStage(blockSize)) {
            stage.encode(values, values.length, dataOutput, context);
        }

        assertTrue(dataOutput.getPosition() > 0);
        final ByteArrayDataInput payloadIn = new ByteArrayDataInput(dataBuffer, 0, dataOutput.getPosition());
        assertEquals(8, payloadIn.readVInt());
    }

    public void testAllZerosWritesOnlyBitsPerValue() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        final EncodingContext context = createEncodingContext(blockSize);
        final byte[] dataBuffer = new byte[blockSize * Long.BYTES + 64];
        final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(dataBuffer);
        try (BitPackCodecStage stage = new BitPackCodecStage(blockSize)) {
            stage.encode(values, values.length, dataOutput, context);
        }

        assertEquals(1, dataOutput.getPosition());
        final ByteArrayDataInput payloadIn = new ByteArrayDataInput(dataBuffer, 0, dataOutput.getPosition());
        assertEquals(0, payloadIn.readVInt());
    }

    public void testRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(LongStream.generate(BitPackCodecStageTests::randomLong).limit(blockSize).toArray(), blockSize, blockSize);
    }

    public void testRoundTripZeroBits() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(new long[blockSize], blockSize, blockSize);
    }

    public void testBitsPerValueComputation() throws IOException {
        final int blockSize = randomBlockSize();
        try (BitPackCodecStage stage = new BitPackCodecStage(blockSize)) {

            final long[] oneBitValues = LongStream.generate(() -> randomLongBetween(0, 1)).limit(blockSize).toArray();
            final EncodingContext context1 = createEncodingContext(blockSize);

            final byte[] dataBuffer1 = new byte[blockSize * Long.BYTES + 64];
            final ByteArrayDataOutput dataOut1 = new ByteArrayDataOutput(dataBuffer1);
            stage.encode(oneBitValues, oneBitValues.length, dataOut1, context1);

            final ByteArrayDataInput payloadIn1 = new ByteArrayDataInput(dataBuffer1, 0, dataOut1.getPosition());
            assertEquals(1, payloadIn1.readVInt());

            final long[] eightBitValues = LongStream.generate(() -> randomLongBetween(128, 255)).limit(blockSize).toArray();
            final EncodingContext context2 = createEncodingContext(blockSize);

            final byte[] dataBuffer2 = new byte[blockSize * Long.BYTES + 64];
            final ByteArrayDataOutput dataOut2 = new ByteArrayDataOutput(dataBuffer2);
            stage.encode(eightBitValues, eightBitValues.length, dataOut2, context2);

            final ByteArrayDataInput payloadIn2 = new ByteArrayDataInput(dataBuffer2, 0, dataOut2.getPosition());
            assertEquals(8, payloadIn2.readVInt());
        }
    }

    public void testRoundTrip64BitValues() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(
            LongStream.concat(LongStream.of(Long.MAX_VALUE, Long.MIN_VALUE), LongStream.generate(BitPackCodecStageTests::randomLong))
                .limit(blockSize)
                .toArray(),
            blockSize,
            blockSize
        );
    }

    public void testSpanningValues() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(LongStream.range(0, blockSize).map(i -> (1L << 23) + i).toArray(), blockSize, blockSize);
    }

    public void testPartialBlockRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final int partialSize = randomIntBetween(10, blockSize - 1);
        assertRoundTrip(LongStream.range(0, partialSize).map(i -> (1L << 23) + i).toArray(), partialSize, blockSize);
    }

    public void testPartialBlockSingleValue() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(new long[] { randomLong() }, 1, blockSize);
    }

    public void testPartialBlockAllZeros() throws IOException {
        final int blockSize = randomBlockSize();
        final int partialSize = randomIntBetween(10, blockSize - 1);
        assertRoundTrip(new long[partialSize], partialSize, blockSize);
    }

    public void testPartialBlockLargeValues() throws IOException {
        final int blockSize = randomBlockSize();
        final int partialSize = randomIntBetween(10, blockSize - 1);
        assertRoundTrip(LongStream.range(0, partialSize).map(i -> Long.MAX_VALUE - i).toArray(), partialSize, blockSize);
    }

    public void testPartialBlockSmallValues() throws IOException {
        final int blockSize = randomBlockSize();
        final int partialSize = randomIntBetween(10, blockSize - 1);
        assertRoundTrip(LongStream.range(0, partialSize).map(i -> Long.MIN_VALUE + i).toArray(), partialSize, blockSize);
    }

    private EncodingContext createEncodingContext(int blockSize) {
        return createEncodingContext(blockSize, StageId.BIT_PACK.id);
    }

    private void assertRoundTrip(final long[] original, int valueCount, int blockSize) throws IOException {
        final long[] values = new long[blockSize];
        System.arraycopy(original, 0, values, 0, Math.min(original.length, valueCount));

        final EncodingContext encodingContext = createEncodingContext(blockSize);
        try (BitPackCodecStage bitPackStage = new BitPackCodecStage(blockSize)) {

            final byte[] dataBuffer = new byte[blockSize * Long.BYTES + 64];
            final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(dataBuffer);
            bitPackStage.encode(values, valueCount, dataOutput, encodingContext);

            final DecodingContext decodingContext = createDecodingContext(blockSize, StageId.BIT_PACK.id);
            final ByteArrayDataInput dataInput = new ByteArrayDataInput(dataBuffer, 0, dataOutput.getPosition());

            final long[] decoded = new long[blockSize];
            int decodedCount = bitPackStage.decode(decoded, dataInput, decodingContext);

            assertEquals(blockSize, decodedCount);
            assertArrayEquals(original, Arrays.copyOf(decoded, original.length));
        }
    }
}
