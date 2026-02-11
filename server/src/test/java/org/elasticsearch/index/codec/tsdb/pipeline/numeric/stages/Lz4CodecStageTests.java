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
import java.util.stream.LongStream;

public class Lz4CodecStageTests extends PayloadCodecStageTestCase {

    public void testIdAndName() {
        assertEquals((byte) 0xA5, new Lz4EncodeStage(128).id());
        assertEquals((byte) 0xA5, new Lz4DecodeStage(128).id());
    }

    public void testRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(LongStream.range(0, blockSize).map(i -> i % 256).toArray(), blockSize, false);
    }

    public void testRoundTripHighCompression() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(LongStream.range(0, blockSize).map(i -> i % 256).toArray(), blockSize, true);
    }

    public void testRoundTripAllZeros() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(new long[blockSize], blockSize, randomBoolean());
    }

    public void testRoundTripLargeValues() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(
            LongStream.range(0, blockSize).map(i -> i * randomLongBetween(100000, 1000000)).toArray(),
            blockSize,
            randomBoolean()
        );
    }

    public void testRoundTripRandomValues() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(LongStream.generate(Lz4CodecStageTests::randomLong).limit(blockSize).toArray(), blockSize, randomBoolean());
    }

    public void testPartialBlockRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final int valueCount = randomIntBetween(1, blockSize - 1);
        final boolean highCompression = randomBoolean();
        final long[] original = new long[blockSize];
        for (int i = 0; i < valueCount; i++) {
            original[i] = randomLong();
        }

        final long[] values = original.clone();
        final EncodingContext encodingContext = createEncodingContext(blockSize);

        final byte[] dataBuffer = new byte[blockSize * Long.BYTES * 2];
        final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(dataBuffer);
        new Lz4EncodeStage(blockSize, highCompression).encode(values, valueCount, dataOutput, encodingContext);

        final DecodingContext decodingContext = createDecodingContext(blockSize);
        final ByteArrayDataInput dataInput = new ByteArrayDataInput(dataBuffer, 0, dataOutput.getPosition());
        final long[] decoded = new long[blockSize];
        assertEquals(blockSize, new Lz4DecodeStage(blockSize).decode(decoded, dataInput, decodingContext));
        assertArrayEquals(original, decoded);
    }

    public void testBufferReuseMultipleOperations() throws IOException {
        final int blockSize = randomBlockSize();
        final boolean highCompression = randomBoolean();
        final Lz4EncodeStage encodeStage = new Lz4EncodeStage(blockSize, highCompression);
        final Lz4DecodeStage decodeStage = new Lz4DecodeStage(blockSize);

        for (int iter = 0; iter < 5; iter++) {
            final long[] original = LongStream.generate(Lz4CodecStageTests::randomLong).limit(blockSize).toArray();
            final long[] values = original.clone();
            final EncodingContext encodingContext = createEncodingContext(blockSize);

            final byte[] dataBuffer = new byte[blockSize * Long.BYTES * 2];
            final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(dataBuffer);
            encodeStage.encode(values, values.length, dataOutput, encodingContext);

            final DecodingContext decodingContext = createDecodingContext(blockSize);
            final ByteArrayDataInput dataInput = new ByteArrayDataInput(dataBuffer, 0, dataOutput.getPosition());
            final long[] decoded = new long[blockSize];
            assertEquals(blockSize, decodeStage.decode(decoded, dataInput, decodingContext));
            assertArrayEquals("Failed on iteration " + iter, original, decoded);
        }
    }

    public void testBlockWithTrailingZeros() throws IOException {
        final int blockSize = randomBlockSize();
        final int numNonZeroValues = randomIntBetween(1, blockSize / 2);
        final long[] original = new long[blockSize];
        for (int i = 0; i < numNonZeroValues; i++) {
            original[i] = randomLongBetween(1, 1000);
        }
        assertRoundTrip(original, blockSize, randomBoolean());
    }

    public void testValueCountExceedsBlockSizeThrows() throws IOException {
        final int blockSize = randomBlockSize();
        final int tooLarge = blockSize + randomIntBetween(1, 100);
        final long[] values = new long[tooLarge];
        final EncodingContext encodingContext = createEncodingContext(blockSize);

        final byte[] dataBuffer = new byte[tooLarge * Long.BYTES * 2];
        final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(dataBuffer);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new Lz4EncodeStage(blockSize).encode(values, values.length, dataOutput, encodingContext)
        );
        assertTrue(e.getMessage().contains("exceeds block size"));
    }

    public void testEmptyBlock() throws IOException {
        final int blockSize = randomBlockSize();
        final EncodingContext encodingContext = createEncodingContext(blockSize);
        final byte[] dataBuffer = new byte[64];
        final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(dataBuffer);
        new Lz4EncodeStage(blockSize).encode(new long[blockSize], 0, dataOutput, encodingContext);

        final DecodingContext decodingContext = createDecodingContext(blockSize);
        final ByteArrayDataInput dataInput = new ByteArrayDataInput(dataBuffer, 0, dataOutput.getPosition());
        assertEquals(0, new Lz4DecodeStage(blockSize).decode(new long[blockSize], dataInput, decodingContext));
    }

    public void testEqualsHashCode() {
        final var a = new Lz4EncodeStage(128, false);
        final var b = new Lz4EncodeStage(128, false);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, new Lz4EncodeStage(128, true));
        assertNotEquals(a, new Lz4EncodeStage(256, false));

        final var c = new Lz4DecodeStage(128);
        final var d = new Lz4DecodeStage(128);
        assertEquals(c, d);
        assertEquals(c.hashCode(), d.hashCode());
        assertNotEquals(c, new Lz4DecodeStage(256));
    }

    public void testToString() {
        assertEquals("Lz4EncodeStage{highCompression=false, blockSize=128}", new Lz4EncodeStage(128).toString());
        assertEquals("Lz4EncodeStage{highCompression=true, blockSize=256}", new Lz4EncodeStage(256, true).toString());
        assertEquals("Lz4DecodeStage{blockSize=128}", new Lz4DecodeStage(128).toString());
    }

    private EncodingContext createEncodingContext(int blockSize) {
        return createEncodingContext(blockSize, StageId.LZ4.id);
    }

    private DecodingContext createDecodingContext(int blockSize) {
        return createDecodingContext(blockSize, StageId.LZ4.id);
    }

    private void assertRoundTrip(final long[] original, int blockSize, boolean highCompression) throws IOException {
        final long[] values = original.clone();
        final EncodingContext encodingContext = createEncodingContext(blockSize);

        final byte[] dataBuffer = new byte[blockSize * Long.BYTES * 2];
        final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(dataBuffer);
        new Lz4EncodeStage(blockSize, highCompression).encode(values, original.length, dataOutput, encodingContext);

        final DecodingContext decodingContext = createDecodingContext(blockSize);
        final ByteArrayDataInput dataInput = new ByteArrayDataInput(dataBuffer, 0, dataOutput.getPosition());
        final long[] decoded = new long[original.length];
        assertEquals(blockSize, new Lz4DecodeStage(blockSize).decode(decoded, dataInput, decodingContext));
        assertArrayEquals(original, decoded);
    }
}
