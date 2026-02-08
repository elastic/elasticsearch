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

public class ZstdCodecStageTests extends PayloadCodecStageTestCase {

    public void testIdAndName() {
        try (ZstdCodecStage stage = new ZstdCodecStage(randomBlockSize(), ZstdCodecStage.DEFAULT_COMPRESSION_LEVEL)) {
            assertEquals((byte) 0xA2, stage.id());
            assertEquals("zstd", stage.name());
        }
    }

    public void testCustomBlockSize() {
        final int blockSize = randomBlockSize();
        try (ZstdCodecStage stage = new ZstdCodecStage(blockSize, randomIntBetween(1, 10))) {
            assertEquals(blockSize, stage.blockSize());
        }
    }

    public void testRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(LongStream.range(0, blockSize).map(i -> i % 256).toArray(), blockSize);
    }

    public void testRoundTripAllZeros() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(new long[blockSize], blockSize);
    }

    public void testRoundTripLargeValues() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(LongStream.range(0, blockSize).map(i -> i * randomLongBetween(100000, 1000000)).toArray(), blockSize);
    }

    public void testRoundTripRandomValues() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(LongStream.generate(ZstdCodecStageTests::randomLong).limit(blockSize).toArray(), blockSize);
    }

    public void testPartialBlockRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final int valueCount = randomIntBetween(1, blockSize - 1);
        final long[] original = new long[blockSize];
        for (int i = 0; i < valueCount; i++) {
            original[i] = randomLong();
        }

        final long[] values = original.clone();
        final EncodingContext encodingContext = createEncodingContext(blockSize);

        try (ZstdCodecStage zstdStage = new ZstdCodecStage(blockSize, ZstdCodecStage.DEFAULT_COMPRESSION_LEVEL)) {
            final byte[] dataBuffer = new byte[blockSize * Long.BYTES * 2];
            final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(dataBuffer);
            zstdStage.encode(values, valueCount, dataOutput, encodingContext);

            final DecodingContext decodingContext = createDecodingContext(blockSize);
            final ByteArrayDataInput dataInput = new ByteArrayDataInput(dataBuffer, 0, dataOutput.getPosition());
            final long[] decoded = new long[blockSize];

            assertEquals(blockSize, zstdStage.decode(decoded, dataInput, decodingContext));
            assertArrayEquals(original, decoded);
        }
    }

    public void testCompressionLevel() throws IOException {
        final int blockSize = randomBlockSize();
        final long value = randomLong();
        final long[] values = LongStream.generate(() -> value).limit(blockSize).toArray();

        for (int level : new int[] { 1, 3, 6 }) {
            assertRoundTripWithLevel(values, blockSize, level);
        }
    }

    public void testBufferReuseMultipleOperations() throws IOException {
        final int blockSize = randomBlockSize();
        try (ZstdCodecStage zstdStage = new ZstdCodecStage(blockSize, ZstdCodecStage.DEFAULT_COMPRESSION_LEVEL)) {
            for (int iter = 0; iter < 5; iter++) {
                final long[] original = LongStream.generate(ZstdCodecStageTests::randomLong).limit(blockSize).toArray();
                final long[] values = original.clone();
                final EncodingContext encodingContext = createEncodingContext(blockSize);

                final byte[] dataBuffer = new byte[blockSize * Long.BYTES * 2];
                final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(dataBuffer);
                zstdStage.encode(values, values.length, dataOutput, encodingContext);

                final DecodingContext decodingContext = createDecodingContext(blockSize);
                final ByteArrayDataInput dataInput = new ByteArrayDataInput(dataBuffer, 0, dataOutput.getPosition());
                final long[] decoded = new long[blockSize];

                assertEquals(blockSize, zstdStage.decode(decoded, dataInput, decodingContext));
                assertArrayEquals("Failed on iteration " + iter, original, decoded);
            }
        }
    }

    public void testBlockWithTrailingZeros() throws IOException {
        final int blockSize = randomBlockSize();
        final int numNonZeroValues = randomIntBetween(1, blockSize / 2);
        final long[] original = new long[blockSize];
        for (int i = 0; i < numNonZeroValues; i++) {
            original[i] = randomLongBetween(1, 1000);
        }
        assertRoundTrip(original, blockSize);
    }

    public void testValueCountExceedsBlockSizeThrows() throws IOException {
        final int blockSize = randomBlockSize();
        final int tooLarge = blockSize + randomIntBetween(1, 100);
        final long[] values = new long[tooLarge];
        final EncodingContext encodingContext = createEncodingContext(blockSize);

        try (ZstdCodecStage zstdStage = new ZstdCodecStage(blockSize, ZstdCodecStage.DEFAULT_COMPRESSION_LEVEL)) {
            final byte[] dataBuffer = new byte[tooLarge * Long.BYTES * 2];
            final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(dataBuffer);

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> zstdStage.encode(values, values.length, dataOutput, encodingContext)
            );
            assertTrue(e.getMessage().contains("exceeds block size"));
        }
    }

    public void testCustomBlockSizeRoundTrip() throws IOException {
        final int customBlockSize = randomBlockSize();
        final long[] original = LongStream.generate(ZstdCodecStageTests::randomLong).limit(customBlockSize).toArray();
        final long[] values = original.clone();

        final EncodingContext encodingContext = createEncodingContext(customBlockSize);

        try (ZstdCodecStage zstdStage = new ZstdCodecStage(customBlockSize, 1)) {
            assertEquals(customBlockSize, zstdStage.blockSize());

            final byte[] dataBuffer = new byte[customBlockSize * Long.BYTES * 2];
            final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(dataBuffer);
            zstdStage.encode(values, values.length, dataOutput, encodingContext);

            final DecodingContext decodingContext = createDecodingContext(customBlockSize);
            final ByteArrayDataInput dataInput = new ByteArrayDataInput(dataBuffer, 0, dataOutput.getPosition());
            final long[] decoded = new long[customBlockSize];

            assertEquals(customBlockSize, zstdStage.decode(decoded, dataInput, decodingContext));
            assertArrayEquals(original, decoded);
        }
    }

    private EncodingContext createEncodingContext(int blockSize) {
        return createEncodingContext(blockSize, StageId.ZSTD.id);
    }

    private DecodingContext createDecodingContext(int blockSize) {
        return createDecodingContext(blockSize, StageId.ZSTD.id);
    }

    private void assertRoundTrip(final long[] original, int blockSize) throws IOException {
        assertRoundTripWithLevel(original, blockSize, ZstdCodecStage.DEFAULT_COMPRESSION_LEVEL);
    }

    private void assertRoundTripWithLevel(final long[] original, int blockSize, int compressionLevel) throws IOException {
        final long[] values = original.clone();
        final EncodingContext encodingContext = createEncodingContext(blockSize);

        try (ZstdCodecStage zstdStage = new ZstdCodecStage(blockSize, compressionLevel)) {
            final byte[] dataBuffer = new byte[blockSize * Long.BYTES * 2];
            final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(dataBuffer);
            zstdStage.encode(values, original.length, dataOutput, encodingContext);

            final DecodingContext decodingContext = createDecodingContext(blockSize);
            final ByteArrayDataInput dataInput = new ByteArrayDataInput(dataBuffer, 0, dataOutput.getPosition());
            final long[] decoded = new long[original.length];

            assertEquals(blockSize, zstdStage.decode(decoded, dataInput, decodingContext));
            assertArrayEquals(original, decoded);
        }
    }
}
