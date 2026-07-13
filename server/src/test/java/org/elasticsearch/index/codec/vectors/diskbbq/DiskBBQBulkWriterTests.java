/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class DiskBBQBulkWriterTests extends ESTestCase {

    private static final Integer[] VALID_BIT_SIZES = { 1, 2, 4, 7 };
    private static final Integer[] INVALID_BIT_SIZES = { 0, 3, 5, 6, 8, 16 };

    public void testLargeBitEncodingWritesIntComponentSum() throws Exception {
        assertLargeBitEncoding(7);
    }

    public void testFromBitSizeValidValues() throws IOException {
        int bits = randomFrom(VALID_BIT_SIZES);
        try (IndexOutput out = new ByteBuffersIndexOutput(new ByteBuffersDataOutput(), "test", "test")) {
            DiskBBQBulkWriter writer = DiskBBQBulkWriter.fromBitSize(bits, 32, out);
            assertNotNull(writer);
        }
    }

    public void testFromBitSizeInvalidValues() throws IOException {
        int bits = randomFrom(INVALID_BIT_SIZES);
        try (IndexOutput out = new ByteBuffersIndexOutput(new ByteBuffersDataOutput(), "test", "test")) {
            expectThrows(IllegalArgumentException.class, () -> DiskBBQBulkWriter.fromBitSize(bits, 32, out));
        }
    }

    private void assertLargeBitEncoding(int bits) throws IOException {
        int dimensions = randomIntBetween(2, 64);
        int bulkSize = randomIntBetween(2, 16);
        int numVectors = bulkSize + randomIntBetween(1, bulkSize - 1); // guarantees a bulk block + tail
        byte[][] vectors = new byte[numVectors][dimensions];
        OptimizedScalarQuantizer.QuantizationResult[] corrections = new OptimizedScalarQuantizer.QuantizationResult[numVectors];
        for (int i = 0; i < numVectors; i++) {
            random().nextBytes(vectors[i]);
            corrections[i] = new OptimizedScalarQuantizer.QuantizationResult(
                randomFloat(),
                randomFloat(),
                randomFloat(),
                randomIntBetween(0, 200_000)
            );
        }

        ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();
        try (IndexOutput out = new ByteBuffersIndexOutput(buffer, "diskbbq", "diskbbq")) {
            DiskBBQBulkWriter writer = DiskBBQBulkWriter.fromBitSize(bits, bulkSize, out);
            writer.writeVectors(new TestQuantizedVectorValues(vectors, corrections), null);
        }

        try (IndexInput in = new ByteArrayIndexInput("diskbbq", buffer.toArrayCopy())) {
            // bulk block: vectors then corrections (lower[], upper[], componentSum[], additional[])
            for (int i = 0; i < bulkSize; i++) {
                assertVectorEquals(in, vectors[i], dimensions);
            }
            for (int i = 0; i < bulkSize; i++) {
                assertEquals(corrections[i].lowerInterval(), readFloat(in), 0.0f);
            }
            for (int i = 0; i < bulkSize; i++) {
                assertEquals(corrections[i].upperInterval(), readFloat(in), 0.0f);
            }
            for (int i = 0; i < bulkSize; i++) {
                assertEquals(corrections[i].quantizedComponentSum(), in.readInt());
            }
            for (int i = 0; i < bulkSize; i++) {
                assertEquals(corrections[i].additionalCorrection(), readFloat(in), 0.0f);
            }
            // tail: each vector followed by its own correction (lower, upper, additional, componentSum)
            for (int i = bulkSize; i < numVectors; i++) {
                assertVectorEquals(in, vectors[i], dimensions);
                assertEquals(corrections[i].lowerInterval(), readFloat(in), 0.0f);
                assertEquals(corrections[i].upperInterval(), readFloat(in), 0.0f);
                assertEquals(corrections[i].additionalCorrection(), readFloat(in), 0.0f);
                assertEquals(corrections[i].quantizedComponentSum(), in.readInt());
            }
        }
    }

    private static float readFloat(IndexInput in) throws IOException {
        return Float.intBitsToFloat(in.readInt());
    }

    private static void assertVectorEquals(IndexInput in, byte[] expected, int dimensions) throws IOException {
        byte[] actual = new byte[dimensions];
        in.readBytes(actual, 0, dimensions);
        assertArrayEquals(expected, actual);
    }

    private static class TestQuantizedVectorValues implements QuantizedVectorValues {
        private final byte[][] vectors;
        private final OptimizedScalarQuantizer.QuantizationResult[] corrections;
        private int index = -1;

        TestQuantizedVectorValues(byte[][] vectors, OptimizedScalarQuantizer.QuantizationResult[] corrections) {
            this.vectors = vectors;
            this.corrections = corrections;
        }

        @Override
        public int count() {
            return vectors.length;
        }

        @Override
        public byte[] next() {
            index++;
            return vectors[index];
        }

        @Override
        public OptimizedScalarQuantizer.QuantizationResult getCorrections() {
            return corrections[index];
        }
    }
}
