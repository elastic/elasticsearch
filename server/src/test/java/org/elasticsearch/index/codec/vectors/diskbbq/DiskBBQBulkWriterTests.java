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

    public void testLargeBitEncodingWritesIntComponentSum() throws Exception {
        assertLargeBitEncoding(7);
    }

    private void assertLargeBitEncoding(int bits) throws IOException {
        int dimensions = 4;
        int bulkSize = 2;
        byte[][] vectors = buildVectors(bits);
        OptimizedScalarQuantizer.QuantizationResult[] corrections = new OptimizedScalarQuantizer.QuantizationResult[] {
            new OptimizedScalarQuantizer.QuantizationResult(0.25f, 1.25f, 2.5f, 70000),
            new OptimizedScalarQuantizer.QuantizationResult(0.5f, 1.5f, 3.5f, 70001),
            new OptimizedScalarQuantizer.QuantizationResult(0.75f, 1.75f, 4.5f, 70002) };

        ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();
        try (IndexOutput out = new ByteBuffersIndexOutput(buffer, "diskbbq", "diskbbq")) {
            DiskBBQBulkWriter writer = DiskBBQBulkWriter.fromBitSize(bits, bulkSize, out);
            writer.writeVectors(new TestQuantizedVectorValues(vectors, corrections), null);
        }

        try (IndexInput in = new ByteArrayIndexInput("diskbbq", buffer.toArrayCopy())) {
            // bulk vectors
            assertVectorEquals(in, vectors[0], dimensions);
            assertVectorEquals(in, vectors[1], dimensions);
            // bulk corrections: lower, upper, component sum, additional
            assertEquals(corrections[0].lowerInterval(), readFloat(in), 0.0f);
            assertEquals(corrections[1].lowerInterval(), readFloat(in), 0.0f);
            assertEquals(corrections[0].upperInterval(), readFloat(in), 0.0f);
            assertEquals(corrections[1].upperInterval(), readFloat(in), 0.0f);
            assertEquals(corrections[0].quantizedComponentSum(), in.readInt());
            assertEquals(corrections[1].quantizedComponentSum(), in.readInt());
            assertEquals(corrections[0].additionalCorrection(), readFloat(in), 0.0f);
            assertEquals(corrections[1].additionalCorrection(), readFloat(in), 0.0f);
            // tail vector
            assertVectorEquals(in, vectors[2], dimensions);
            assertEquals(corrections[2].lowerInterval(), readFloat(in), 0.0f);
            assertEquals(corrections[2].upperInterval(), readFloat(in), 0.0f);
            assertEquals(corrections[2].additionalCorrection(), readFloat(in), 0.0f);
            assertEquals(corrections[2].quantizedComponentSum(), in.readInt());
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

    private static byte[][] buildVectors(int bits) {
        return new byte[][] { new byte[] { 10, 20, 30, 40 }, new byte[] { 50, 60, 70, 80 }, new byte[] { 90, 100, 110, 120 } };
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
