/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn;

import org.apache.lucene.index.VectorEncoding;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class MultiFileVectorReaderTests extends ESTestCase {

    /**
     * Writes a .fvec file: each vector is prefixed with a little-endian int (the dimension),
     * followed by dim little-endian floats.
     */
    private Path writeFvecFile(String name, float[][] vectors) throws IOException {
        Path file = createTempDir().resolve(name);
        int dim = vectors[0].length;
        int bytesPerVector = Integer.BYTES + dim * Float.BYTES;
        ByteBuffer buf = ByteBuffer.allocate(vectors.length * bytesPerVector).order(ByteOrder.LITTLE_ENDIAN);
        for (float[] vec : vectors) {
            buf.putInt(dim);
            for (float v : vec) {
                buf.putFloat(v);
            }
        }
        buf.flip();
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            ch.write(buf);
        }
        return file;
    }

    /**
     * Writes a raw float vector file (no per-vector header), used when dim is known upfront.
     */
    private Path writeRawFloatFile(String name, float[][] vectors) throws IOException {
        Path file = createTempDir().resolve(name);
        int dim = vectors[0].length;
        ByteBuffer buf = ByteBuffer.allocate(vectors.length * dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (float[] vec : vectors) {
            for (float v : vec) {
                buf.putFloat(v);
            }
        }
        buf.flip();
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            ch.write(buf);
        }
        return file;
    }

    public void testSingleFvecFileWithAutoDetectedDim() throws IOException {
        float[][] vectors = { { 1.0f, 2.0f, 3.0f }, { 4.0f, 5.0f, 6.0f } };
        Path file = writeFvecFile("test.fvec", vectors);

        try (var reader = IndexVectorReader.MultiFileVectorReader.create(List.of(file), -1, VectorEncoding.FLOAT32, Integer.MAX_VALUE)) {
            assertEquals(3, reader.dim());
            assertEquals(2, reader.totalDocs());
            assertArrayEquals(vectors[0], reader.nextFloatVector(0), 0f);
            assertArrayEquals(vectors[1], reader.nextFloatVector(1), 0f);
        }
    }

    public void testMultipleFvecFilesWithAutoDetectedDim() throws IOException {
        float[][] vectors1 = { { 1.0f, 2.0f }, { 3.0f, 4.0f } };
        float[][] vectors2 = { { 5.0f, 6.0f }, { 7.0f, 8.0f }, { 9.0f, 10.0f } };
        Path file1 = writeFvecFile("part1.fvec", vectors1);
        Path file2 = writeFvecFile("part2.fvec", vectors2);

        try (
            var reader = IndexVectorReader.MultiFileVectorReader.create(
                List.of(file1, file2),
                -1,
                VectorEncoding.FLOAT32,
                Integer.MAX_VALUE
            )
        ) {
            assertEquals(2, reader.dim());
            assertEquals(5, reader.totalDocs());
            // Read all vectors sequentially across both files
            assertArrayEquals(vectors1[0], reader.nextFloatVector(0), 0f);
            assertArrayEquals(vectors1[1], reader.nextFloatVector(1), 0f);
            assertArrayEquals(vectors2[0], reader.nextFloatVector(2), 0f);
            assertArrayEquals(vectors2[1], reader.nextFloatVector(3), 0f);
            assertArrayEquals(vectors2[2], reader.nextFloatVector(4), 0f);
        }
    }

    public void testMultipleFvecFilesWithMaxDocsCap() throws IOException {
        float[][] vectors1 = { { 1.0f, 2.0f }, { 3.0f, 4.0f } };
        float[][] vectors2 = { { 5.0f, 6.0f }, { 7.0f, 8.0f } };
        Path file1 = writeFvecFile("part1.fvec", vectors1);
        Path file2 = writeFvecFile("part2.fvec", vectors2);

        try (var reader = IndexVectorReader.MultiFileVectorReader.create(List.of(file1, file2), -1, VectorEncoding.FLOAT32, 3)) {
            assertEquals(2, reader.dim());
            assertEquals(3, reader.totalDocs());
            assertArrayEquals(vectors1[0], reader.nextFloatVector(0), 0f);
            assertArrayEquals(vectors1[1], reader.nextFloatVector(1), 0f);
            assertArrayEquals(vectors2[0], reader.nextFloatVector(2), 0f);
        }
    }

    public void testRawFloatFilesWithKnownDim() throws IOException {
        float[][] vectors1 = { { 1.0f, 2.0f, 3.0f } };
        float[][] vectors2 = { { 4.0f, 5.0f, 6.0f } };
        Path file1 = writeRawFloatFile("part1.bin", vectors1);
        Path file2 = writeRawFloatFile("part2.bin", vectors2);

        try (
            var reader = IndexVectorReader.MultiFileVectorReader.create(List.of(file1, file2), 3, VectorEncoding.FLOAT32, Integer.MAX_VALUE)
        ) {
            assertEquals(3, reader.dim());
            assertEquals(2, reader.totalDocs());
            assertArrayEquals(vectors1[0], reader.nextFloatVector(0), 0f);
            assertArrayEquals(vectors2[0], reader.nextFloatVector(1), 0f);
        }
    }

}
