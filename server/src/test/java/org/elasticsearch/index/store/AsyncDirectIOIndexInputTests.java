/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class AsyncDirectIOIndexInputTests extends ESTestCase {

    @SuppressForbidden(reason = "requires Files.getFileStore")
    private static int getBlockSize(Path path) throws IOException {
        return Math.toIntExact(Files.getFileStore(path).getBlockSize());
    }

    private static final int BASE_BUFFER_SIZE = 8192;

    public void testPrefetchEdgeCase() throws IOException {
        byte[] bytes = new byte[BASE_BUFFER_SIZE * 32 + randomIntBetween(1, BASE_BUFFER_SIZE)];
        int offset = 84;
        float[] vectorActual = new float[768];
        int[] toSeek = new int[] { 1, 2, 3, 5, 6, 9, 11, 14, 15, 16, 18, 23, 24, 25, 26, 29, 30, 31 };
        int byteSize = vectorActual.length * Float.BYTES;
        Path path = createTempDir("testDirectIODirectory");
        int blockSize = getBlockSize(path);
        random().nextBytes(bytes);
        try (Directory dir = new NIOFSDirectory(path)) {
            try (var output = dir.createOutput("test", org.apache.lucene.store.IOContext.DEFAULT)) {
                output.writeBytes(bytes, bytes.length);
            }
            try (
                AsyncDirectIOIndexInput actualInput = new AsyncDirectIOIndexInput(
                    path.resolve("test"),
                    blockSize,
                    BASE_BUFFER_SIZE,
                    toSeek.length + 1
                );
            ) {
                IndexInput actualSlice = actualInput.slice("vectors", offset, bytes.length - offset);
                for (int seek : toSeek) {
                    actualSlice.prefetch((long) seek * byteSize, byteSize);
                }
                for (int seek : toSeek) {
                    actualSlice.seek((long) seek * byteSize);
                    actualSlice.readFloats(vectorActual, 0, vectorActual.length);
                    assertEquals("mismatch at seek: " + seek, (seek + 1) * byteSize, actualSlice.getFilePointer());
                }
            }
        }
    }

    public void testLargePrefetch() throws IOException {
        byte[] bytes = new byte[BASE_BUFFER_SIZE * 10 + randomIntBetween(1, BASE_BUFFER_SIZE)];
        int offset = randomIntBetween(1, BASE_BUFFER_SIZE);
        int numBytes = randomIntBetween(BASE_BUFFER_SIZE + 1, BASE_BUFFER_SIZE * 8);
        random().nextBytes(bytes);
        byte[] trueBytes = new byte[numBytes];
        System.arraycopy(bytes, offset, trueBytes, 0, numBytes);
        Path path = createTempDir("testDirectIODirectory");
        int blockSize = getBlockSize(path);
        try (Directory dir = new NIOFSDirectory(path)) {
            try (var output = dir.createOutput("test", org.apache.lucene.store.IOContext.DEFAULT)) {
                output.writeBytes(bytes, bytes.length);
            }
            try (
                AsyncDirectIOIndexInput actualInput = new AsyncDirectIOIndexInput(
                    path.resolve("test"),
                    blockSize,
                    blockSize,
                    randomIntBetween(2, 16)
                );
            ) {
                byte[] actualBytes = new byte[numBytes];
                // prefetch everything at once
                actualInput.prefetch(offset, numBytes);
                actualInput.seek(offset);
                actualInput.readBytes(actualBytes, 0, actualBytes.length);
                assertArrayEquals(trueBytes, actualBytes);
            }
        }
    }

    public void testWriteThenReadBytesConsistency() throws IOException {
        byte[] bytes = new byte[BASE_BUFFER_SIZE * 8 + randomIntBetween(1, BASE_BUFFER_SIZE)];
        random().nextBytes(bytes);
        Path path = createTempDir("testDirectIODirectory");
        int blockSize = getBlockSize(path);
        int bufferSize = 1024 * 4;
        List<Integer> seeks = new ArrayList<>();
        int lastSeek = 0;
        seeks.add(0);
        while (lastSeek < bytes.length) {
            int nextSeek = randomIntBetween(lastSeek, Math.min(lastSeek + bufferSize, bytes.length - 1));
            seeks.add(nextSeek);
            lastSeek = nextSeek + 1;
        }
        try (Directory dir = new NIOFSDirectory(path)) {
            try (var output = dir.createOutput("test", org.apache.lucene.store.IOContext.DEFAULT)) {
                output.writeBytes(bytes, bytes.length);
            }
            try (
                AsyncDirectIOIndexInput actualInput = new AsyncDirectIOIndexInput(
                    path.resolve("test"),
                    blockSize,
                    bufferSize,
                    seeks.size()
                );
                IndexInput expectedInput = dir.openInput("test", org.apache.lucene.store.IOContext.DEFAULT)
            ) {
                assert expectedInput instanceof AsyncDirectIOIndexInput == false;
                byte[] actualBytes = new byte[bufferSize / 2];
                byte[] expectedBytes = new byte[bufferSize / 2];
                int prevSeek = 0;
                for (int j = 1; j < seeks.size(); j++) {
                    actualInput.seek(prevSeek);
                    expectedInput.seek(prevSeek);
                    int seek = seeks.get(j);
                    int toRead = Math.min(actualBytes.length, bytes.length - prevSeek);
                    expectedInput.readBytes(expectedBytes, 0, toRead);
                    actualInput.readBytes(actualBytes, 0, toRead);
                    prevSeek = seek;
                    assertArrayEquals(expectedBytes, actualBytes);
                }
            }

            try (
                AsyncDirectIOIndexInput actualPretchingInput = new AsyncDirectIOIndexInput(
                    path.resolve("test"),
                    blockSize,
                    bufferSize,
                    seeks.size()
                );
                IndexInput expectedInput = dir.openInput("test", org.apache.lucene.store.IOContext.DEFAULT)
            ) {
                assert expectedInput instanceof AsyncDirectIOIndexInput == false;
                byte[] actualBytes = new byte[bufferSize / 2];
                byte[] expectedBytes = new byte[bufferSize / 2];
                for (int seek : seeks) {
                    // always prefetch just a page
                    actualPretchingInput.prefetch(seek, 1);
                }
                int prevSeek = 0;
                for (int j = 1; j < seeks.size(); j++) {
                    actualPretchingInput.seek(prevSeek);
                    expectedInput.seek(prevSeek);
                    int seek = seeks.get(j);
                    int toRead = Math.min(actualBytes.length, bytes.length - prevSeek);
                    actualPretchingInput.readBytes(actualBytes, 0, toRead);
                    expectedInput.readBytes(expectedBytes, 0, toRead);
                    prevSeek = seek;
                    assertArrayEquals(expectedBytes, actualBytes);
                }
            }
        }
    }

}
