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
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class AsynchronousDirectIOTests extends ESTestCase {

    public void testWriteThenReadBytes() throws IOException {
        byte[] bytes = new byte[8192 * 16 + 1];
        random().nextBytes(bytes);
        Path path = createTempDir("testDirectIODirectory");
        try (Directory dir = new NIOFSDirectory(path)) {
            try (var output = dir.createOutput("test", org.apache.lucene.store.IOContext.DEFAULT)) {
                output.writeBytes(bytes, bytes.length);
            }
        }
        int blockSize = Math.toIntExact(Files.getFileStore(path).getBlockSize());
        try (AsyncDirectIOIndexInput input = new AsyncDirectIOIndexInput(path.resolve("test"), blockSize, 1024 * 4, 4)) {
            int chunkSize = randomIntBetween(1, 8192);
            byte[] readBytes = new byte[bytes.length];
            int read = 0;
            int prefetched = 0;
            while (prefetched < bytes.length) {
                int toPrefetch = Math.min(chunkSize, bytes.length - prefetched);
                input.prefetch(prefetched, toPrefetch);
                prefetched += toPrefetch;
            }
            while (read < bytes.length) {
                int toRead = Math.min(chunkSize, bytes.length - read);
                input.readBytes(readBytes, read, toRead);
                read += toRead;
            }
            assertArrayEquals(bytes, readBytes);
        }
    }

}
