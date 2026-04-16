/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * a copy and modification from Lucene util
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */

package org.elasticsearch.test.knn;

import org.apache.lucene.index.VectorEncoding;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.knn.data.PartitionDataGenerator;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.knn.KnnIndexTester.logger;

/**
 * Provide vectors for indexing. Implementations must be thread-safe.
 */
public interface IndexVectorReader extends Closeable {
    /** Returns the next float vector. Thread-safe. */
    float[] nextFloatVector(int docOrd) throws IOException;

    /** Returns the next byte vector. Thread-safe. */
    byte[] nextByteVector(int docOrd) throws IOException;

    @Override
    default void close() throws IOException {}

    /**
     * An {@link IndexVectorReader} that reads vectors from a file channel.
     */
    class FileVectorReader implements IndexVectorReader {
        private final VectorReader reader;
        private final int dim;

        FileVectorReader(VectorReader reader, int dim) {
            this.reader = reader;
            this.dim = dim;
        }

        @Override
        public float[] nextFloatVector(int docOrd) throws IOException {
            float[] dest = new float[dim];
            reader.next(dest);
            return dest;
        }

        @Override
        public byte[] nextByteVector(int docOrd) throws IOException {
            byte[] dest = new byte[dim];
            reader.next(dest);
            return dest;
        }
    }

    /**
     * An {@link IndexVectorReader} that generates vectors from a {@link PartitionDataGenerator}.
     */
    class PartitionGeneratingVectorReader implements IndexVectorReader {
        private final PartitionDataGenerator generator;

        public PartitionGeneratingVectorReader(PartitionDataGenerator generator) {
            this.generator = generator;
        }

        @Override
        public float[] nextFloatVector(int docOrd) {
            return generator.nextVector();
        }

        @Override
        public byte[] nextByteVector(int docOrd) {
            return generator.nextByteVector();
        }
    }

    /**
     * An {@link IndexVectorReader} that reads vectors sequentially across multiple files.
     * Handles dim detection from file headers, wraps around within each file, and caps at a maximum doc count.
     */
    class MultiFileVectorReader implements IndexVectorReader {
        private final List<VectorReader> readers;
        private final List<FileChannel> channels;
        private final int[] docsPerReader;
        private final int totalDocs;
        private final int dim;
        private int currentReaderIdx;
        private int docsReadFromCurrent;

        private MultiFileVectorReader(List<VectorReader> readers, List<FileChannel> channels, int[] docsPerReader, int totalDocs, int dim) {
            this.readers = readers;
            this.channels = channels;
            this.docsPerReader = docsPerReader;
            this.totalDocs = totalDocs;
            this.dim = dim;
            this.currentReaderIdx = 0;
            this.docsReadFromCurrent = 0;
        }

        public static MultiFileVectorReader create(List<Path> docPaths, int requestedDim, VectorEncoding vectorEncoding, int maxDocs)
            throws IOException {
            List<VectorReader> readers = new ArrayList<>();
            List<FileChannel> channels = new ArrayList<>();
            int[] docsPerReader = new int[docPaths.size()];
            int resolvedDim = requestedDim;
            int docsRemaining = maxDocs;
            boolean hasFvecHeader = requestedDim == -1;

            for (int f = 0; f < docPaths.size() && docsRemaining > 0; f++) {
                Path docsPath = docPaths.get(f);
                FileChannel in = FileChannel.open(docsPath);
                channels.add(in);
                long docsPathSizeInBytes = in.size();
                int offsetByteSize = hasFvecHeader ? Integer.BYTES : 0;
                int dim = resolvedDim;
                if (dim == -1) {
                    ByteBuffer preamble = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
                    int bytesRead = Channels.readFromFileChannel(in, 0, preamble);
                    if (bytesRead < Integer.BYTES) {
                        throw new IllegalArgumentException(
                            "docsPath \"" + docsPath + "\" does not contain a valid dims?  size=" + docsPathSizeInBytes
                        );
                    }
                    dim = preamble.getInt(0);
                    if (dim <= 0) {
                        throw new IllegalArgumentException("docsPath \"" + docsPath + "\" has invalid dimension: " + dim);
                    }
                    resolvedDim = dim;
                }
                if (docsPathSizeInBytes % (((long) dim * vectorEncoding.byteSize + offsetByteSize)) != 0) {
                    throw new IllegalArgumentException(
                        "docsPath \"" + docsPath + "\" does not contain a whole number of vectors?  size=" + docsPathSizeInBytes
                    );
                }
                int fileDocs = (int) (docsPathSizeInBytes / ((long) dim * vectorEncoding.byteSize + offsetByteSize));
                fileDocs = Math.min(docsRemaining, fileDocs);
                logger.info(
                    "path={}, docsPathSizeInBytes={}, numDocs={}, dim={}, vectorEncoding={}, byteSize={}",
                    docsPath,
                    docsPathSizeInBytes,
                    fileDocs,
                    dim,
                    vectorEncoding,
                    vectorEncoding.byteSize
                );
                docsPerReader[f] = fileDocs;
                docsRemaining -= fileDocs;
                readers.add(VectorReader.create(in, dim, vectorEncoding, offsetByteSize));
            }
            int totalDocs = 0;
            for (int d : docsPerReader) {
                totalDocs += d;
            }
            return new MultiFileVectorReader(readers, channels, docsPerReader, totalDocs, resolvedDim);
        }

        public int totalDocs() {
            return totalDocs;
        }

        public int dim() {
            return dim;
        }

        private VectorReader currentReader() {
            while (currentReaderIdx < readers.size() && docsReadFromCurrent >= docsPerReader[currentReaderIdx]) {
                currentReaderIdx++;
                docsReadFromCurrent = 0;
            }
            return readers.get(currentReaderIdx);
        }

        @Override
        public synchronized float[] nextFloatVector(int docOrd) throws IOException {
            VectorReader reader = currentReader();
            docsReadFromCurrent++;
            float[] dest = new float[dim];
            reader.next(dest);
            return dest;
        }

        @Override
        public synchronized byte[] nextByteVector(int docOrd) throws IOException {
            VectorReader reader = currentReader();
            docsReadFromCurrent++;
            byte[] dest = new byte[dim];
            reader.next(dest);
            return dest;
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(channels);
        }
    }

    class VectorReader {
        final float[] target;
        final int offsetByteSize;
        final ByteBuffer bytes;
        final FileChannel input;
        long position;

        public static VectorReader create(FileChannel input, int dim, VectorEncoding vectorEncoding, int offsetByteSize)
            throws IOException {
            int bufferSize = dim * vectorEncoding.byteSize;
            if (input.size() % ((long) dim * vectorEncoding.byteSize + offsetByteSize) != 0) {
                throw new IllegalArgumentException(
                    "vectors file \"" + input + "\" does not contain a whole number of vectors?  size=" + input.size()
                );
            }
            return new VectorReader(input, dim, bufferSize, offsetByteSize);
        }

        VectorReader(FileChannel input, int dim, int bufferSize, int offsetByteSize) throws IOException {
            this.offsetByteSize = offsetByteSize;
            this.bytes = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
            this.input = input;
            this.target = new float[dim];
            reset();
        }

        void reset() throws IOException {
            position = offsetByteSize;
            input.position(position);
        }

        private void readNext() throws IOException {
            int bytesRead = Channels.readFromFileChannel(this.input, position, bytes);
            if (bytesRead < bytes.capacity()) {
                position = offsetByteSize;
                bytes.position(0);
                // wrap around back to the start of the file if we hit the end:
                logger.warn("VectorReader hit EOF when reading {}; now wrapping around to start of file again", this.input);
                input.position(position);
                bytesRead = Channels.readFromFileChannel(this.input, position, bytes);
                if (bytesRead < bytes.capacity()) {
                    throw new IllegalStateException(
                        "vector file " + input + " doesn't even have enough bytes for a single vector?  got bytesRead=" + bytesRead
                    );
                }
            }
            position += bytesRead + offsetByteSize;
            bytes.position(0);
        }

        public synchronized void next(float[] dest) throws IOException {
            readNext();
            bytes.asFloatBuffer().get(dest);
        }

        public synchronized void next(byte[] dest) throws IOException {
            readNext();
            bytes.get(0, dest);
        }
    }
}
