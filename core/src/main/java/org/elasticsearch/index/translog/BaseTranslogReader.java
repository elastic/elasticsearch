/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * A base class for all classes that allows reading ops from translog files
 */
public abstract class BaseTranslogReader implements Comparable<BaseTranslogReader> {

    protected final long generation;
    protected final FileChannel channel;
    protected final Path path;
    protected final long firstOperationOffset;

    public BaseTranslogReader(long generation, FileChannel channel, Path path, long firstOperationOffset) {
        assert Translog.parseIdFromFileName(path) == generation : "generation mismatch. Path: " + Translog.parseIdFromFileName(path) + " but generation: " + generation;

        this.generation = generation;
        this.path = path;
        this.channel = channel;
        this.firstOperationOffset = firstOperationOffset;
    }

    public long getGeneration() {
        return this.generation;
    }

    public abstract long sizeInBytes();

    abstract public int totalOperations();

    public final long getFirstOperationOffset() {
        return firstOperationOffset;
    }

    public Translog.Operation read(Translog.Location location) throws IOException {
        assert location.generation == generation : "read location's translog generation [" + location.generation + "] is not [" + generation + "]";
        ByteBuffer buffer = ByteBuffer.allocate(location.size);
        try (BufferedChecksumStreamInput checksumStreamInput = checksummedStream(buffer, location.translogLocation, location.size, null)) {
            return read(checksumStreamInput);
        }
    }

    /** read the size of the op (i.e., number of bytes, including the op size) written at the given position */
    protected final int readSize(ByteBuffer reusableBuffer, long position) {
        // read op size from disk
        assert reusableBuffer.capacity() >= 4 : "reusable buffer must have capacity >=4 when reading opSize. got [" + reusableBuffer.capacity() + "]";
        try {
            reusableBuffer.clear();
            reusableBuffer.limit(4);
            readBytes(reusableBuffer, position);
            reusableBuffer.flip();
            // Add an extra 4 to account for the operation size integer itself
            final int size = reusableBuffer.getInt() + 4;
            final long maxSize = sizeInBytes() - position;
            if (size < 0 || size > maxSize) {
                throw new TranslogCorruptedException("operation size is corrupted must be [0.." + maxSize + "] but was: " + size);
            }

            return size;
        } catch (IOException e) {
            throw new ElasticsearchException("unexpected exception reading from translog snapshot of " + this.path, e);
        }
    }

    public Translog.Snapshot newSnapshot() {
        return new TranslogSnapshot(generation, channel, path, firstOperationOffset, sizeInBytes(), totalOperations());
    }

    /**
     * reads an operation at the given position and returns it. The buffer length is equal to the number
     * of bytes reads.
     */
    protected final BufferedChecksumStreamInput checksummedStream(ByteBuffer reusableBuffer, long position, int opSize, BufferedChecksumStreamInput reuse) throws IOException {
        final ByteBuffer buffer;
        if (reusableBuffer.capacity() >= opSize) {
            buffer = reusableBuffer;
        } else {
            buffer = ByteBuffer.allocate(opSize);
        }
        buffer.clear();
        buffer.limit(opSize);
        readBytes(buffer, position);
        buffer.flip();
        return new BufferedChecksumStreamInput(new ByteBufferStreamInput(buffer), reuse);
    }

    protected Translog.Operation read(BufferedChecksumStreamInput inStream) throws IOException {
        return Translog.readOperation(inStream);
    }

    /**
     * reads bytes at position into the given buffer, filling it.
     */
    abstract protected void readBytes(ByteBuffer buffer, long position) throws IOException;

    @Override
    public String toString() {
        return "translog [" + generation + "][" + path + "]";
    }

    @Override
    public int compareTo(BaseTranslogReader o) {
        return Long.compare(getGeneration(), o.getGeneration());
    }


    public Path path() {
        return path;
    }
}
