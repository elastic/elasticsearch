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

import org.elasticsearch.common.io.Channels;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

final class TranslogSnapshot extends BaseTranslogReader {

    private final int totalOperations;
    private final Checkpoint checkpoint;
    protected final long length;

    private final ByteBuffer reusableBuffer;
    private long position;
    private int readOperations;
    private BufferedChecksumStreamInput reuse;

    /**
     * Create a snapshot of translog file channel.
     */
    TranslogSnapshot(final BaseTranslogReader reader, final long length) {
        super(reader.generation, reader.channel, reader.path, reader.firstOperationOffset);
        this.length = length;
        this.totalOperations = reader.totalOperations();
        this.checkpoint = reader.getCheckpoint();
        this.reusableBuffer = ByteBuffer.allocate(1024);
        readOperations = 0;
        position = firstOperationOffset;
        reuse = null;
    }

    @Override
    public int totalOperations() {
        return totalOperations;
    }

    @Override
    Checkpoint getCheckpoint() {
        return checkpoint;
    }

    public Translog.Operation next() throws IOException {
        if (readOperations < totalOperations) {
            return readOperation();
        } else {
            return null;
        }
    }

    protected Translog.Operation readOperation() throws IOException {
        final int opSize = readSize(reusableBuffer, position);
        reuse = checksummedStream(reusableBuffer, position, opSize, reuse);
        Translog.Operation op = read(reuse);
        position += opSize;
        readOperations++;
        return op;
    }

    public long sizeInBytes() {
        return length;
    }

    /**
     * reads an operation at the given position into the given buffer.
     */
    protected void readBytes(ByteBuffer buffer, long position) throws IOException {
        if (position >= length) {
            throw new EOFException("read requested past EOF. pos [" + position + "] end: [" + length + "], generation: [" + getGeneration() + "], path: [" + path + "]");
        }
        if (position < getFirstOperationOffset()) {
            throw new IOException("read requested before position of first ops. pos [" + position + "] first op on: [" + getFirstOperationOffset() + "], generation: [" + getGeneration() + "], path: [" + path + "]");
        }
        Channels.readFromFileChannelWithEofException(channel, position, buffer);
    }

    @Override
    public String toString() {
        return "TranslogSnapshot{" +
                "readOperations=" + readOperations +
                ", position=" + position +
                ", estimateTotalOperations=" + totalOperations +
                ", length=" + length +
                ", generation=" + generation +
                ", reusableBuffer=" + reusableBuffer +
                '}';
    }

}
