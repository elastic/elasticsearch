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
import java.util.Locale;

public class TranslogSnapshotReader extends BaseTranslogReader {

    private final int totalOperations;

    @Override
    public int totalOperations() {
        return totalOperations;
    }

    private final Checkpoint checkpoint;
    protected final long length;

    private final ByteBuffer reusableBuffer;
    private long position;

    public long getPosition() {
        return position;
    }

    private int readOperations;

    public int getReadOperations() {
        return readOperations;
    }

    private BufferedChecksumStreamInput reuse;

    TranslogSnapshotReader(final BaseTranslogReader reader, final long length) {
        super(reader.generation, reader.channel, reader.path, reader.firstOperationOffset);
        this.length = length;
        this.totalOperations = reader.totalOperations();
        this.checkpoint = reader.getCheckpoint();
        this.reusableBuffer = ByteBuffer.allocate(1024);
        readOperations = 0;
        position = firstOperationOffset;
        reuse = null;
    }

    protected Translog.OperationWithPosition readOperation() throws IOException {
        return readOperationWithPosition(position);
    }

    protected Translog.Operation readOperation(final long readPosition) throws IOException {
        return readOperationWithPosition(readPosition).operation();
    }

    private Translog.OperationWithPosition readOperationWithPosition(final long readPosition) throws IOException {
        final int size = readSize(reusableBuffer, readPosition);
        reuse = checksumStream(reusableBuffer, readPosition, size, reuse);
        final Translog.Operation op = read(reuse);
        position += size;
        readOperations++;
        return new Translog.OperationWithPosition(op, position);
    }

    @Override
    public long sizeInBytes() {
        return length;
    }

    @Override
    Checkpoint getCheckpoint() {
        return checkpoint;
    }

    /**
     * reads an operation at the given position into the given buffer.
     */
    @Override
    protected void readBytes(ByteBuffer buffer, long position) throws IOException {
        if (position >= length) {
            final String message = String.format(
                    Locale.ROOT,
                    "read requested at [%d] past EOF [%d] of generation [%d] at [%s]",
                    position,
                    length,
                    getGeneration(),
                    path);
            throw new EOFException(message);
        }
        if (position < getFirstOperationOffset()) {
            final String message = String.format(
                    Locale.ROOT,
                    "read requested at [%d] before position [%d] of first operation of generation [%d] at [%s]",
                    position,
                    getFirstOperationOffset(),
                    getGeneration(),
                    path);
            throw new IOException(message);
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
