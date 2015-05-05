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
import org.elasticsearch.common.bytes.BytesArray;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A base class for all classes that allows reading ops from translog files
 */
public abstract class ChannelReader implements Closeable, Comparable<ChannelReader> {

    public static final int UNKNOWN_OP_COUNT = -1;

    protected final long id;
    protected final ChannelReference channelReference;
    protected final FileChannel channel;
    protected final AtomicBoolean closed = new AtomicBoolean(false);

    public ChannelReader(long id, ChannelReference channelReference) {
        this.id = id;
        this.channelReference = channelReference;
        this.channel = channelReference.channel();
    }

    public long translogId() {
        return this.id;
    }

    abstract public long sizeInBytes();

    /** the position the first operation is written at */
    public long firstPosition() {
        return channelReference.stream().headerLength();
    }

    abstract public int totalOperations();

    public Translog.Operation read(Translog.Location location) throws IOException {
        assert location.translogId == id : "read location's translog id [" + location.translogId + "] is not [" + id + "]";
        ByteBuffer buffer = ByteBuffer.allocate(location.size);
        return read(buffer, location.translogLocation, location.size);
    }

    /** read the size of the op (i.e., number of bytes, including the op size) written at the given position */
    public int readSize(ByteBuffer reusableBuffer, long position) {
        // read op size from disk
        assert reusableBuffer.capacity() >= 4 : "reusable buffer must have capacity >=4 when reading opSize. got [" + reusableBuffer.capacity() + "]";
        try {
            reusableBuffer.clear();
            reusableBuffer.limit(4);
            readBytes(reusableBuffer, position);
            reusableBuffer.flip();
            // Add an extra 4 to account for the operation size integer itself
            return reusableBuffer.getInt() + 4;
        } catch (IOException e) {
            throw new ElasticsearchException("unexpected exception reading from translog snapshot of " + this.channelReference.file(), e);
        }
    }

    /**
     * reads an operation at the given position and returns it. The buffer length is equal to the number
     * of bytes reads.
     */
    public Translog.Operation read(ByteBuffer reusableBuffer, long position, int opSize) throws IOException {
        final ByteBuffer buffer;
        if (reusableBuffer.capacity() >= opSize) {
            buffer = reusableBuffer;
        } else {
            buffer = ByteBuffer.allocate(opSize);
        }
        buffer.clear();
        buffer.limit(opSize);
        readBytes(buffer, position);
        BytesArray bytesArray = new BytesArray(buffer.array(), 0, buffer.limit());
        return channelReference.stream().read(bytesArray.streamInput());
    }

    /**
     * reads bytes at position into the given buffer, filling it.
     */
    abstract protected void readBytes(ByteBuffer buffer, long position) throws IOException;

    /** create snapshot for this channel */
    abstract public ChannelSnapshot newSnapshot();

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            doClose();
        }
    }

    protected void doClose() throws IOException {
        channelReference.decRef();
    }

    @Override
    public String toString() {
        return "translog [" + id + "][" + channelReference.file() + "]";
    }

    @Override
    public int compareTo(ChannelReader o) {
        return Long.compare(translogId(), o.translogId());
    }
}
