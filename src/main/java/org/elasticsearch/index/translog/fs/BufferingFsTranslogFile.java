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

package org.elasticsearch.index.translog.fs;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogException;
import org.elasticsearch.index.translog.TranslogStream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 */
public final class BufferingFsTranslogFile extends FsTranslogFile {

    private volatile int operationCounter;
    private volatile long lastPosition;
    private volatile long lastWrittenPosition;

    private volatile long lastSyncPosition = 0;

    private byte[] buffer;
    private int bufferCount;
    private WrapperOutputStream bufferOs = new WrapperOutputStream();

    public BufferingFsTranslogFile(ShardId shardId, long id, ChannelReference channelReference, int bufferSize) throws IOException {
        super(shardId, id, channelReference);
        this.buffer = new byte[bufferSize];
        final TranslogStream stream = this.channelReference.stream();
        int headerSize = stream.writeHeader(channelReference.channel());
        this.lastPosition += headerSize;
        this.lastWrittenPosition += headerSize;
        this.lastSyncPosition += headerSize;
    }

    @Override
    public int totalOperations() {
        return operationCounter;
    }

    @Override
    public long sizeInBytes() {
        return lastWrittenPosition;
    }

    @Override
    public Translog.Location add(BytesReference data) throws IOException {
        try (ReleasableLock lock = writeLock.acquire()) {
            operationCounter++;
            long position = lastPosition;
            if (data.length() >= buffer.length) {
                flushBuffer();
                // we use the channel to write, since on windows, writing to the RAF might not be reflected
                // when reading through the channel
                data.writeTo(channelReference.channel());
                lastWrittenPosition += data.length();
                lastPosition += data.length();
                return new Translog.Location(id, position, data.length());
            }
            if (data.length() > buffer.length - bufferCount) {
                flushBuffer();
            }
            data.writeTo(bufferOs);
            lastPosition += data.length();
            return new Translog.Location(id, position, data.length());
        }
    }

    private void flushBuffer() throws IOException {
        assert writeLock.isHeldByCurrentThread();
        if (bufferCount > 0) {
            // we use the channel to write, since on windows, writing to the RAF might not be reflected
            // when reading through the channel
            Channels.writeToChannel(buffer, 0, bufferCount, channelReference.channel());
            lastWrittenPosition += bufferCount;
            bufferCount = 0;
        }
    }

    @Override
    protected void readBytes(ByteBuffer targetBuffer, long position) throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            if (position >= lastWrittenPosition) {
                System.arraycopy(buffer, (int) (position - lastWrittenPosition),
                        targetBuffer.array(), targetBuffer.position(), targetBuffer.limit());
                return;
            }
        }
        // we don't have to have a read lock here because we only write ahead to the file, so all writes has been complete
        // for the requested location.
        Channels.readFromFileChannelWithEofException(channelReference.channel(), position, targetBuffer);
    }

    public FsChannelImmutableReader immutableReader() throws TranslogException {
        if (channelReference.tryIncRef()) {
            try (ReleasableLock lock = writeLock.acquire()) {
                flushBuffer();
                FsChannelImmutableReader reader = new FsChannelImmutableReader(this.id, channelReference, lastWrittenPosition, operationCounter);
                channelReference.incRef(); // for new reader
                return reader;
            } catch (Exception e) {
                throw new TranslogException(shardId, "exception while creating an immutable reader", e);
            } finally {
                channelReference.decRef();
            }
        } else {
            throw new TranslogException(shardId, "can't increment channel [" + channelReference + "] ref count");
        }
    }

    @Override
    public boolean syncNeeded() {
        return lastPosition != lastSyncPosition;
    }

    @Override
    public void sync() throws IOException {
        if (!syncNeeded()) {
            return;
        }
        try (ReleasableLock lock = writeLock.acquire()) {
            flushBuffer();
            lastSyncPosition = lastPosition;
        }
        channelReference.channel().force(false);
    }

    @Override
    protected void doClose() throws IOException {
        try {
            sync();
        } finally {
            super.doClose();
        }
    }

    @Override
    public void reuse(FsTranslogFile other) {
        if (!(other instanceof BufferingFsTranslogFile)) {
            return;
        }
        try (ReleasableLock lock = writeLock.acquire()) {
            try {
                flushBuffer();
                this.buffer = ((BufferingFsTranslogFile) other).buffer;
            } catch (IOException e) {
                throw new TranslogException(shardId, "failed to flush", e);
            }
        }
    }

    public void updateBufferSize(int bufferSize) {
        try (ReleasableLock lock = writeLock.acquire()) {
            if (this.buffer.length == bufferSize) {
                return;
            }
            flushBuffer();
            this.buffer = new byte[bufferSize];
        } catch (IOException e) {
            throw new TranslogException(shardId, "failed to flush", e);
        }
    }

    class WrapperOutputStream extends OutputStream {

        @Override
        public void write(int b) throws IOException {
            buffer[bufferCount++] = (byte) b;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            // we do safety checked when we decide to use this stream...
            System.arraycopy(b, off, buffer, bufferCount, len);
            bufferCount += len;
        }
    }

}
