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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 */
public final class BufferingTranslogWriter extends TranslogWriter {
    private byte[] buffer;
    private int bufferCount;
    private WrapperOutputStream bufferOs = new WrapperOutputStream();

    /* the total offset of this file including the bytes written to the file as well as into the buffer */
    private volatile long totalOffset;

    public BufferingTranslogWriter(ShardId shardId, long generation, ChannelReference channelReference, int bufferSize) throws IOException {
        super(shardId, generation, channelReference);
        this.buffer = new byte[bufferSize];
        this.totalOffset = writtenOffset;
    }

    @Override
    public Translog.Location add(BytesReference data) throws IOException {
        try (ReleasableLock lock = writeLock.acquire()) {
            operationCounter++;
            final long offset = totalOffset;
            if (data.length() >= buffer.length) {
                flush();
                // we use the channel to write, since on windows, writing to the RAF might not be reflected
                // when reading through the channel
                data.writeTo(channel);
                writtenOffset += data.length();
                totalOffset += data.length();
                return new Translog.Location(generation, offset, data.length());
            }
            if (data.length() > buffer.length - bufferCount) {
                flush();
            }
            data.writeTo(bufferOs);
            totalOffset += data.length();
            return new Translog.Location(generation, offset, data.length());
        }
    }

    protected final void flush() throws IOException {
        assert writeLock.isHeldByCurrentThread();
        if (bufferCount > 0) {
            // we use the channel to write, since on windows, writing to the RAF might not be reflected
            // when reading through the channel
            Channels.writeToChannel(buffer, 0, bufferCount, channel);
            writtenOffset += bufferCount;
            bufferCount = 0;
        }
    }

    @Override
    protected void readBytes(ByteBuffer targetBuffer, long position) throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            if (position >= writtenOffset) {
                assert targetBuffer.hasArray() : "buffer must have array";
                final int sourcePosition = (int) (position - writtenOffset);
                System.arraycopy(buffer, sourcePosition,
                        targetBuffer.array(), targetBuffer.position(), targetBuffer.limit());
                targetBuffer.position(targetBuffer.limit());
                return;
            }
        }
        // we don't have to have a read lock here because we only write ahead to the file, so all writes has been complete
        // for the requested location.
        Channels.readFromFileChannelWithEofException(channel, position, targetBuffer);
    }

    @Override
    public boolean syncNeeded() {
        return totalOffset != lastSyncedOffset;
    }

    @Override
    public void sync() throws IOException {
        if (!syncNeeded()) {
            return;
        }
        synchronized (this) {
            try (ReleasableLock lock = writeLock.acquire()) {
                flush();
                lastSyncedOffset = totalOffset;
            }
            // we can do this outside of the write lock but we have to protect from
            // concurrent syncs
            checkpoint(lastSyncedOffset, operationCounter, channelReference);
        }
    }


    public void updateBufferSize(int bufferSize) {
        try (ReleasableLock lock = writeLock.acquire()) {
            if (this.buffer.length != bufferSize) {
                flush();
                this.buffer = new byte[bufferSize];
            }
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

    @Override
    public long sizeInBytes() {
        return totalOffset;
    }
}
