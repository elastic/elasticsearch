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
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.TranslogStream;
import org.elasticsearch.index.translog.TranslogStreams;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 */
public class BufferingFsTranslogFile implements FsTranslogFile {

    private final long id;
    private final ShardId shardId;
    private final ChannelReference channelReference;
    private final TranslogStream translogStream;
    private final int headerSize;

    private final ReadWriteLock rwl = new ReentrantReadWriteLock();
    private final AtomicBoolean closed = new AtomicBoolean();

    private volatile int operationCounter;

    private volatile long lastPosition;
    private volatile long lastWrittenPosition;

    private volatile long lastSyncPosition = 0;

    private byte[] buffer;
    private int bufferCount;
    private WrapperOutputStream bufferOs = new WrapperOutputStream();

    public BufferingFsTranslogFile(ShardId shardId, long id, ChannelReference channelReference, int bufferSize) throws IOException {
        this.shardId = shardId;
        this.id = id;
        this.channelReference = channelReference;
        this.buffer = new byte[bufferSize];
        this.translogStream = TranslogStreams.translogStreamFor(this.channelReference.file());
        this.headerSize = this.translogStream.writeHeader(channelReference.channel());
        this.lastPosition += headerSize;
        this.lastWrittenPosition += headerSize;
        this.lastSyncPosition += headerSize;
    }

    @Override
    public long id() {
        return this.id;
    }

    @Override
    public int estimatedNumberOfOperations() {
        return operationCounter;
    }

    @Override
    public long translogSizeInBytes() {
        return lastWrittenPosition;
    }

    @Override
    public Translog.Location add(BytesReference data) throws IOException {
        rwl.writeLock().lock();
        try {
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
        } finally {
            rwl.writeLock().unlock();
        }
    }

    private void flushBuffer() throws IOException {
        assert (((ReentrantReadWriteLock.WriteLock) rwl.writeLock()).isHeldByCurrentThread());
        if (bufferCount > 0) {
            // we use the channel to write, since on windows, writing to the RAF might not be reflected
            // when reading through the channel
            Channels.writeToChannel(buffer, 0, bufferCount, channelReference.channel());

            lastWrittenPosition += bufferCount;
            bufferCount = 0;
        }
    }

    @Override
    public byte[] read(Translog.Location location) throws IOException {
        rwl.readLock().lock();
        try {
            if (location.translogLocation >= lastWrittenPosition) {
                byte[] data = new byte[location.size];
                System.arraycopy(buffer, (int) (location.translogLocation - lastWrittenPosition), data, 0, location.size);
                return data;
            }
        } finally {
            rwl.readLock().unlock();
        }
        // we don't have to have a read lock here because we only write ahead to the file, so all writes has been complete
        // for the requested location.
        return Channels.readFromFileChannel(channelReference.channel(), location.translogLocation, location.size);
    }

    @Override
    public FsChannelSnapshot snapshot() throws TranslogException {
        if (channelReference.tryIncRef()) {
            boolean success = false;
            try {
                rwl.writeLock().lock();
                try {
                    flushBuffer();
                    FsChannelSnapshot snapshot = new FsChannelSnapshot(this.id, channelReference, lastWrittenPosition, operationCounter);
                    snapshot.seekTo(this.headerSize);
                    success = true;
                    return snapshot;
                } catch (Exception e) {
                    throw new TranslogException(shardId, "exception while creating snapshot", e);
                } finally {
                    rwl.writeLock().unlock();
                }
            } finally {
                if (!success) {
                    channelReference.decRef();
                }
            }
        }
        return null;
    }

    @Override
    public boolean syncNeeded() {
        return lastPosition != lastSyncPosition;
    }

    @Override
    public TranslogStream getStream() {
        return this.translogStream;
    }

    @Override
    public void sync() throws IOException {
        if (!syncNeeded()) {
            return;
        }
        rwl.writeLock().lock();
        try {
            flushBuffer();
            lastSyncPosition = lastPosition;
        } finally {
            rwl.writeLock().unlock();
        }
        channelReference.channel().force(false);
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                sync();
            } finally {
                channelReference.decRef();
            }
        }
    }

    @Override
    public void reuse(FsTranslogFile other) {
        if (!(other instanceof BufferingFsTranslogFile)) {
            return;
        }
        rwl.writeLock().lock();
        try {
            flushBuffer();
            this.buffer = ((BufferingFsTranslogFile) other).buffer;
        } catch (IOException e) {
            throw new TranslogException(shardId, "failed to flush", e);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override
    public void updateBufferSize(int bufferSize) {
        rwl.writeLock().lock();
        try {
            if (this.buffer.length == bufferSize) {
                return;
            }
            flushBuffer();
            this.buffer = new byte[bufferSize];
        } catch (IOException e) {
            throw new TranslogException(shardId, "failed to flush", e);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override
    public Path getPath() {
        return channelReference.file();
    }

    @Override
    public boolean closed() {
        return this.closed.get();
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
