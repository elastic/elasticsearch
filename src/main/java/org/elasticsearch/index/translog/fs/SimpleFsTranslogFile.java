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

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.TranslogStream;
import org.elasticsearch.index.translog.TranslogStreams;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SimpleFsTranslogFile implements FsTranslogFile {

    private final long id;
    private final ShardId shardId;
    private final ChannelReference channelReference;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ReadWriteLock rwl = new ReentrantReadWriteLock();
    private final TranslogStream translogStream;
    private final int headerSize;

    private volatile int operationCounter = 0;

    private volatile long lastPosition = 0;
    private volatile long lastWrittenPosition = 0;

    private volatile long lastSyncPosition = 0;

    public SimpleFsTranslogFile(ShardId shardId, long id, ChannelReference channelReference) throws IOException {
        this.shardId = shardId;
        this.id = id;
        this.channelReference = channelReference;
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
            long position = lastPosition;
            data.writeTo(channelReference.channel());
            lastPosition = lastPosition + data.length();
            lastWrittenPosition = lastWrittenPosition + data.length();
            operationCounter = operationCounter + 1;
            return new Translog.Location(id, position, data.length());
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override
    public byte[] read(Translog.Location location) throws IOException {
        rwl.readLock().lock();
        try {
            return Channels.readFromFileChannel(channelReference.channel(), location.translogLocation, location.size);
        } finally {
            rwl.readLock().unlock();
        }
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

    /**
     * Returns a snapshot on this file, <tt>null</tt> if it failed to snapshot.
     */
    @Override
    public FsChannelSnapshot snapshot() throws TranslogException {
        if (channelReference.tryIncRef()) {
            boolean success = false;
            try {
                rwl.writeLock().lock();
                try {
                    FsChannelSnapshot snapshot = new FsChannelSnapshot(this.id, channelReference, lastWrittenPosition, operationCounter);
                    snapshot.seekTo(this.headerSize);
                    success = true;
                    return snapshot;
                } finally {
                    rwl.writeLock().unlock();
                }
            } catch (FileNotFoundException e) {
                throw new TranslogException(shardId, "failed to create snapshot", e);
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
        return lastWrittenPosition != lastSyncPosition;
    }

    @Override
    public TranslogStream getStream() {
        return this.translogStream;
    }

    @Override
    public Path getPath() {
        return channelReference.file();
    }

    @Override
    public void sync() throws IOException {
        // check if we really need to sync here...
        if (!syncNeeded()) {
            return;
        }
        rwl.writeLock().lock();
        try {
            lastSyncPosition = lastWrittenPosition;
            channelReference.channel().force(false);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override
    public void reuse(FsTranslogFile other) {
        // nothing to do there
    }

    @Override
    public void updateBufferSize(int bufferSize) throws TranslogException {
        // nothing to do here...
    }

    @Override
    public boolean closed() {
        return this.closed.get();
    }

}
