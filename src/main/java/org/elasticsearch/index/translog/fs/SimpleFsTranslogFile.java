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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SimpleFsTranslogFile implements FsTranslogFile {

    private final long id;
    private final ShardId shardId;
    private final RafReference raf;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ReadWriteLock rwl = new ReentrantReadWriteLock();
    private final TranslogStream translogStream;
    private final int headerSize;

    private volatile int operationCounter = 0;

    private volatile long lastPosition = 0;
    private volatile long lastWrittenPosition = 0;

    private volatile long lastSyncPosition = 0;

    public SimpleFsTranslogFile(ShardId shardId, long id, RafReference raf) throws IOException {
        this.shardId = shardId;
        this.id = id;
        this.raf = raf;
        raf.raf().setLength(0);
        this.translogStream = TranslogStreams.translogStreamFor(this.raf.file());
        this.headerSize = this.translogStream.writeHeader(raf.channel());
        this.lastPosition += headerSize;
        this.lastWrittenPosition += headerSize;
        this.lastSyncPosition += headerSize;
    }

    public long id() {
        return this.id;
    }

    public int estimatedNumberOfOperations() {
        return operationCounter;
    }

    public long translogSizeInBytes() {
        return lastWrittenPosition;
    }

    public Translog.Location add(BytesReference data) throws IOException {
        rwl.writeLock().lock();
        try {
            long position = lastPosition;
            data.writeTo(raf.channel());
            lastPosition = lastPosition + data.length();
            lastWrittenPosition = lastWrittenPosition + data.length();
            operationCounter = operationCounter + 1;
            return new Translog.Location(id, position, data.length());
        } finally {
            rwl.writeLock().unlock();
        }
    }

    public byte[] read(Translog.Location location) throws IOException {
        rwl.readLock().lock();
        try {
            return Channels.readFromFileChannel(raf.channel(), location.translogLocation, location.size);
        } finally {
            rwl.readLock().unlock();
        }
    }

    public void close(boolean delete) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            if (!delete) {
                try {
                    sync();
                } catch (Exception e) {
                    throw new TranslogException(shardId, "failed to sync on close", e);
                }
            }
        } finally {
            raf.decreaseRefCount(delete);
        }
    }

    /**
     * Returns a snapshot on this file, <tt>null</tt> if it failed to snapshot.
     */
    public FsChannelSnapshot snapshot() throws TranslogException {
        if (raf.increaseRefCount()) {
            boolean success = false;
            try {
                rwl.writeLock().lock();
                try {
                    FsChannelSnapshot snapshot = new FsChannelSnapshot(this.id, raf, lastWrittenPosition, operationCounter);
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
                    raf.decreaseRefCount(false);
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

    public void sync() throws IOException {
        // check if we really need to sync here...
        if (!syncNeeded()) {
            return;
        }
        rwl.writeLock().lock();
        try {
            lastSyncPosition = lastWrittenPosition;
            raf.channel().force(false);
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
}
