/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleFsTranslogFile implements FsTranslogFile {

    private final long id;
    private final ShardId shardId;
    private final RafReference raf;

    private final AtomicInteger operationCounter = new AtomicInteger();

    private final AtomicLong lastPosition = new AtomicLong(0);
    private final AtomicLong lastWrittenPosition = new AtomicLong(0);

    private volatile long lastSyncPosition = 0;

    public SimpleFsTranslogFile(ShardId shardId, long id, RafReference raf) throws IOException {
        this.shardId = shardId;
        this.id = id;
        this.raf = raf;
        raf.raf().setLength(0);
    }

    public long id() {
        return this.id;
    }

    public int estimatedNumberOfOperations() {
        return operationCounter.get();
    }

    public long translogSizeInBytes() {
        return lastWrittenPosition.get();
    }

    public Translog.Location add(byte[] data, int from, int size) throws IOException {
        long position = lastPosition.getAndAdd(size);
        raf.channel().write(ByteBuffer.wrap(data, from, size), position);
        lastWrittenPosition.getAndAdd(size);
        operationCounter.incrementAndGet();
        return new Translog.Location(id, position, size);
    }

    public byte[] read(Translog.Location location) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(location.size);
        raf.channel().read(buffer, location.translogLocation);
        return buffer.array();
    }

    public void close(boolean delete) {
        sync();
        raf.decreaseRefCount(delete);
    }

    /**
     * Returns a snapshot on this file, <tt>null</tt> if it failed to snapshot.
     */
    public FsChannelSnapshot snapshot() throws TranslogException {
        try {
            if (!raf.increaseRefCount()) {
                return null;
            }
            return new FsChannelSnapshot(this.id, raf, lastWrittenPosition.get(), operationCounter.get());
        } catch (Exception e) {
            throw new TranslogException(shardId, "Failed to snapshot", e);
        }
    }

    @Override
    public boolean syncNeeded() {
        return lastWrittenPosition.get() != lastSyncPosition;
    }

    public void sync() {
        try {
            // check if we really need to sync here...
            long last = lastWrittenPosition.get();
            if (last == lastSyncPosition) {
                return;
            }
            lastSyncPosition = last;
            raf.channel().force(false);
        } catch (Exception e) {
            // ignore
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
