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

import java.io.IOException;
import java.nio.ByteBuffer;

public final class SimpleFsTranslogFile extends FsTranslogFile {

    private volatile int operationCounter = 0;
    private volatile long lastPosition = 0;
    private volatile long lastWrittenPosition = 0;
    private volatile long lastSyncPosition = 0;

    public SimpleFsTranslogFile(ShardId shardId, long id, ChannelReference channelReference) throws IOException {
        super(shardId, id, channelReference);
        int headerSize = this.channelReference.stream().writeHeader(channelReference.channel());
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
            long position = lastPosition;
            data.writeTo(channelReference.channel());
            lastPosition = lastPosition + data.length();
            lastWrittenPosition = lastWrittenPosition + data.length();
            operationCounter = operationCounter + 1;
            return new Translog.Location(id, position, data.length());
        }
    }

    @Override
    protected void readBytes(ByteBuffer buffer, long position) throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            Channels.readFromFileChannelWithEofException(channelReference.channel(), position, buffer);
        }
    }

    @Override
    public void doClose() throws IOException {
        try {
            sync();
        } finally {
            super.doClose();
        }
    }

    public FsChannelImmutableReader immutableReader() throws TranslogException {
        if (channelReference.tryIncRef()) {
            try (ReleasableLock lock = writeLock.acquire()) {
                FsChannelImmutableReader reader = new FsChannelImmutableReader(this.id, channelReference, lastWrittenPosition, operationCounter);
                channelReference.incRef(); // for the new object
                return reader;
            } finally {
                channelReference.decRef();
            }
        } else {
            throw new TranslogException(shardId, "can't increment channel [" + channelReference + "] channel ref count");
        }

    }

    @Override
    public boolean syncNeeded() {
        return lastWrittenPosition != lastSyncPosition;
    }

    @Override
    public void sync() throws IOException {
        // check if we really need to sync here...
        if (!syncNeeded()) {
            return;
        }
        try (ReleasableLock lock = writeLock.acquire()) {
            lastSyncPosition = lastWrittenPosition;
            channelReference.channel().force(false);
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
