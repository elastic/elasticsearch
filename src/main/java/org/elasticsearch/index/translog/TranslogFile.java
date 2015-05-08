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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TranslogFile extends ChannelReader {

    protected final ShardId shardId;
    protected final ReleasableLock readLock;
    protected final ReleasableLock writeLock;
    /* the offset in bytes that was written when the file was last synced*/
    protected volatile long lastSyncedOffset;
    /* the number of translog operations written to this file */
    protected volatile int operationCounter;
    /* the offset in bytes written to the file */
    protected volatile long writtenOffset;

    public TranslogFile(ShardId shardId, long id, ChannelReference channelReference) throws IOException {
        super(id, channelReference);
        this.shardId = shardId;
        ReadWriteLock rwl = new ReentrantReadWriteLock();
        readLock = new ReleasableLock(rwl.readLock());
        writeLock = new ReleasableLock(rwl.writeLock());
        final TranslogStream stream = this.channelReference.stream();
        int headerSize = stream.writeHeader(channelReference.channel());
        this.writtenOffset += headerSize;
        this.lastSyncedOffset += headerSize;
    }


    public enum Type {

        SIMPLE() {
            @Override
            public TranslogFile create(ShardId shardId, long id, ChannelReference channelReference, int bufferSize) throws IOException {
                return new TranslogFile(shardId, id, channelReference);
            }
        },
        BUFFERED() {
            @Override
            public TranslogFile create(ShardId shardId, long id, ChannelReference channelReference, int bufferSize) throws IOException {
                return new BufferingTranslogFile(shardId, id, channelReference, bufferSize);
            }
        };

        public abstract TranslogFile create(ShardId shardId, long id, ChannelReference raf, int bufferSize) throws IOException;

        public static Type fromString(String type) {
            if (SIMPLE.name().equalsIgnoreCase(type)) {
                return SIMPLE;
            } else if (BUFFERED.name().equalsIgnoreCase(type)) {
                return BUFFERED;
            }
            throw new IllegalArgumentException("No translog fs type [" + type + "]");
        }
    }


    /** add the given bytes to the translog and return the location they were written at */
    public Translog.Location add(BytesReference data) throws IOException {
        try (ReleasableLock lock = writeLock.acquire()) {
            long position = writtenOffset;
            data.writeTo(channelReference.channel());
            writtenOffset = writtenOffset + data.length();
            operationCounter = operationCounter + 1;
            return new Translog.Location(id, position, data.length());
        }
    }

    /** reuse resources from another translog file, which is guaranteed not to be used anymore */
    public void reuse(TranslogFile other) throws TranslogException {}

    /** change the size of the internal buffer if relevant */
    public void updateBufferSize(int bufferSize) throws TranslogException {}

    /** write all buffered ops to disk and fsync file */
    public void sync() throws IOException {
        // check if we really need to sync here...
        if (syncNeeded()) {
            try (ReleasableLock lock = writeLock.acquire()) {
                lastSyncedOffset = writtenOffset;
                channelReference.channel().force(false);
            }
        }
    }

    /** returns true if there are buffered ops */
    public boolean syncNeeded() {
        return writtenOffset != lastSyncedOffset; // by default nothing is buffered
    }

    @Override
    public int totalOperations() {
        return operationCounter;
    }

    @Override
    public long sizeInBytes() {
        return writtenOffset;
    }

    @Override
    public ChannelSnapshot newSnapshot() {
        return new ChannelSnapshot(immutableReader());
    }

    /**
     * Flushes the buffer if the translog is buffered.
     */
    protected void flush() throws IOException {}

    /**
     * returns a new reader that follows the current writes (most importantly allows making
     * repeated snapshots that includes new content)
     */
    public ChannelReader reader() {
        channelReference.incRef();
        boolean success = false;
        try {
            ChannelReader reader = new InnerReader(this.id, channelReference);
            success = true;
            return reader;
        } finally {
            if (!success) {
                channelReference.decRef();
            }
        }
    }


    /** returns a new immutable reader which only exposes the current written operation * */
    public ChannelImmutableReader immutableReader() throws TranslogException {
        if (channelReference.tryIncRef()) {
            try (ReleasableLock lock = writeLock.acquire()) {
                flush();
                ChannelImmutableReader reader = new ChannelImmutableReader(this.id, channelReference, writtenOffset, operationCounter);
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

    boolean assertBytesAtLocation(Translog.Location location, BytesReference expectedBytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(location.size);
        readBytes(buffer, location.translogLocation);
        return new BytesArray(buffer.array()).equals(expectedBytes);
    }

    /**
     * this class is used when one wants a reference to this file which exposes all recently written operation.
     * as such it needs access to the internals of the current reader
     */
    final class InnerReader extends ChannelReader {

        public InnerReader(long id, ChannelReference channelReference) {
            super(id, channelReference);
        }

        @Override
        public long sizeInBytes() {
            return TranslogFile.this.sizeInBytes();
        }

        @Override
        public int totalOperations() {
            return TranslogFile.this.totalOperations();
        }

        @Override
        protected void readBytes(ByteBuffer buffer, long position) throws IOException {
            TranslogFile.this.readBytes(buffer, position);
        }

        @Override
        public ChannelSnapshot newSnapshot() {
            return TranslogFile.this.newSnapshot();
        }
    }

    /**
     * Syncs the translog up to at least the given offset unless already synced
     * @return <code>true</code> if this call caused an actual sync operation
     */
    public boolean syncUpTo(long offset) throws IOException {
        if (lastSyncedOffset < offset) {
            sync();
            return true;
        }
        return false;
    }

    @Override
    protected final void doClose() throws IOException {
        try {
            sync();
        } finally {
            super.doClose();
        }
    }

    @Override
    protected void readBytes(ByteBuffer buffer, long position) throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            Channels.readFromFileChannelWithEofException(channelReference.channel(), position, buffer);
        }
    }
}
