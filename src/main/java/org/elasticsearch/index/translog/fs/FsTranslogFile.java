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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class FsTranslogFile extends FsChannelReader {

    protected final ShardId shardId;
    protected final ReleasableLock readLock;
    protected final ReleasableLock writeLock;

    public FsTranslogFile(ShardId shardId, long id, ChannelReference channelReference) {
        super(id, channelReference);
        this.shardId = shardId;
        ReadWriteLock rwl = new ReentrantReadWriteLock();
        readLock = new ReleasableLock(rwl.readLock());
        writeLock = new ReleasableLock(rwl.writeLock());
    }


    public static enum Type {

        SIMPLE() {
            @Override
            public FsTranslogFile create(ShardId shardId, long id, ChannelReference channelReference, int bufferSize) throws IOException {
                return new SimpleFsTranslogFile(shardId, id, channelReference);
            }
        },
        BUFFERED() {
            @Override
            public FsTranslogFile create(ShardId shardId, long id, ChannelReference channelReference, int bufferSize) throws IOException {
                return new BufferingFsTranslogFile(shardId, id, channelReference, bufferSize);
            }
        };

        public abstract FsTranslogFile create(ShardId shardId, long id, ChannelReference raf, int bufferSize) throws IOException;

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
    public abstract Translog.Location add(BytesReference data) throws IOException;

    /** reuse resources from another translog file, which is guaranteed not to be used anymore */
    public abstract void reuse(FsTranslogFile other) throws TranslogException;

    /** change the size of the internal buffer if relevant */
    public abstract void updateBufferSize(int bufferSize) throws TranslogException;

    /** write all buffered ops to disk and fsync file */
    public abstract void sync() throws IOException;

    /** returns true if there are buffered ops */
    public abstract boolean syncNeeded();

    @Override
    public FsChannelSnapshot newSnapshot() {
        return new FsChannelSnapshot(immutableReader());
    }

    /**
     * returns a new reader that follows the current writes (most importantly allows making
     * repeated snapshots that includes new content)
     */
    public FsChannelReader reader() {
        channelReference.incRef();
        boolean success = false;
        try {
            FsChannelReader reader = new InnerReader(this.id, channelReference);
            success = true;
            return reader;
        } finally {
            if (!success) {
                channelReference.decRef();
            }
        }
    }


    /** returns a new immutable reader which only exposes the current written operation * */
    abstract public FsChannelImmutableReader immutableReader();

    boolean assertBytesAtLocation(Translog.Location location, BytesReference expectedBytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(location.size);
        readBytes(buffer, location.translogLocation);
        return new BytesArray(buffer.array()).equals(expectedBytes);
    }

    /**
     * this class is used when one wants a reference to this file which exposes all recently written operation.
     * as such it needs access to the internals of the current reader
     */
    final class InnerReader extends FsChannelReader {

        public InnerReader(long id, ChannelReference channelReference) {
            super(id, channelReference);
        }

        @Override
        public long sizeInBytes() {
            return FsTranslogFile.this.sizeInBytes();
        }

        @Override
        public int totalOperations() {
            return FsTranslogFile.this.totalOperations();
        }

        @Override
        protected void readBytes(ByteBuffer buffer, long position) throws IOException {
            FsTranslogFile.this.readBytes(buffer, position);
        }

        @Override
        public FsChannelSnapshot newSnapshot() {
            return FsTranslogFile.this.newSnapshot();
        }
    }
}
