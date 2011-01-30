/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.CachedStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogException;
import org.elasticsearch.index.translog.TranslogStreams;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author kimchy (shay.banon)
 */
public class FsTranslog extends AbstractIndexShardComponent implements Translog {

    private final File location;

    private final boolean useStream;

    private final Object mutex = new Object();

    private boolean syncOnEachOperation = false;

    private volatile long id = 0;

    private final AtomicInteger operationCounter = new AtomicInteger();

    private AtomicLong lastPosition = new AtomicLong(0);
    private AtomicLong lastWrittenPosition = new AtomicLong(0);

    private RafReference raf;

    @Inject public FsTranslog(ShardId shardId, @IndexSettings Settings indexSettings, NodeEnvironment nodeEnv) {
        super(shardId, indexSettings);
        this.location = new File(nodeEnv.shardLocation(shardId), "translog");
        this.location.mkdirs();
        this.useStream = componentSettings.getAsBoolean("use_stream", false);
    }

    public FsTranslog(ShardId shardId, @IndexSettings Settings indexSettings, File location) {
        this(shardId, indexSettings, location, false);
    }

    public FsTranslog(ShardId shardId, @IndexSettings Settings indexSettings, File location, boolean useStream) {
        super(shardId, indexSettings);
        this.location = location;
        this.location.mkdirs();
        this.useStream = useStream;
    }

    public File location() {
        return location;
    }

    @Override public long currentId() {
        return this.id;
    }

    @Override public int numberOfOperations() {
        return operationCounter.get();
    }

    @Override public long memorySizeInBytes() {
        return 0;
    }

    @Override public long translogSizeInBytes() {
        return lastWrittenPosition.get();
    }

    @Override public void clearUnreferenced() {
        synchronized (mutex) {
            File[] files = location.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.getName().equals("translog-" + id)) {
                        continue;
                    }
                    try {
                        file.delete();
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }
        }
    }

    @Override public void newTranslog() throws TranslogException {
        synchronized (mutex) {
            operationCounter.set(0);
            lastPosition.set(0);
            lastWrittenPosition.set(0);
            this.id = id + 1;
            if (raf != null) {
                raf.decreaseRefCount(true);
            }
            try {
                raf = new RafReference(new File(location, "translog-" + id));
                raf.raf().setLength(0);
            } catch (IOException e) {
                raf = null;
                throw new TranslogException(shardId, "translog not found", e);
            }
        }
    }

    @Override public void newTranslog(long id) throws TranslogException {
        synchronized (mutex) {
            operationCounter.set(0);
            lastPosition.set(0);
            lastWrittenPosition.set(0);
            this.id = id;
            if (raf != null) {
                raf.decreaseRefCount(true);
            }
            try {
                raf = new RafReference(new File(location, "translog-" + id));
                // clean the file if it exists
                raf.raf().setLength(0);
            } catch (IOException e) {
                raf = null;
                throw new TranslogException(shardId, "translog not found", e);
            }
        }
    }

    @Override public void add(Operation operation) throws TranslogException {
        try {
            BytesStreamOutput out = CachedStreamOutput.cachedBytes();
            out.writeInt(0); // marker for the size...
            TranslogStreams.writeTranslogOperation(out, operation);
            out.flush();

            int size = out.size();
            out.seek(0);
            out.writeInt(size - 4);

            long position = lastPosition.getAndAdd(size);
            // use channel#write and not raf#write since it allows for concurrent writes
            // with regards to positions
            raf.channel().write(ByteBuffer.wrap(out.unsafeByteArray(), 0, size), position);
            if (syncOnEachOperation) {
                raf.channel().force(false);
            }
            synchronized (mutex) {
                lastWrittenPosition.getAndAdd(size);
                operationCounter.incrementAndGet();
            }
        } catch (Exception e) {
            throw new TranslogException(shardId, "Failed to write operation [" + operation + "]", e);
        }
    }

    @Override public Snapshot snapshot() throws TranslogException {
        synchronized (mutex) {
            try {
                raf.increaseRefCount();
                raf.channel().force(true);  // sync here, so we make sure we read back teh data?
                if (useStream) {
                    return new FsStreamSnapshot(shardId, this.id, raf, lastWrittenPosition.get(), operationCounter.get(), operationCounter.get());
                } else {
                    return new FsChannelSnapshot(shardId, this.id, raf, lastWrittenPosition.get(), operationCounter.get(), operationCounter.get());
                }
            } catch (Exception e) {
                throw new TranslogException(shardId, "Failed to snapshot", e);
            }
        }
    }

    @Override public Snapshot snapshot(Snapshot snapshot) {
        synchronized (mutex) {
            if (currentId() != snapshot.translogId()) {
                return snapshot();
            }
            try {
                raf.increaseRefCount();
                raf.channel().force(true); // sync here, so we make sure we read back teh data?
                if (useStream) {
                    FsStreamSnapshot newSnapshot = new FsStreamSnapshot(shardId, id, raf, lastWrittenPosition.get(), operationCounter.get(), operationCounter.get() - snapshot.totalOperations());
                    newSnapshot.seekForward(snapshot.position());
                    return newSnapshot;
                } else {
                    FsChannelSnapshot newSnapshot = new FsChannelSnapshot(shardId, id, raf, lastWrittenPosition.get(), operationCounter.get(), operationCounter.get() - snapshot.totalOperations());
                    newSnapshot.seekForward(snapshot.position());
                    return newSnapshot;
                }
            } catch (Exception e) {
                throw new TranslogException(shardId, "Failed to snapshot", e);
            }
        }
    }

    @Override public void sync() {
        synchronized (mutex) {
            if (raf != null) {
                try {
                    raf.channel().force(true);
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    @Override public void syncOnEachOperation(boolean syncOnEachOperation) {
        synchronized (mutex) {
            this.syncOnEachOperation = syncOnEachOperation;
        }
    }

    @Override public void close(boolean delete) {
        synchronized (mutex) {
            if (raf != null) {
                raf.decreaseRefCount(delete);
                raf = null;
            }
        }
    }
}
