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
import org.elasticsearch.common.io.FastByteArrayOutputStream;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogException;
import org.elasticsearch.index.translog.TranslogStreams;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author kimchy (shay.banon)
 */
public class FsTranslog extends AbstractIndexShardComponent implements Translog {

    private final File location;

    private final boolean useStream;

    private final Object mutex = new Object();

    private volatile long id;

    private final AtomicInteger operationCounter = new AtomicInteger();

    private RafReference raf;

    private volatile SoftReference<FastByteArrayOutputStream> cachedBos = new SoftReference<FastByteArrayOutputStream>(new FastByteArrayOutputStream());

    @Inject public FsTranslog(ShardId shardId, @IndexSettings Settings indexSettings, NodeEnvironment nodeEnv) {
        super(shardId, indexSettings);
        this.location = new File(new File(new File(new File(nodeEnv.nodeFile(), "indices"), shardId.index().name()), Integer.toString(shardId.id())), "translog");
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

    @Override public long currentId() {
        return this.id;
    }

    @Override public int size() {
        return operationCounter.get();
    }

    @Override public ByteSizeValue estimateMemorySize() {
        return new ByteSizeValue(0, ByteSizeUnit.BYTES);
    }

    @Override public void newTranslog(long id) throws TranslogException {
        synchronized (mutex) {
            operationCounter.set(0);
            this.id = id;
            if (raf != null) {
                raf.decreaseRefCount();
            }
            try {
                raf = new RafReference(new File(location, "translog-" + id));
            } catch (FileNotFoundException e) {
                raf = null;
                throw new TranslogException(shardId, "translog not found", e);
            }
        }
    }

    @Override public void add(Operation operation) throws TranslogException {
        synchronized (mutex) {
            FastByteArrayOutputStream bos = cachedBos.get();
            if (bos == null) {
                bos = new FastByteArrayOutputStream();
                cachedBos = new SoftReference<FastByteArrayOutputStream>(bos);
            }
            try {
                bos.reset();
                OutputStreamStreamOutput bosOs = new OutputStreamStreamOutput(bos);
                TranslogStreams.writeTranslogOperation(bosOs, operation);
                bosOs.flush();
                raf.raf().writeInt(bos.size());
                raf.raf().write(bos.unsafeByteArray(), 0, bos.size());
                operationCounter.incrementAndGet();
            } catch (Exception e) {
                throw new TranslogException(shardId, "Failed to write operation [" + operation + "]", e);
            }
        }
    }

    @Override public Snapshot snapshot() throws TranslogException {
        synchronized (mutex) {
            try {
                raf.increaseRefCount();
                if (useStream) {
                    return new FsStreamSnapshot(shardId, this.id, raf, raf.raf().getFilePointer());
                } else {
                    return new FsChannelSnapshot(shardId, this.id, raf, raf.raf().getFilePointer());
                }
            } catch (IOException e) {
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
                if (useStream) {
                    FsStreamSnapshot newSnapshot = new FsStreamSnapshot(shardId, id, raf, raf.raf().getFilePointer());
                    newSnapshot.seekForward(snapshot.position());
                    return newSnapshot;
                } else {
                    FsChannelSnapshot newSnapshot = new FsChannelSnapshot(shardId, id, raf, raf.raf().getFilePointer());
                    newSnapshot.seekForward(snapshot.position());
                    return newSnapshot;
                }
            } catch (IOException e) {
                throw new TranslogException(shardId, "Failed to snapshot", e);
            }
        }
    }

    @Override public void close() {
        synchronized (mutex) {
            if (raf != null) {
                raf.decreaseRefCount();
                raf = null;
            }
        }
    }
}
