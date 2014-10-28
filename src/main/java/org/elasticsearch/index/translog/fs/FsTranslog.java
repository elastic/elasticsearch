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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.*;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 */
public class FsTranslog extends AbstractIndexShardComponent implements Translog {

    public static final String INDEX_TRANSLOG_FS_TYPE = "index.translog.fs.type";

    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            FsTranslogFile.Type type = FsTranslogFile.Type.fromString(settings.get(INDEX_TRANSLOG_FS_TYPE, FsTranslog.this.type.name()));
            if (type != FsTranslog.this.type) {
                logger.info("updating type from [{}] to [{}]", FsTranslog.this.type, type);
                FsTranslog.this.type = type;
            }
        }
    }

    private final IndexSettingsService indexSettingsService;
    private final BigArrays bigArrays;

    private final ReadWriteLock rwl = new ReentrantReadWriteLock();
    private final File[] locations;

    private volatile FsTranslogFile current;
    private volatile FsTranslogFile trans;

    private FsTranslogFile.Type type;

    private boolean syncOnEachOperation = false;

    private volatile int bufferSize;
    private volatile int transientBufferSize;

    private final ApplySettings applySettings = new ApplySettings();

    @Inject
    public FsTranslog(ShardId shardId, @IndexSettings Settings indexSettings, IndexSettingsService indexSettingsService, NodeEnvironment nodeEnv, BigArrays bigArrays) {
        super(shardId, indexSettings);
        this.indexSettingsService = indexSettingsService;
        this.bigArrays = bigArrays;
        File[] shardLocations = nodeEnv.shardLocations(shardId);
        this.locations = new File[shardLocations.length];
        for (int i = 0; i < shardLocations.length; i++) {
            locations[i] = new File(shardLocations[i], "translog");
            FileSystemUtils.mkdirs(locations[i]);
        }

        this.type = FsTranslogFile.Type.fromString(componentSettings.get("type", FsTranslogFile.Type.BUFFERED.name()));
        this.bufferSize = (int) componentSettings.getAsBytesSize("buffer_size", ByteSizeValue.parseBytesSizeValue("64k")).bytes(); // Not really interesting, updated by IndexingMemoryController...
        this.transientBufferSize = (int) componentSettings.getAsBytesSize("transient_buffer_size", ByteSizeValue.parseBytesSizeValue("8k")).bytes();

        indexSettingsService.addListener(applySettings);
    }

    public FsTranslog(ShardId shardId, @IndexSettings Settings indexSettings, File location) {
        super(shardId, indexSettings);
        this.indexSettingsService = null;
        this.locations = new File[]{location};
        FileSystemUtils.mkdirs(location);
        this.bigArrays = BigArrays.NON_RECYCLING_INSTANCE;

        this.type = FsTranslogFile.Type.fromString(componentSettings.get("type", FsTranslogFile.Type.BUFFERED.name()));
        this.bufferSize = (int) componentSettings.getAsBytesSize("buffer_size", ByteSizeValue.parseBytesSizeValue("64k")).bytes();
    }

    @Override
    public void closeWithDelete() {
        close(true);
    }

    @Override
    public void close() throws ElasticsearchException {
        close(false);
    }

    @Override
    public void updateBuffer(ByteSizeValue bufferSize) {
        this.bufferSize = bufferSize.bytesAsInt();
        rwl.writeLock().lock();
        try {
            FsTranslogFile current1 = this.current;
            if (current1 != null) {
                current1.updateBufferSize(this.bufferSize);
            }
            current1 = this.trans;
            if (current1 != null) {
                current1.updateBufferSize(this.bufferSize);
            }
        } finally {
            rwl.writeLock().unlock();
        }
    }

    private void close(boolean delete) {
        if (indexSettingsService != null) {
            indexSettingsService.removeListener(applySettings);
        }
        rwl.writeLock().lock();
        try {
            FsTranslogFile current1 = this.current;
            if (current1 != null) {
                current1.close(delete);
            }
            current1 = this.trans;
            if (current1 != null) {
                current1.close(delete);
            }
        } finally {
            rwl.writeLock().unlock();
        }
    }

    public File[] locations() {
        return locations;
    }

    @Override
    public long currentId() {
        FsTranslogFile current1 = this.current;
        if (current1 == null) {
            return -1;
        }
        return current1.id();
    }

    @Override
    public int estimatedNumberOfOperations() {
        FsTranslogFile current1 = this.current;
        if (current1 == null) {
            return 0;
        }
        return current1.estimatedNumberOfOperations();
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    @Override
    public long translogSizeInBytes() {
        FsTranslogFile current1 = this.current;
        if (current1 == null) {
            return 0;
        }
        return current1.translogSizeInBytes();
    }

    @Override
    public void clearUnreferenced() {
        rwl.writeLock().lock();
        try {
            for (File location : locations) {
                File[] files = location.listFiles();
                if (files != null) {
                    for (File file : files) {
                        if (file.getName().equals("translog-" + current.id())) {
                            continue;
                        }
                        if (trans != null && file.getName().equals("translog-" + trans.id())) {
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
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override
    public void newTranslog(long id) throws TranslogException {
        rwl.writeLock().lock();
        try {
            FsTranslogFile newFile;
            long size = Long.MAX_VALUE;
            File location = null;
            for (File file : locations) {
                long currentFree = file.getFreeSpace();
                if (currentFree < size) {
                    size = currentFree;
                    location = file;
                } else if (currentFree == size && ThreadLocalRandom.current().nextBoolean()) {
                    location = file;
                }
            }
            try {
                newFile = type.create(shardId, id, new RafReference(new File(location, "translog-" + id)), bufferSize);
            } catch (IOException e) {
                throw new TranslogException(shardId, "failed to create new translog file", e);
            }
            FsTranslogFile old = current;
            current = newFile;
            if (old != null) {
                // we might create a new translog overriding the current translog id
                boolean delete = true;
                if (old.id() == id) {
                    delete = false;
                }
                old.close(delete);
            }
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override
    public void newTransientTranslog(long id) throws TranslogException {
        rwl.writeLock().lock();
        try {
            assert this.trans == null;
            long size = Long.MAX_VALUE;
            File location = null;
            for (File file : locations) {
                long currentFree = file.getFreeSpace();
                if (currentFree < size) {
                    size = currentFree;
                    location = file;
                } else if (currentFree == size && ThreadLocalRandom.current().nextBoolean()) {
                    location = file;
                }
            }
            this.trans = type.create(shardId, id, new RafReference(new File(location, "translog-" + id)), transientBufferSize);
        } catch (IOException e) {
            throw new TranslogException(shardId, "failed to create new translog file", e);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override
    public void makeTransientCurrent() {
        FsTranslogFile old;
        rwl.writeLock().lock();
        try {
            assert this.trans != null;
            old = current;
            this.current = this.trans;
            this.trans = null;
        } finally {
            rwl.writeLock().unlock();
        }
        old.close(true);
        current.reuse(old);
    }

    @Override
    public void revertTransient() {
        FsTranslogFile tmpTransient;
        rwl.writeLock().lock();
        try {
            tmpTransient = trans;
            this.trans = null;
        } finally {
            rwl.writeLock().unlock();
        }
        // previous transient might be null because it was failed on its creation
        // for example
        if (tmpTransient != null) {
            tmpTransient.close(true);
        }
    }

    /**
     * Returns the translog that should be read for the specified location. If
     * the transient or current translog does not match, returns null
     */
    private FsTranslogFile translogForLocation(Location location) {
        if (trans != null && trans.id() == location.translogId) {
            return this.trans;
        }
        if (current.id() == location.translogId) {
            return this.current;
        }
        return null;
    }

    /**
     * Read the Operation object from the given location, returns null if the
     * Operation could not be read.
     */
    @Override
    public Translog.Operation read(Location location) {
        rwl.readLock().lock();
        try {
            FsTranslogFile translog = translogForLocation(location);
            if (translog != null) {
                byte[] data = translog.read(location);
                try (BytesStreamInput in = new BytesStreamInput(data, false)) {
                    // Return the Operation using the current version of the
                    // stream based on which translog is being read
                    return translog.getStream().read(in);
                }
            }
            return null;
        } catch (IOException e) {
            throw new ElasticsearchException("failed to read source from traslog location " + location, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public Location add(Operation operation) throws TranslogException {
        rwl.readLock().lock();
        boolean released = false;
        ReleasableBytesStreamOutput out = null;
        try {
            out = new ReleasableBytesStreamOutput(bigArrays);
            TranslogStreams.writeTranslogOperation(out, operation);
            ReleasableBytesReference bytes = out.bytes();
            Location location = current.add(bytes);
            if (syncOnEachOperation) {
                current.sync();
            }

            assert new BytesArray(current.read(location)).equals(bytes);

            FsTranslogFile trans = this.trans;
            if (trans != null) {
                try {
                    location = trans.add(bytes);
                } catch (ClosedChannelException e) {
                    // ignore
                }
            }
            Releasables.close(bytes);
            released = true;
            return location;
        } catch (Throwable e) {
            throw new TranslogException(shardId, "Failed to write operation [" + operation + "]", e);
        } finally {
            rwl.readLock().unlock();
            if (!released && out != null) {
                Releasables.close(out.bytes());
            }
        }
    }

    @Override
    public FsChannelSnapshot snapshot() throws TranslogException {
        while (true) {
            FsChannelSnapshot snapshot = current.snapshot();
            if (snapshot != null) {
                return snapshot;
            }
            Thread.yield();
        }
    }

    @Override
    public Snapshot snapshot(Snapshot snapshot) {
        FsChannelSnapshot snap = snapshot();
        if (snap.translogId() == snapshot.translogId()) {
            snap.seekTo(snapshot.position());
        }
        return snap;
    }

    @Override
    public void sync() throws IOException {
        FsTranslogFile current1 = this.current;
        if (current1 == null) {
            return;
        }
        try {
            current1.sync();
        } catch (IOException e) {
            // if we switches translots (!=), then this failure is not relevant
            // we are working on a new translog
            if (this.current == current1) {
                throw e;
            }
        }
    }

    @Override
    public boolean syncNeeded() {
        FsTranslogFile current1 = this.current;
        return current1 != null && current1.syncNeeded();
    }

    @Override
    public void syncOnEachOperation(boolean syncOnEachOperation) {
        this.syncOnEachOperation = syncOnEachOperation;
        if (syncOnEachOperation) {
            type = FsTranslogFile.Type.SIMPLE;
        } else {
            type = FsTranslogFile.Type.BUFFERED;
        }
    }

    @Override
    public TranslogStats stats() {
        return new TranslogStats(estimatedNumberOfOperations(), translogSizeInBytes());
    }
}
