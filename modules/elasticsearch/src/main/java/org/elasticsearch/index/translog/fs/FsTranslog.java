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
import org.elasticsearch.common.io.FileSystemUtils;
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
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author kimchy (shay.banon)
 */
public class FsTranslog extends AbstractIndexShardComponent implements Translog {

    private final ReadWriteLock rwl = new ReentrantReadWriteLock();
    private final File location;

    private volatile FsTranslogFile current;
    private volatile FsTranslogFile trans;

    private boolean syncOnEachOperation = false;

    @Inject public FsTranslog(ShardId shardId, @IndexSettings Settings indexSettings, NodeEnvironment nodeEnv) {
        super(shardId, indexSettings);
        this.location = new File(nodeEnv.shardLocation(shardId), "translog");
        FileSystemUtils.mkdirs(this.location);
    }

    public FsTranslog(ShardId shardId, @IndexSettings Settings indexSettings, File location) {
        super(shardId, indexSettings);
        this.location = location;
        FileSystemUtils.mkdirs(this.location);
    }

    public File location() {
        return location;
    }

    @Override public long currentId() {
        FsTranslogFile current1 = this.current;
        if (current1 == null) {
            return -1;
        }
        return current1.id();
    }

    @Override public int estimatedNumberOfOperations() {
        FsTranslogFile current1 = this.current;
        if (current1 == null) {
            return 0;
        }
        return current1.estimatedNumberOfOperations();
    }

    @Override public long memorySizeInBytes() {
        return 0;
    }

    @Override public long translogSizeInBytes() {
        FsTranslogFile current1 = this.current;
        if (current1 == null) {
            return 0;
        }
        return current1.translogSizeInBytes();
    }

    @Override public void clearUnreferenced() {
        rwl.writeLock().lock();
        try {
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
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override public void newTranslog(long id) throws TranslogException {
        rwl.writeLock().lock();
        try {
            FsTranslogFile newFile;
            try {
                newFile = new FsTranslogFile(shardId, id, new RafReference(new File(location, "translog-" + id)));
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

    @Override public void newTransientTranslog(long id) throws TranslogException {
        rwl.writeLock().lock();
        try {
            assert this.trans == null;
            this.trans = new FsTranslogFile(shardId, id, new RafReference(new File(location, "translog-" + id)));
        } catch (IOException e) {
            throw new TranslogException(shardId, "failed to create new translog file", e);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override public void makeTransientCurrent() {
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
    }

    @Override public void revertTransient() {
        FsTranslogFile old;
        rwl.writeLock().lock();
        try {
            old = trans;
            this.trans = null;
        } finally {
            rwl.writeLock().unlock();
        }
        old.close(true);
    }

    public byte[] read(Location location) {
        rwl.readLock().lock();
        try {
            FsTranslogFile trans = this.trans;
            if (trans != null && trans.id() == location.translogId) {
                try {
                    return trans.read(location);
                } catch (Exception e) {
                    // ignore
                }
            }
            if (current.id() == location.translogId) {
                try {
                    return current.read(location);
                } catch (Exception e) {
                    // ignore
                }
            }
            return null;
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override public Location add(Operation operation) throws TranslogException {
        CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
        rwl.readLock().lock();
        try {
            BytesStreamOutput out = cachedEntry.cachedBytes();
            out.writeInt(0); // marker for the size...
            TranslogStreams.writeTranslogOperation(out, operation);
            out.flush();

            int size = out.size();
            out.seek(0);
            out.writeInt(size - 4);

            Location location = current.add(out.underlyingBytes(), 0, size);
            if (syncOnEachOperation) {
                current.sync();
            }
            FsTranslogFile trans = this.trans;
            if (trans != null) {
                try {
                    location = trans.add(out.underlyingBytes(), 0, size);
                } catch (ClosedChannelException e) {
                    // ignore
                }
            }
            return location;
        } catch (Exception e) {
            throw new TranslogException(shardId, "Failed to write operation [" + operation + "]", e);
        } finally {
            rwl.readLock().unlock();
            CachedStreamOutput.pushEntry(cachedEntry);
        }
    }

    @Override public FsChannelSnapshot snapshot() throws TranslogException {
        while (true) {
            FsChannelSnapshot snapshot = current.snapshot();
            if (snapshot != null) {
                return snapshot;
            }
            Thread.yield();
        }
    }

    @Override public Snapshot snapshot(Snapshot snapshot) {
        FsChannelSnapshot snap = snapshot();
        if (snap.translogId() == snapshot.translogId()) {
            snap.seekForward(snapshot.position());
        }
        return snap;
    }

    @Override public void sync() {
        FsTranslogFile current1 = this.current;
        if (current1 == null) {
            return;
        }
        current1.sync();
    }

    @Override public void syncOnEachOperation(boolean syncOnEachOperation) {
        this.syncOnEachOperation = syncOnEachOperation;
    }

    @Override public void close(boolean delete) {
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
}
