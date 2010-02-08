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

package org.elasticsearch.index.gateway.fs;

import com.google.inject.Inject;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.gateway.IndexShardGateway;
import org.elasticsearch.index.gateway.IndexShardGatewayRecoveryException;
import org.elasticsearch.index.gateway.IndexShardGatewaySnapshotFailedException;
import org.elasticsearch.index.gateway.RecoveryStatus;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.InternalIndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.SizeUnit;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.settings.Settings;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Lists.*;
import static org.elasticsearch.index.translog.TranslogStreams.*;
import static org.elasticsearch.util.lucene.Directories.*;

/**
 * @author kimchy (Shay Banon)
 */
public class FsIndexShardGateway extends AbstractIndexShardComponent implements IndexShardGateway {

    private final InternalIndexShard indexShard;

    private final ThreadPool threadPool;

    private final Store store;

    private final File location;

    private final File locationIndex;

    private final File locationTranslog;

    private long lastIndexVersion;

    private long lastTranslogId = -1;

    private int lastTranslogSize;

    private RandomAccessFile translogFile;

    @Inject public FsIndexShardGateway(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool, FsIndexGateway fsIndexGateway, IndexShard indexShard, Store store) {
        super(shardId, indexSettings);
        this.threadPool = threadPool;
        this.indexShard = (InternalIndexShard) indexShard;
        this.store = store;
        this.location = new File(fsIndexGateway.indexGatewayHome(), Integer.toString(shardId.id()));
        this.locationIndex = new File(location, "index");
        this.locationTranslog = new File(location, "translog");

        locationIndex.mkdirs();
        locationTranslog.mkdirs();
    }

    @Override public boolean requiresSnapshotScheduling() {
        return true;
    }

    @Override public String toString() {
        return "fs[" + location + "]";
    }

    @Override public void close() {
    }

    @Override public RecoveryStatus recover() throws IndexShardGatewayRecoveryException {
        RecoveryStatus.Index recoveryStatusIndex = recoverIndex();
        RecoveryStatus.Translog recoveryStatusTranslog = recoverTranslog();
        // update the last up to date values
        indexShard.snapshot(new Engine.SnapshotHandler() {
            @Override public void snapshot(SnapshotIndexCommit snapshotIndexCommit, Translog.Snapshot translogSnapshot) throws EngineException {
                lastIndexVersion = snapshotIndexCommit.getVersion();
                lastTranslogId = translogSnapshot.translogId();
                lastTranslogSize = translogSnapshot.size();
            }
        });
        return new RecoveryStatus(recoveryStatusIndex, recoveryStatusTranslog);
    }

    @Override public void snapshot(final SnapshotIndexCommit snapshotIndexCommit, final Translog.Snapshot translogSnapshot) {
        boolean indexDirty = false;
        boolean translogDirty = false;

        if (lastIndexVersion != snapshotIndexCommit.getVersion()) {
            indexDirty = true;
            // snapshot into the index
            final CountDownLatch latch = new CountDownLatch(snapshotIndexCommit.getFiles().length);
            final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
            for (final String fileName : snapshotIndexCommit.getFiles()) {
                if (fileName.equals(snapshotIndexCommit.getSegmentsFileName())) {
                    latch.countDown();
                    continue;
                }
                try {
                    IndexInput indexInput = snapshotIndexCommit.getDirectory().openInput(fileName);
                    File snapshotFile = new File(locationIndex, fileName);
                    if (snapshotFile.exists() && (snapshotFile.length() == indexInput.length())) {
                        // we assume its the same one, no need to copy
                        latch.countDown();
                        continue;
                    }
                    indexInput.close();
                } catch (Exception e) {
                    logger.debug("Failed to verify file equality based on length, copying...", e);
                }
                threadPool.execute(new Runnable() {
                    @Override public void run() {
                        try {
                            copyFromDirectory(snapshotIndexCommit.getDirectory(), fileName, new File(locationIndex, fileName));
                        } catch (Exception e) {
                            lastException.set(e);
                        } finally {
                            latch.countDown();
                        }
                    }
                });
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                lastException.set(e);
            }
            if (lastException.get() != null) {
                throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to perform snapshot (index files)", lastException.get());
            }
        }
        if (translogSnapshot.translogId() != lastTranslogId || translogFile == null) {
            translogDirty = true;
            if (translogFile != null) {
                try {
                    translogFile.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            try {
                File f = new File(locationTranslog, "translog-" + translogSnapshot.translogId());
                translogFile = new RandomAccessFile(f, "rw");
                translogFile.writeInt(-1); // write the number of operations header with -1 currently
                // double check that we managed to read/write correctly
                translogFile.seek(0);
                if (translogFile.readInt() != -1) {
                    throw new ElasticSearchIllegalStateException("Wrote to snapshot file [" + f + "] but did not read...");
                }
                for (Translog.Operation operation : translogSnapshot) {
                    writeTranslogOperation(translogFile, operation);
                }
            } catch (Exception e) {
                try {
                    translogFile.close();
                } catch (IOException e1) {
                    // ignore
                }
                translogFile = null;
                throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to snapshot translog", e);
            }
        } else if (translogSnapshot.size() > lastTranslogSize) {
            translogDirty = true;
            try {
                for (Translog.Operation operation : translogSnapshot.skipTo(lastTranslogSize)) {
                    writeTranslogOperation(translogFile, operation);
                }
            } catch (Exception e) {
                try {
                    translogFile.close();
                } catch (IOException e1) {
                    // ignore
                }
                translogFile = null;
                throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to snapshot translog", e);
            }
        }

        // now write the segments file and update the translog header
        try {
            if (indexDirty) {
                copyFromDirectory(snapshotIndexCommit.getDirectory(), snapshotIndexCommit.getSegmentsFileName(),
                        new File(locationIndex, snapshotIndexCommit.getSegmentsFileName()));
            }
            if (translogDirty) {
                translogFile.seek(0);
                translogFile.writeInt(translogSnapshot.size());
                translogFile.seek(translogFile.length());
                translogFile.getFD().sync();
            }
        } catch (Exception e) {
            try {
                translogFile.close();
            } catch (IOException e1) {
                // ignore
            }
            translogFile = null;
            throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to finalize snapshot", e);
        }

        // delete the old translog
        if (lastTranslogId != translogSnapshot.translogId()) {
            new File(locationTranslog, "translog-" + lastTranslogId).delete();
        }


        lastIndexVersion = snapshotIndexCommit.getVersion();
        lastTranslogId = translogSnapshot.translogId();
        lastTranslogSize = translogSnapshot.size();
    }

    private RecoveryStatus.Index recoverIndex() throws IndexShardGatewayRecoveryException {
        File[] files = locationIndex.listFiles();
        final CountDownLatch latch = new CountDownLatch(files.length);
        final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
        for (final File file : files) {
            threadPool.execute(new Runnable() {
                @Override public void run() {
                    try {
                        copyToDirectory(file, store.directory(), file.getName());
                    } catch (Exception e) {
                        logger.debug("Failed to read [" + file + "] into [" + store + "]", e);
                        lastException.set(e);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            lastException.set(e);
        }
        if (lastException.get() != null) {
            throw new IndexShardGatewayRecoveryException(shardId(), "Failed to recover index files", lastException.get());
        }
        long totalSize = 0;
        for (File file : files) {
            totalSize += file.length();
        }
        return new RecoveryStatus.Index(files.length, new SizeValue(totalSize, SizeUnit.BYTES));
    }

    private RecoveryStatus.Translog recoverTranslog() throws IndexShardGatewayRecoveryException {
        RandomAccessFile raf = null;
        try {
            long recoveryTranslogId = findLatestTranslogId(locationTranslog);
            if (recoveryTranslogId == -1) {
                // no recovery file found, start the shard and bail
                indexShard.start();
                return new RecoveryStatus.Translog(0, new SizeValue(0, SizeUnit.BYTES));
            }
            File recoveryTranslogFile = new File(locationTranslog, "translog-" + recoveryTranslogId);
            raf = new RandomAccessFile(recoveryTranslogFile, "r");
            int numberOfOperations = raf.readInt();
            ArrayList<Translog.Operation> operations = newArrayListWithExpectedSize(numberOfOperations);
            for (int i = 0; i < numberOfOperations; i++) {
                operations.add(readTranslogOperation(raf));
            }
            indexShard.performRecovery(operations);
            return new RecoveryStatus.Translog(operations.size(), new SizeValue(recoveryTranslogFile.length(), SizeUnit.BYTES));
        } catch (Exception e) {
            throw new IndexShardGatewayRecoveryException(shardId(), "Failed to perform recovery of translog", e);
        } finally {
            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    private static long findLatestTranslogId(File location) {
        File[] files = location.listFiles(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return name.startsWith("translog-");
            }
        });

        long index = -1;
        for (File file : files) {
            String name = file.getName();
            try {
                RandomAccessFile raf = new RandomAccessFile(file, "r");
                // if header is -1, then its not properly written, ignore it
                if (raf.readInt() == -1) {
                    continue;
                }
            } catch (Exception e) {
                // broken file, continue
                continue;
            }
            long fileIndex = Long.parseLong(name.substring(name.indexOf('-') + 1));
            if (fileIndex >= index) {
                index = fileIndex;
            }
        }

        return index;
    }
}
