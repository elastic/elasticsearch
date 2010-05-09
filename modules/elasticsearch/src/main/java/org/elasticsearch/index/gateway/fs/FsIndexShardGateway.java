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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.gateway.IndexShardGateway;
import org.elasticsearch.index.gateway.IndexShardGatewayRecoveryException;
import org.elasticsearch.index.gateway.IndexShardGatewaySnapshotFailedException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.SizeUnit;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.io.stream.DataInputStreamInput;
import org.elasticsearch.util.io.stream.DataOutputStreamOutput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.settings.Settings;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.translog.TranslogStreams.*;
import static org.elasticsearch.util.collect.Lists.*;
import static org.elasticsearch.util.io.FileSystemUtils.*;
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

    @Override public void close(boolean delete) {
        if (delete) {
            deleteRecursively(location, true);
        }
    }

    @Override public RecoveryStatus recover() throws IndexShardGatewayRecoveryException {
        RecoveryStatus.Index recoveryStatusIndex = recoverIndex();
        RecoveryStatus.Translog recoveryStatusTranslog = recoverTranslog();
        return new RecoveryStatus(recoveryStatusIndex, recoveryStatusTranslog);
    }

    @Override public SnapshotStatus snapshot(Snapshot snapshot) {
        long totalTimeStart = System.currentTimeMillis();
        boolean indexDirty = false;
        boolean translogDirty = false;

        final SnapshotIndexCommit snapshotIndexCommit = snapshot.indexCommit();
        final Translog.Snapshot translogSnapshot = snapshot.translogSnapshot();

        int indexNumberOfFiles = 0;
        long indexTotalFilesSize = 0;
        long indexTime = 0;
        if (snapshot.indexChanged()) {
            long time = System.currentTimeMillis();
            indexDirty = true;
            // snapshot into the index
            final CountDownLatch latch = new CountDownLatch(snapshotIndexCommit.getFiles().length);
            final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
            for (final String fileName : snapshotIndexCommit.getFiles()) {
                // don't copy over the segments file, it will be copied over later on as part of the
                // final snapshot phase
                if (fileName.equals(snapshotIndexCommit.getSegmentsFileName())) {
                    latch.countDown();
                    continue;
                }
                IndexInput indexInput = null;
                try {
                    indexInput = snapshotIndexCommit.getDirectory().openInput(fileName);
                    File snapshotFile = new File(locationIndex, fileName);
                    if (snapshotFile.exists() && (snapshotFile.length() == indexInput.length())) {
                        // we assume its the same one, no need to copy
                        latch.countDown();
                        continue;
                    }
                } catch (Exception e) {
                    logger.debug("Failed to verify file equality based on length, copying...", e);
                } finally {
                    if (indexInput != null) {
                        try {
                            indexInput.close();
                        } catch (IOException e) {
                            // ignore
                        }
                    }
                }
                indexNumberOfFiles++;
                try {
                    indexTotalFilesSize += snapshotIndexCommit.getDirectory().fileLength(fileName);
                } catch (IOException e) {
                    // ignore...
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
            indexTime = System.currentTimeMillis() - time;
        }
        // we reopen the RAF each snapshot and not keep an open one since we want to make sure we
        // can sync it to disk later on (close it as well) 
        File translogFile = new File(locationTranslog, "translog-" + translogSnapshot.translogId());
        RandomAccessFile translogRaf = null;

        // if we have a different trnaslogId we want to flush the full translog to a new file (based on the translogId).
        // If we still work on existing translog, just append the latest translog operations
        int translogNumberOfOperations = 0;
        long translogTime = 0;
        if (snapshot.newTranslogCreated()) {
            translogDirty = true;
            try {
                long time = System.currentTimeMillis();
                translogRaf = new RandomAccessFile(translogFile, "rw");
                StreamOutput out = new DataOutputStreamOutput(translogRaf);
                out.writeInt(-1); // write the number of operations header with -1 currently
                for (Translog.Operation operation : translogSnapshot) {
                    translogNumberOfOperations++;
                    writeTranslogOperation(out, operation);
                }
                translogTime = System.currentTimeMillis() - time;
            } catch (Exception e) {
                try {
                    translogRaf.close();
                } catch (IOException e1) {
                    // ignore
                }
                throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to snapshot translog into [" + translogFile + "]", e);
            }
        } else if (snapshot.sameTranslogNewOperations()) {
            translogDirty = true;
            try {
                long time = System.currentTimeMillis();
                translogRaf = new RandomAccessFile(translogFile, "rw");
                // seek to the end, since we append
                translogRaf.seek(translogRaf.length());
                StreamOutput out = new DataOutputStreamOutput(translogRaf);
                for (Translog.Operation operation : translogSnapshot.skipTo(snapshot.lastTranslogSize())) {
                    translogNumberOfOperations++;
                    writeTranslogOperation(out, operation);
                }
                translogTime = System.currentTimeMillis() - time;
            } catch (Exception e) {
                try {
                    if (translogRaf != null) {
                        translogRaf.close();
                    }
                } catch (Exception e1) {
                    // ignore
                }
                throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to append snapshot translog into [" + translogFile + "]", e);
            }
        }

        // now write the segments file and update the translog header
        try {
            if (indexDirty) {
                indexNumberOfFiles++;
                indexTotalFilesSize += snapshotIndexCommit.getDirectory().fileLength(snapshotIndexCommit.getSegmentsFileName());
                long time = System.currentTimeMillis();
                copyFromDirectory(snapshotIndexCommit.getDirectory(), snapshotIndexCommit.getSegmentsFileName(),
                        new File(locationIndex, snapshotIndexCommit.getSegmentsFileName()));
                indexTime += (System.currentTimeMillis() - time);
            }
        } catch (Exception e) {
            try {
                if (translogRaf != null) {
                    translogRaf.close();
                }
            } catch (Exception e1) {
                // ignore
            }
            throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to finalize index snapshot into [" + new File(locationIndex, snapshotIndexCommit.getSegmentsFileName()) + "]", e);
        }

        try {
            if (translogDirty) {
                translogRaf.seek(0);
                translogRaf.writeInt(translogSnapshot.size());
                translogRaf.close();

                // now, sync the translog
                syncFile(translogFile);
            }
        } catch (Exception e) {
            if (translogRaf != null) {
                try {
                    translogRaf.close();
                } catch (Exception e1) {
                    // ignore
                }
            }
            throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to finalize snapshot into [" + translogFile + "]", e);
        }

        // delete the old translog
        if (snapshot.newTranslogCreated()) {
            new File(locationTranslog, "translog-" + snapshot.lastTranslogId()).delete();
        }

        // delete files that no longer exists in the index
        if (indexDirty) {
            File[] existingFiles = locationIndex.listFiles();
            for (File existingFile : existingFiles) {
                boolean found = false;
                for (final String fileName : snapshotIndexCommit.getFiles()) {
                    if (existingFile.getName().equals(fileName)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    existingFile.delete();
                }
            }
        }

        return new SnapshotStatus(new TimeValue(System.currentTimeMillis() - totalTimeStart),
                new SnapshotStatus.Index(indexNumberOfFiles, new SizeValue(indexTotalFilesSize), new TimeValue(indexTime)),
                new SnapshotStatus.Translog(translogNumberOfOperations, new TimeValue(translogTime)));
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

        long version = -1;
        try {
            if (IndexReader.indexExists(store.directory())) {
                version = IndexReader.getCurrentVersion(store.directory());
            }
        } catch (IOException e) {
            throw new IndexShardGatewayRecoveryException(shardId(), "Failed to fetch index version after copying it over", e);
        }

        return new RecoveryStatus.Index(version, files.length, new SizeValue(totalSize, SizeUnit.BYTES));
    }

    private RecoveryStatus.Translog recoverTranslog() throws IndexShardGatewayRecoveryException {
        RandomAccessFile raf = null;
        try {
            long recoveryTranslogId = findLatestTranslogId(locationTranslog);
            if (recoveryTranslogId == -1) {
                // no recovery file found, start the shard and bail
                indexShard.start();
                return new RecoveryStatus.Translog(-1, 0, new SizeValue(0, SizeUnit.BYTES));
            }
            File recoveryTranslogFile = new File(locationTranslog, "translog-" + recoveryTranslogId);
            raf = new RandomAccessFile(recoveryTranslogFile, "r");
            int numberOfOperations = raf.readInt();
            ArrayList<Translog.Operation> operations = newArrayListWithExpectedSize(numberOfOperations);
            for (int i = 0; i < numberOfOperations; i++) {
                operations.add(readTranslogOperation(new DataInputStreamInput(raf)));
            }
            indexShard.performRecovery(operations);
            return new RecoveryStatus.Translog(recoveryTranslogId, operations.size(), new SizeValue(recoveryTranslogFile.length(), SizeUnit.BYTES));
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

    private long findLatestTranslogId(File location) {
        File[] files = location.listFiles(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return name.startsWith("translog-");
            }
        });
        if (files == null) {
            return -1;
        }

        long index = -1;
        for (File file : files) {
            String name = file.getName();
            RandomAccessFile raf = null;
            try {
                raf = new RandomAccessFile(file, "r");
                // if header is -1, then its not properly written, ignore it
                if (raf.readInt() == -1) {
                    continue;
                }
            } catch (Exception e) {
                // broken file, continue
                continue;
            } finally {
                try {
                    raf.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            long fileIndex = Long.parseLong(name.substring(name.indexOf('-') + 1));
            if (fileIndex >= index) {
                index = fileIndex;
            }
        }

        return index;
    }
}
