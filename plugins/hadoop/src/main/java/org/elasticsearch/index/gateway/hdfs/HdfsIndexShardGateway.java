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

package org.elasticsearch.index.gateway.hdfs;

import org.apache.hadoop.fs.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Directories;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.gateway.IndexGateway;
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
import org.elasticsearch.indices.recovery.throttler.RecoveryThrottler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.SizeUnit;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.io.stream.DataInputStreamInput;
import org.elasticsearch.util.io.stream.DataOutputStreamOutput;
import org.elasticsearch.util.io.stream.StreamOutput;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.lucene.Directories.*;
import static org.elasticsearch.index.translog.TranslogStreams.*;

/**
 * @author kimchy (shay.banon)
 */
public class HdfsIndexShardGateway extends AbstractIndexShardComponent implements IndexShardGateway {

    private final InternalIndexShard indexShard;

    private final ThreadPool threadPool;

    private final RecoveryThrottler recoveryThrottler;

    private final Store store;

    private final FileSystem fileSystem;

    private final Path path;

    private final Path indexPath;

    private final Path translogPath;

    private volatile FSDataOutputStream currentTranslogStream = null;

    @Inject public HdfsIndexShardGateway(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool, IndexGateway hdfsIndexGateway,
                                         IndexShard indexShard, Store store, RecoveryThrottler recoveryThrottler) {
        super(shardId, indexSettings);
        this.indexShard = (InternalIndexShard) indexShard;
        this.threadPool = threadPool;
        this.recoveryThrottler = recoveryThrottler;
        this.store = store;

        this.fileSystem = ((HdfsIndexGateway) hdfsIndexGateway).fileSystem();
        this.path = new Path(((HdfsIndexGateway) hdfsIndexGateway).indexPath(), Integer.toString(shardId.id()));

        this.indexPath = new Path(path, "index");
        this.translogPath = new Path(path, "translog");
    }

    @Override public void close(boolean delete) throws ElasticSearchException {
        if (currentTranslogStream != null) {
            try {
                currentTranslogStream.close();
            } catch (IOException e) {
                // ignore
            }
        }
        if (delete) {
            try {
                fileSystem.delete(path, true);
            } catch (IOException e) {
                logger.warn("Failed to delete [{}]", e, path);
            }
        }
    }

    @Override public boolean requiresSnapshotScheduling() {
        return true;
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
                    FileStatus fileStatus = fileSystem.getFileStatus(new Path(indexPath, fileName));
                    if (fileStatus.getLen() == indexInput.length()) {
                        // we assume its the same one, no need to copy
                        latch.countDown();
                        continue;
                    }
                } catch (FileNotFoundException e) {
                    // that's fine!
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
                        Path copyTo = new Path(indexPath, fileName);
                        FSDataOutputStream fileStream;
                        try {
                            fileStream = fileSystem.create(copyTo, true);
                            copyFromDirectory(snapshotIndexCommit.getDirectory(), fileName, fileStream);
                        } catch (Exception e) {
                            lastException.set(new IndexShardGatewaySnapshotFailedException(shardId, "Failed to copy to [" + copyTo + "], from dir [" + snapshotIndexCommit.getDirectory() + "] and file [" + fileName + "]", e));
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

        int translogNumberOfOperations = 0;
        long translogTime = 0;
        if (snapshot.newTranslogCreated() || currentTranslogStream == null) {
            translogDirty = true;
            long time = System.currentTimeMillis();
            // a new translog, close the current stream
            if (currentTranslogStream != null) {
                try {
                    currentTranslogStream.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            Path currentTranslogPath = new Path(translogPath, "translog-" + translogSnapshot.translogId());
            try {
                currentTranslogStream = fileSystem.create(currentTranslogPath, true);
                StreamOutput out = new DataOutputStreamOutput(currentTranslogStream);
                for (Translog.Operation operation : translogSnapshot) {
                    translogNumberOfOperations++;
                    writeTranslogOperation(out, operation);
                }
                currentTranslogStream.flush();
                currentTranslogStream.sync();
            } catch (Exception e) {
                currentTranslogPath = null;
                if (currentTranslogStream != null) {
                    try {
                        currentTranslogStream.close();
                    } catch (IOException e1) {
                        // ignore
                    } finally {
                        currentTranslogStream = null;
                    }
                }
                throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to snapshot translog into [" + currentTranslogPath + "]", e);
            }
            translogTime = System.currentTimeMillis() - time;
        } else if (snapshot.sameTranslogNewOperations()) {
            translogDirty = true;
            long time = System.currentTimeMillis();
            try {
                StreamOutput out = new DataOutputStreamOutput(currentTranslogStream);
                for (Translog.Operation operation : translogSnapshot.skipTo(snapshot.lastTranslogSize())) {
                    translogNumberOfOperations++;
                    writeTranslogOperation(out, operation);
                }
            } catch (Exception e) {
                try {
                    currentTranslogStream.close();
                } catch (IOException e1) {
                    // ignore
                } finally {
                    currentTranslogStream = null;
                }
            }
            translogTime = System.currentTimeMillis() - time;
        }


        // now write the segments file and update the translog header
        if (indexDirty) {
            Path segmentsPath = new Path(indexPath, snapshotIndexCommit.getSegmentsFileName());
            try {
                indexNumberOfFiles++;
                indexTotalFilesSize += snapshotIndexCommit.getDirectory().fileLength(snapshotIndexCommit.getSegmentsFileName());
                long time = System.currentTimeMillis();
                FSDataOutputStream fileStream;
                fileStream = fileSystem.create(segmentsPath, true);
                copyFromDirectory(snapshotIndexCommit.getDirectory(), snapshotIndexCommit.getSegmentsFileName(), fileStream);
                indexTime += (System.currentTimeMillis() - time);
            } catch (Exception e) {
                throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to finalize index snapshot into [" + segmentsPath + "]", e);
            }
        }

        // delete the old translog
        if (snapshot.newTranslogCreated()) {
            try {
                fileSystem.delete(new Path(translogPath, "translog-" + snapshot.lastTranslogId()), false);
            } catch (IOException e) {
                // ignore
            }
        }

        // delete files that no longer exists in the index
        if (indexDirty) {
            try {
                FileStatus[] existingFiles = fileSystem.listStatus(indexPath);
                if (existingFiles != null) {
                    for (FileStatus existingFile : existingFiles) {
                        boolean found = false;
                        for (final String fileName : snapshotIndexCommit.getFiles()) {
                            if (existingFile.getPath().getName().equals(fileName)) {
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            fileSystem.delete(existingFile.getPath(), false);
                        }
                    }
                }
            } catch (Exception e) {
                // no worries, failed to clean old ones, will clean them later
            }
        }


        return new SnapshotStatus(new TimeValue(System.currentTimeMillis() - totalTimeStart),
                new SnapshotStatus.Index(indexNumberOfFiles, new SizeValue(indexTotalFilesSize), new TimeValue(indexTime)),
                new SnapshotStatus.Translog(translogNumberOfOperations, new TimeValue(translogTime)));
    }

    private RecoveryStatus.Index recoverIndex() throws IndexShardGatewayRecoveryException {
        FileStatus[] files;
        try {
            files = fileSystem.listStatus(indexPath);
        } catch (IOException e) {
            throw new IndexShardGatewayRecoveryException(shardId(), "Failed to list files", e);
        }
        if (files == null || files.length == 0) {
            return new RecoveryStatus.Index(-1, 0, new SizeValue(0, SizeUnit.BYTES), TimeValue.timeValueMillis(0));
        }

        final CountDownLatch latch = new CountDownLatch(files.length);
        final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
        final AtomicLong throttlingWaitTime = new AtomicLong();
        for (final FileStatus file : files) {
            threadPool.execute(new Runnable() {
                @Override public void run() {
                    try {
                        long throttlingStartTime = System.currentTimeMillis();
                        while (!recoveryThrottler.tryStream(shardId, file.getPath().getName())) {
                            Thread.sleep(recoveryThrottler.throttleInterval().millis());
                        }
                        throttlingWaitTime.addAndGet(System.currentTimeMillis() - throttlingStartTime);
                        FSDataInputStream fileStream = fileSystem.open(file.getPath());
                        Directories.copyToDirectory(fileStream, store.directory(), file.getPath().getName());
                    } catch (Exception e) {
                        logger.debug("Failed to read [" + file + "] into [" + store + "]", e);
                        lastException.set(e);
                    } finally {
                        recoveryThrottler.streamDone(shardId, file.getPath().getName());
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
        for (FileStatus file : files) {
            totalSize += file.getLen();
        }

        long version = -1;
        try {
            if (IndexReader.indexExists(store.directory())) {
                version = IndexReader.getCurrentVersion(store.directory());
            }
        } catch (IOException e) {
            throw new IndexShardGatewayRecoveryException(shardId(), "Failed to fetch index version after copying it over", e);
        }

        return new RecoveryStatus.Index(version, files.length, new SizeValue(totalSize, SizeUnit.BYTES), TimeValue.timeValueMillis(throttlingWaitTime.get()));
    }

    private RecoveryStatus.Translog recoverTranslog() throws IndexShardGatewayRecoveryException {
        FSDataInputStream fileStream = null;
        try {
            long recoveryTranslogId = findLatestTranslogId();
            if (recoveryTranslogId == -1) {
                // no recovery file found, start the shard and bail
                indexShard.start();
                return new RecoveryStatus.Translog(-1, 0, new SizeValue(0, SizeUnit.BYTES));
            }
            FileStatus status = fileSystem.getFileStatus(new Path(translogPath, "translog-" + recoveryTranslogId));
            fileStream = fileSystem.open(status.getPath());
            ArrayList<Translog.Operation> operations = Lists.newArrayList();
            for (; ;) {
                try {
                    operations.add(readTranslogOperation(new DataInputStreamInput(fileStream)));
                } catch (EOFException e) {
                    // reached end of stream
                    break;
                }
            }
            indexShard.performRecovery(operations);
            return new RecoveryStatus.Translog(recoveryTranslogId, operations.size(), new SizeValue(status.getLen(), SizeUnit.BYTES));
        } catch (Exception e) {
            throw new IndexShardGatewayRecoveryException(shardId(), "Failed to perform recovery of translog", e);
        } finally {
            if (fileStream != null) {
                try {
                    fileStream.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }


    private long findLatestTranslogId() throws IOException {
        FileStatus[] files = fileSystem.listStatus(translogPath, new PathFilter() {
            @Override public boolean accept(Path path) {
                return path.getName().startsWith("translog-");
            }
        });
        if (files == null) {
            return -1;
        }

        long index = -1;
        for (FileStatus file : files) {
            String name = file.getPath().getName();
            long fileIndex = Long.parseLong(name.substring(name.indexOf('-') + 1));
            if (fileIndex >= index) {
                index = fileIndex;
            }
        }

        return index;
    }
}
