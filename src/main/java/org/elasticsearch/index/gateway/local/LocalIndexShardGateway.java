/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.gateway.local;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.gateway.IndexShardGateway;
import org.elasticsearch.index.gateway.IndexShardGatewayRecoveryException;
import org.elasticsearch.index.gateway.RecoveryStatus;
import org.elasticsearch.index.gateway.SnapshotStatus;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStreams;
import org.elasticsearch.index.translog.fs.FsTranslog;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ScheduledFuture;

/**
 *
 */
public class LocalIndexShardGateway extends AbstractIndexShardComponent implements IndexShardGateway {

    private final ThreadPool threadPool;

    private final InternalIndexShard indexShard;

    private final RecoveryStatus recoveryStatus = new RecoveryStatus();

    private volatile ScheduledFuture flushScheduler;
    private final TimeValue syncInterval;

    @Inject
    public LocalIndexShardGateway(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool, IndexShard indexShard) {
        super(shardId, indexSettings);
        this.threadPool = threadPool;
        this.indexShard = (InternalIndexShard) indexShard;

        syncInterval = componentSettings.getAsTime("sync", TimeValue.timeValueSeconds(5));
        if (syncInterval.millis() > 0) {
            this.indexShard.translog().syncOnEachOperation(false);
            flushScheduler = threadPool.schedule(syncInterval, ThreadPool.Names.SAME, new Sync());
        } else if (syncInterval.millis() == 0) {
            flushScheduler = null;
            this.indexShard.translog().syncOnEachOperation(true);
        } else {
            flushScheduler = null;
        }
    }

    @Override
    public String toString() {
        return "local";
    }

    @Override
    public RecoveryStatus recoveryStatus() {
        return recoveryStatus;
    }

    @Override
    public void recover(boolean indexShouldExists, RecoveryStatus recoveryStatus) throws IndexShardGatewayRecoveryException {
        recoveryStatus.index().startTime(System.currentTimeMillis());
        recoveryStatus.updateStage(RecoveryStatus.Stage.INDEX);
        long version = -1;
        long translogId = -1;
        try {
            SegmentInfos si = null;
            try {
                si = Lucene.readSegmentInfos(indexShard.store().directory());
            } catch (Throwable e) {
                String files = "_unknown_";
                try {
                    files = Arrays.toString(indexShard.store().directory().listAll());
                } catch (Throwable e1) {
                    files += " (failure=" + ExceptionsHelper.detailedMessage(e1) + ")";
                }
                if (indexShouldExists && indexShard.store().indexStore().persistent()) {
                    throw new IndexShardGatewayRecoveryException(shardId(), "shard allocated for local recovery (post api), should exist, but doesn't, current files: " + files, e);
                }
            }
            if (si != null) {
                if (indexShouldExists) {
                    version = si.getVersion();
                    if (si.getUserData().containsKey(Translog.TRANSLOG_ID_KEY)) {
                        translogId = Long.parseLong(si.getUserData().get(Translog.TRANSLOG_ID_KEY));
                    } else {
                        translogId = version;
                    }
                    logger.trace("using existing shard data, translog id [{}]", translogId);
                } else {
                    // it exists on the directory, but shouldn't exist on the FS, its a leftover (possibly dangling)
                    // its a "new index create" API, we have to do something, so better to clean it than use same data
                    logger.trace("cleaning existing shard, shouldn't exists");
                    IndexWriter writer = new IndexWriter(indexShard.store().directory(), new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER).setOpenMode(IndexWriterConfig.OpenMode.CREATE));
                    writer.close();
                }
            }
        } catch (Throwable e) {
            throw new IndexShardGatewayRecoveryException(shardId(), "failed to fetch index version after copying it over", e);
        }
        recoveryStatus.index().updateVersion(version);
        recoveryStatus.index().time(System.currentTimeMillis() - recoveryStatus.index().startTime());

        // since we recover from local, just fill the files and size
        try {
            int numberOfFiles = 0;
            long totalSizeInBytes = 0;
            for (String name : indexShard.store().directory().listAll()) {
                numberOfFiles++;
                totalSizeInBytes += indexShard.store().directory().fileLength(name);
            }
            recoveryStatus.index().files(numberOfFiles, totalSizeInBytes, numberOfFiles, totalSizeInBytes);
        } catch (Exception e) {
            // ignore
        }

        recoveryStatus.start().startTime(System.currentTimeMillis());
        recoveryStatus.updateStage(RecoveryStatus.Stage.START);
        if (translogId == -1) {
            // no translog files, bail
            indexShard.postRecovery("post recovery from gateway, no translog");
            // no index, just start the shard and bail
            recoveryStatus.start().time(System.currentTimeMillis() - recoveryStatus.start().startTime());
            recoveryStatus.start().checkIndexTime(indexShard.checkIndexTook());
            return;
        }

        // move an existing translog, if exists, to "recovering" state, and start reading from it
        FsTranslog translog = (FsTranslog) indexShard.translog();
        String translogName = "translog-" + translogId;
        String recoverTranslogName = translogName + ".recovering";


        File recoveringTranslogFile = null;
        for (File translogLocation : translog.locations()) {
            File tmpRecoveringFile = new File(translogLocation, recoverTranslogName);
            if (!tmpRecoveringFile.exists()) {
                File tmpTranslogFile = new File(translogLocation, translogName);
                if (tmpTranslogFile.exists()) {
                    for (int i = 0; i < 3; i++) {
                        if (tmpTranslogFile.renameTo(tmpRecoveringFile)) {
                            recoveringTranslogFile = tmpRecoveringFile;
                            break;
                        }
                    }
                }
            } else {
                recoveringTranslogFile = tmpRecoveringFile;
                break;
            }
        }

        if (recoveringTranslogFile == null || !recoveringTranslogFile.exists()) {
            // no translog to recovery from, start and bail
            // no translog files, bail
            indexShard.postRecovery("post recovery from gateway, no translog");
            // no index, just start the shard and bail
            recoveryStatus.start().time(System.currentTimeMillis() - recoveryStatus.start().startTime());
            recoveryStatus.start().checkIndexTime(indexShard.checkIndexTook());
            return;
        }

        // recover from the translog file
        indexShard.performRecoveryPrepareForTranslog();
        recoveryStatus.start().time(System.currentTimeMillis() - recoveryStatus.start().startTime());
        recoveryStatus.start().checkIndexTime(indexShard.checkIndexTook());

        recoveryStatus.translog().startTime(System.currentTimeMillis());
        recoveryStatus.updateStage(RecoveryStatus.Stage.TRANSLOG);
        FileInputStream fs = null;
        try {
            fs = new FileInputStream(recoveringTranslogFile);
            InputStreamStreamInput si = new InputStreamStreamInput(fs);
            while (true) {
                Translog.Operation operation;
                try {
                    int opSize = si.readInt();
                    operation = TranslogStreams.readTranslogOperation(si);
                } catch (EOFException e) {
                    // ignore, not properly written the last op
                    break;
                } catch (IOException e) {
                    // ignore, not properly written last op
                    break;
                }
                try {
                    indexShard.performRecoveryOperation(operation);
                    recoveryStatus.translog().addTranslogOperations(1);
                } catch (ElasticSearchException e) {
                    if (e.status() == RestStatus.BAD_REQUEST) {
                        // mainly for MapperParsingException and Failure to detect xcontent
                        logger.info("ignoring recovery of a corrupt translog entry", e);
                    } else {
                        throw e;
                    }
                }
            }
        } catch (Throwable e) {
            // we failed to recovery, make sure to delete the translog file (and keep the recovering one)
            indexShard.translog().closeWithDelete();
            throw new IndexShardGatewayRecoveryException(shardId, "failed to recover shard", e);
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                // ignore
            }
        }
        indexShard.performRecoveryFinalization(true);

        recoveringTranslogFile.delete();

        recoveryStatus.translog().time(System.currentTimeMillis() - recoveryStatus.translog().startTime());
    }

    @Override
    public String type() {
        return "local";
    }

    @Override
    public SnapshotStatus snapshot(Snapshot snapshot) {
        return null;
    }

    @Override
    public SnapshotStatus lastSnapshotStatus() {
        return null;
    }

    @Override
    public SnapshotStatus currentSnapshotStatus() {
        return null;
    }

    @Override
    public boolean requiresSnapshot() {
        return false;
    }

    @Override
    public boolean requiresSnapshotScheduling() {
        return false;
    }

    @Override
    public void close() {
        if (flushScheduler != null) {
            flushScheduler.cancel(false);
        }
    }

    @Override
    public SnapshotLock obtainSnapshotLock() throws Exception {
        return NO_SNAPSHOT_LOCK;
    }

    class Sync implements Runnable {
        @Override
        public void run() {
            // don't re-schedule  if its closed..., we are done
            if (indexShard.state() == IndexShardState.CLOSED) {
                return;
            }
            if (indexShard.state() == IndexShardState.STARTED && indexShard.translog().syncNeeded()) {
                threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            indexShard.translog().sync();
                        } catch (Exception e) {
                            if (indexShard.state() == IndexShardState.STARTED) {
                                logger.warn("failed to sync translog", e);
                            }
                        }
                        if (indexShard.state() != IndexShardState.CLOSED) {
                            flushScheduler = threadPool.schedule(syncInterval, ThreadPool.Names.SAME, Sync.this);
                        }
                    }
                });
            } else {
                flushScheduler = threadPool.schedule(syncInterval, ThreadPool.Names.SAME, Sync.this);
            }
        }
    }
}
