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

package org.elasticsearch.index.gateway.local;

import com.google.common.collect.Sets;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.gateway.IndexShardGateway;
import org.elasticsearch.index.gateway.IndexShardGatewayRecoveryException;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.translog.*;
import org.elasticsearch.index.translog.fs.FsTranslog;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class LocalIndexShardGateway extends AbstractIndexShardComponent implements IndexShardGateway {

    private static final int RECOVERY_TRANSLOG_RENAME_RETRIES = 3;

    private final ThreadPool threadPool;
    private final MappingUpdatedAction mappingUpdatedAction;
    private final IndexService indexService;
    private final InternalIndexShard indexShard;

    private final TimeValue waitForMappingUpdatePostRecovery;

    private final RecoveryState recoveryState = new RecoveryState();

    private volatile ScheduledFuture flushScheduler;
    private final TimeValue syncInterval;

    @Inject
    public LocalIndexShardGateway(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool, MappingUpdatedAction mappingUpdatedAction,
                                  IndexService indexService, IndexShard indexShard) {
        super(shardId, indexSettings);
        this.threadPool = threadPool;
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.indexService = indexService;
        this.indexShard = (InternalIndexShard) indexShard;

        this.waitForMappingUpdatePostRecovery = componentSettings.getAsTime("wait_for_mapping_update_post_recovery", TimeValue.timeValueSeconds(30));
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
    public RecoveryState recoveryState() {
        return recoveryState;
    }

    @Override
    public void recover(boolean indexShouldExists, RecoveryState recoveryState) throws IndexShardGatewayRecoveryException {
        recoveryState.getIndex().startTime(System.currentTimeMillis());
        recoveryState.setStage(RecoveryState.Stage.INDEX);
        long version = -1;
        long translogId = -1;
        indexShard.store().incRef();
        try {
            try {
                indexShard.store().failIfCorrupted();
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
                    if (indexShouldExists && indexShard.indexService().store().persistent()) {
                        throw new IndexShardGatewayRecoveryException(shardId(), "shard allocated for local recovery (post api), should exist, but doesn't, current files: " + files, e);
                    }
                }
                if (si != null) {
                    if (indexShouldExists) {
                        version = si.getVersion();
                        /**
                         * We generate the translog ID before each lucene commit to ensure that
                         * we can read the current translog ID safely when we recover. The commits metadata
                         * therefor contains always the current / active translog ID.
                         */
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
            recoveryState.getIndex().updateVersion(version);
            recoveryState.getIndex().time(System.currentTimeMillis() - recoveryState.getIndex().startTime());

            // since we recover from local, just fill the files and size
            try {
                int numberOfFiles = 0;
                long totalSizeInBytes = 0;
                for (String name : indexShard.store().directory().listAll()) {
                    numberOfFiles++;
                    long length =  indexShard.store().directory().fileLength(name);
                    totalSizeInBytes += length;
                    recoveryState.getIndex().addFileDetail(name, length, length);
                }
                RecoveryState.Index index = recoveryState.getIndex();
                index.totalFileCount(numberOfFiles);
                index.totalByteCount(totalSizeInBytes);
                index.reusedFileCount(numberOfFiles);
                index.reusedByteCount(totalSizeInBytes);
                index.recoveredFileCount(numberOfFiles);
                index.recoveredByteCount(totalSizeInBytes);
            } catch (Exception e) {
                // ignore
            }

            recoveryState.getStart().startTime(System.currentTimeMillis());
            recoveryState.setStage(RecoveryState.Stage.START);
            if (translogId == -1) {
                // no translog files, bail
                indexShard.postRecovery("post recovery from gateway, no translog");
                // no index, just start the shard and bail
                recoveryState.getStart().time(System.currentTimeMillis() - recoveryState.getStart().startTime());
                recoveryState.getStart().checkIndexTime(indexShard.checkIndexTook());
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
                        for (int i = 0; i < RECOVERY_TRANSLOG_RENAME_RETRIES; i++) {
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
                recoveryState.getStart().time(System.currentTimeMillis() - recoveryState.getStart().startTime());
                recoveryState.getStart().checkIndexTime(indexShard.checkIndexTook());
                return;
            }

            // recover from the translog file
            indexShard.performRecoveryPrepareForTranslog();
            recoveryState.getStart().time(System.currentTimeMillis() - recoveryState.getStart().startTime());
            recoveryState.getStart().checkIndexTime(indexShard.checkIndexTook());

            recoveryState.getTranslog().startTime(System.currentTimeMillis());
            recoveryState.setStage(RecoveryState.Stage.TRANSLOG);
            StreamInput in = null;

            final Set<String> typesToUpdate = Sets.newHashSet();
            try {
                TranslogStream stream = TranslogStreams.translogStreamFor(recoveringTranslogFile);
                try {
                    in = stream.openInput(recoveringTranslogFile);
                } catch (TruncatedTranslogException e) {
                    // file is empty or header has been half-written and should be ignored
                    logger.trace("ignoring truncation exception, the translog is either empty or half-written ([{}])", e.getMessage());
                }
                while (true) {
                    if (in == null) {
                        break;
                    }
                    Translog.Operation operation;
                    try {
                        if (stream instanceof LegacyTranslogStream) {
                            in.readInt(); // ignored opSize
                        }
                        operation = stream.read(in);
                    } catch (EOFException e) {
                        // ignore, not properly written the last op
                        logger.trace("ignoring translog EOF exception, the last operation was not properly written ([{}])", e.getMessage());
                        break;
                    } catch (IOException e) {
                        // ignore, not properly written last op
                        logger.trace("ignoring translog IO exception, the last operation was not properly written ([{}])", e.getMessage());
                        break;
                    }
                    try {
                        Engine.IndexingOperation potentialIndexOperation = indexShard.performRecoveryOperation(operation);
                        if (potentialIndexOperation != null && potentialIndexOperation.parsedDoc().mappingsModified()) {
                            if (!typesToUpdate.contains(potentialIndexOperation.docMapper().type())) {
                                typesToUpdate.add(potentialIndexOperation.docMapper().type());
                            }
                        }
                        recoveryState.getTranslog().addTranslogOperations(1);
                    } catch (ElasticsearchException e) {
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
                IOUtils.closeWhileHandlingException(in);
            }
            indexShard.performRecoveryFinalization(true);

            recoveringTranslogFile.delete();

            for (final String type : typesToUpdate) {
                final CountDownLatch latch = new CountDownLatch(1);
                mappingUpdatedAction.updateMappingOnMaster(indexService.index().name(), indexService.mapperService().documentMapper(type), indexService.indexUUID(), new MappingUpdatedAction.MappingUpdateListener() {
                    @Override
                    public void onMappingUpdate() {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        latch.countDown();
                        logger.debug("failed to send mapping update post recovery to master for [{}]", t, type);
                    }
                });

                try {
                    boolean waited = latch.await(waitForMappingUpdatePostRecovery.millis(), TimeUnit.MILLISECONDS);
                    if (!waited) {
                        logger.debug("waited for mapping update on master for [{}], yet timed out");
                    }
                } catch (InterruptedException e) {
                    logger.debug("interrupted while waiting for mapping update");
                }
            }
        } finally {
            indexShard.store().decRef();
        }

        recoveryState.getTranslog().time(System.currentTimeMillis() - recoveryState.getTranslog().startTime());
    }

    @Override
    public String type() {
        return "local";
    }


    @Override
    public void close() {
        if (flushScheduler != null) {
            flushScheduler.cancel(false);
        }
    }

    class Sync implements Runnable {
        @Override
        public void run() {
            // don't re-schedule  if its closed..., we are done
            if (indexShard.state() == IndexShardState.CLOSED) {
                return;
            }
            if (indexShard.state() == IndexShardState.STARTED && indexShard.translog().syncNeeded()) {
                threadPool.executor(ThreadPool.Names.FLUSH).execute(new Runnable() {
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
