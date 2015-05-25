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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.FlushNotAllowedEngineException;
import org.elasticsearch.index.gateway.IndexShardGateway;
import org.elasticsearch.index.gateway.IndexShardGatewayRecoveryException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.*;
import org.elasticsearch.index.translog.fs.FsTranslog;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class LocalIndexShardGateway extends AbstractIndexShardComponent implements IndexShardGateway {

    public static final String SYNC_INTERVAL = "index.gateway.local.sync";
    private static final int RECOVERY_TRANSLOG_RENAME_RETRIES = 3;

    private final ThreadPool threadPool;
    private final MappingUpdatedAction mappingUpdatedAction;
    private final IndexService indexService;
    private final IndexShard indexShard;

    private final TimeValue waitForMappingUpdatePostRecovery;

    private volatile ScheduledFuture flushScheduler;
    private final TimeValue syncInterval;
    private final CancellableThreads cancellableThreads = new CancellableThreads();

    @Inject
    public LocalIndexShardGateway(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool, MappingUpdatedAction mappingUpdatedAction,
                                  IndexService indexService, IndexShard indexShard) {
        super(shardId, indexSettings);
        this.threadPool = threadPool;
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.indexService = indexService;
        this.indexShard = indexShard;

        this.waitForMappingUpdatePostRecovery = componentSettings.getAsTime("wait_for_mapping_update_post_recovery", TimeValue.timeValueSeconds(30));
        syncInterval = indexSettings.getAsTime(SYNC_INTERVAL, TimeValue.timeValueSeconds(5));
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
    public void recover(boolean indexShouldExists, RecoveryState recoveryState) throws IndexShardGatewayRecoveryException {
        indexShard.prepareForIndexRecovery();
        long version = -1;
        long translogId = -1;
        SegmentInfos si = null;
        final Set<String> typesToUpdate = Sets.newHashSet();
        indexShard.store().incRef();
        try {
            try {
                indexShard.store().failIfCorrupted();
                try {
                    si = indexShard.store().readLastCommittedSegmentsInfo();
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

            // since we recover from local, just fill the files and size
            try {
                final RecoveryState.Index index = recoveryState.getIndex();
                if (si != null) {
                    final Directory directory = indexShard.store().directory();
                    for (String name : Lucene.files(si)) {
                        long length = directory.fileLength(name);
                        index.addFileDetail(name, length, true);
                    }
                }
            } catch (IOException e) {
                logger.debug("failed to list file details", e);
            }

            File recoveringTranslogFile = null;
            if (translogId == -1) {
                logger.trace("no translog id set (indexShouldExist [{}])", indexShouldExists);
            } else {
                try (Lock lock = Lucene.acquireWriteLock(indexShard.store().directory())){
                    // move an existing translog, if exists, to "recovering" state, and start reading from it
                    FsTranslog translog = (FsTranslog) indexShard.translog();
                    String translogName = "translog-" + translogId;
                    String recoverTranslogName = translogName + ".recovering";

                    logger.trace("try recover from translog file {} locations: {}", translogName, Arrays.toString(translog.locations()));
                    for (File translogLocation : translog.locations()) {
                        File tmpRecoveringFile = new File(translogLocation, recoverTranslogName);
                        if (!tmpRecoveringFile.exists()) {
                            File tmpTranslogFile = new File(translogLocation, translogName);
                            if (tmpTranslogFile.exists()) {
                                logger.trace("Translog file found in {} - renaming", translogLocation);
                                boolean success = false;
                                for (int i = 0; i < RECOVERY_TRANSLOG_RENAME_RETRIES; i++) {
                                    if (tmpTranslogFile.renameTo(tmpRecoveringFile)) {

                                        recoveringTranslogFile = tmpRecoveringFile;
                                        logger.trace("Renamed translog from {} to {}", tmpTranslogFile.getName(), recoveringTranslogFile.getName());
                                        success = true;
                                        break;
                                    }
                                }
                                if (success == false) {
                                    try {
                                        // this is a fallback logic that to ensure we can recover from the file.
                                        // on windows a virus-scanner etc can hold on to the file and after retrying
                                        // we just skip the recovery and the engine will reuse the file and truncate it.
                                        // in 2.0 this is all not needed since translog files are write once.
                                        Files.copy(tmpTranslogFile.toPath(), tmpRecoveringFile.toPath());
                                        recoveringTranslogFile = tmpRecoveringFile;
                                        logger.trace("Copied translog from {} to {}", tmpTranslogFile.getName(), recoveringTranslogFile.getName());
                                    } catch (IOException ex) {
                                        throw new ElasticsearchException("failed to copy recovery file", ex);
                                    }
                                }
                            } else {
                                logger.trace("Translog file NOT found in {} - continue", translogLocation);
                            }
                        } else {
                            recoveringTranslogFile = tmpRecoveringFile;
                            break;
                        }
                    }
                }  catch (IOException ex) {
                    throw new ElasticsearchException("failed to obtain write log pre translog recovery", ex);
                }
            }
            // we must do this *after* we capture translog name so the engine creation will not make a new one.
            // also we have to do this regardless of whether we have a translog, to follow the recovery stages.
            indexShard.prepareForTranslogRecovery();

            if (recoveringTranslogFile == null || !recoveringTranslogFile.exists()) {
                // no translog to recovery from, start and bail
                // no translog files, bail
                recoveryState.getTranslog().totalOperations(0);
                recoveryState.getTranslog().totalOperationsOnStart(0);
                indexShard.finalizeRecovery();
                indexShard.postRecovery("post recovery from gateway, no translog");
                // no index, just start the shard and bail
                return;
            }

            StreamInput in = null;
            if (logger.isTraceEnabled()) {
                logger.trace("recovering translog file: {} length: {}", recoveringTranslogFile, recoveringTranslogFile.length());
            }
            try {
                TranslogStream stream = TranslogStreams.translogStreamFor(recoveringTranslogFile);
                try {
                    in = stream.openInput(recoveringTranslogFile);
                } catch (TruncatedTranslogException e) {
                    // file is empty or header has been half-written and should be ignored
                    logger.trace("ignoring truncation exception, the translog is either empty or half-written", e);
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
                    } catch (TruncatedTranslogException|EOFException e) {
                        // ignore, not properly written the last op
                        logger.trace("ignoring translog EOF exception, the last operation was not properly written", e);
                        break;
                    } catch (IOException e) {
                        // ignore, not properly written last op
                        logger.trace("ignoring translog IO exception, the last operation was not properly written", e);
                        break;
                    }
                    try {
                        Engine.IndexingOperation potentialIndexOperation = indexShard.performRecoveryOperation(operation);
                        if (potentialIndexOperation != null && potentialIndexOperation.parsedDoc().mappingsModified()) {
                            if (!typesToUpdate.contains(potentialIndexOperation.docMapper().type())) {
                                typesToUpdate.add(potentialIndexOperation.docMapper().type());
                            }
                        }
                        recoveryState.getTranslog().incrementRecoveredOperations();
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
            // we flush to trim the translog, in case it was big.
            try {
                FlushRequest flushRequest = new FlushRequest();
                flushRequest.force(false);
                flushRequest.waitIfOngoing(false);
                indexShard.flush(flushRequest);
            } catch (FlushNotAllowedEngineException e) {
                // to be safe we catch the FNAEX , at this point no one can recover
                logger.debug("skipping flush at end of recovery (not allowed)", e);
            }
            indexShard.finalizeRecovery();
            indexShard.postRecovery("post recovery from gateway");

            try {
                Files.deleteIfExists(recoveringTranslogFile.toPath());
            } catch (Exception ex) {
                logger.debug("Failed to delete recovering translog file {}", ex, recoveringTranslogFile);
            }
        } finally {
            indexShard.store().decRef();
        }

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
            cancellableThreads.execute(new CancellableThreads.Interruptable() {
                @Override
                public void run() throws InterruptedException {
                    try {
                        boolean waited = latch.await(waitForMappingUpdatePostRecovery.millis(), TimeUnit.MILLISECONDS);
                        if (!waited) {
                            logger.debug("waited for mapping update on master for [{}], yet timed out", type);
                        }
                    } catch (InterruptedException e) {
                        logger.debug("interrupted while waiting for mapping update");
                    }
                }
            });

        }
    }

    @Override
    public String type() {
        return "local";
    }


    @Override
    public void close() {
        FutureUtils.cancel(flushScheduler);
        cancellableThreads.cancel("closed");
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
