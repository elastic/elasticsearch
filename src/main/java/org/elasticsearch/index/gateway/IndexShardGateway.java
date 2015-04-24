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

package org.elasticsearch.index.gateway;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class IndexShardGateway extends AbstractIndexShardComponent implements Closeable {

    private final ThreadPool threadPool;
    private final MappingUpdatedAction mappingUpdatedAction;
    private final IndexService indexService;
    private final IndexShard indexShard;
    private final TimeValue waitForMappingUpdatePostRecovery;
    private final TimeValue syncInterval;

    private volatile ScheduledFuture<?> flushScheduler;
    private final CancellableThreads cancellableThreads = new CancellableThreads();


    @Inject
    public IndexShardGateway(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool, MappingUpdatedAction mappingUpdatedAction,
                             IndexService indexService, IndexShard indexShard) {
        super(shardId, indexSettings);
        this.threadPool = threadPool;
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.indexService = indexService;
        this.indexShard = indexShard;

        this.waitForMappingUpdatePostRecovery = indexSettings.getAsTime("index.gateway.wait_for_mapping_update_post_recovery", TimeValue.timeValueMinutes(15));
        syncInterval = indexSettings.getAsTime("index.gateway.sync", TimeValue.timeValueSeconds(5));
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

    /**
     * Recovers the state of the shard from the gateway.
     */
    public void recover(boolean indexShouldExists, RecoveryState recoveryState) throws IndexShardGatewayRecoveryException {
        indexShard.prepareForIndexRecovery();
        long version = -1;
        final Map<String, Mapping> typesToUpdate;
        SegmentInfos si = null;
        indexShard.store().incRef();
        try {
            try {
                indexShard.store().failIfCorrupted();
                try {
                    si = Lucene.readSegmentInfos(indexShard.store().directory());
                } catch (Throwable e) {
                    String files = "_unknown_";
                    try {
                        files = Arrays.toString(indexShard.store().directory().listAll());
                    } catch (Throwable e1) {
                        files += " (failure=" + ExceptionsHelper.detailedMessage(e1) + ")";
                    }
                    if (indexShouldExists) {
                        throw new IndexShardGatewayRecoveryException(shardId(), "shard allocated for local recovery (post api), should exist, but doesn't, current files: " + files, e);
                    }
                }
                if (si != null) {
                    if (indexShouldExists) {
                        version = si.getVersion();
                    } else {
                        // it exists on the directory, but shouldn't exist on the FS, its a leftover (possibly dangling)
                        // its a "new index create" API, we have to do something, so better to clean it than use same data
                        logger.trace("cleaning existing shard, shouldn't exists");
                        IndexWriter writer = new IndexWriter(indexShard.store().directory(), new IndexWriterConfig(Lucene.STANDARD_ANALYZER).setOpenMode(IndexWriterConfig.OpenMode.CREATE));
                        writer.close();
                        recoveryState.getTranslog().totalOperations(0);
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
            if (indexShouldExists == false) {
                recoveryState.getTranslog().totalOperations(0);
                recoveryState.getTranslog().totalOperationsOnStart(0);
            }
            typesToUpdate = indexShard.performTranslogRecovery();

            indexShard.finalizeRecovery();
            for (Map.Entry<String, Mapping> entry : typesToUpdate.entrySet()) {
                validateMappingUpdate(entry.getKey(), entry.getValue());
            }
            indexShard.postRecovery("post recovery from gateway");
        } catch (EngineException e) {
            throw new IndexShardGatewayRecoveryException(shardId, "failed to recovery from gateway", e);
        } finally {
            indexShard.store().decRef();
        }
    }

    private void validateMappingUpdate(final String type, Mapping update) {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        mappingUpdatedAction.updateMappingOnMaster(indexService.index().name(), type, update, waitForMappingUpdatePostRecovery, new MappingUpdatedAction.MappingUpdateListener() {
            @Override
            public void onMappingUpdate() {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                latch.countDown();
                error.set(t);
            }
        });
        cancellableThreads.execute(new CancellableThreads.Interruptable() {
            @Override
            public void run() throws InterruptedException {
                try {
                    if (latch.await(waitForMappingUpdatePostRecovery.millis(), TimeUnit.MILLISECONDS) == false) {
                        logger.debug("waited for mapping update on master for [{}], yet timed out", type);
                    } else {
                        if (error.get() != null) {
                            throw new IndexShardGatewayRecoveryException(shardId, "Failed to propagate mappings on master post recovery", error.get());
                        }
                    }
                } catch (InterruptedException e) {
                    logger.debug("interrupted while waiting for mapping update");
                    throw e;
                }
            }
        });
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

    @Override
    public String toString() {
        return "shard_gateway";
    }
}
