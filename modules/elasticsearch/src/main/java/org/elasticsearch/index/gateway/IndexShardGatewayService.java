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

package org.elasticsearch.index.gateway;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.component.CloseableIndexComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.throttler.RecoveryThrottler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author kimchy (shay.banon)
 */
public class IndexShardGatewayService extends AbstractIndexShardComponent implements CloseableIndexComponent {

    private final boolean snapshotOnClose;

    private final ThreadPool threadPool;

    private final InternalIndexShard indexShard;

    private final IndexShardGateway shardGateway;

    private final Store store;

    private final RecoveryThrottler recoveryThrottler;


    private volatile long lastIndexVersion;

    private volatile long lastTranslogId = -1;

    private volatile int lastTranslogSize;

    private final AtomicBoolean recovered = new AtomicBoolean();

    private final TimeValue snapshotInterval;

    private volatile ScheduledFuture snapshotScheduleFuture;

    @Inject public IndexShardGatewayService(ShardId shardId, @IndexSettings Settings indexSettings,
                                            ThreadPool threadPool, IndexShard indexShard, IndexShardGateway shardGateway,
                                            Store store, RecoveryThrottler recoveryThrottler) {
        super(shardId, indexSettings);
        this.threadPool = threadPool;
        this.indexShard = (InternalIndexShard) indexShard;
        this.shardGateway = shardGateway;
        this.store = store;
        this.recoveryThrottler = recoveryThrottler;

        this.snapshotOnClose = componentSettings.getAsBoolean("snapshot_on_close", true);
        this.snapshotInterval = componentSettings.getAsTime("snapshot_interval", TimeValue.timeValueSeconds(10));
    }

    /**
     * Should be called when the shard routing state has changed (note, after the state has been set on the shard).
     */
    public void routingStateChanged() {
        scheduleSnapshotIfNeeded();
    }

    /**
     * Recovers the state of the shard from the gateway.
     */
    public synchronized void recover() throws IndexShardGatewayRecoveryException, IgnoreGatewayRecoveryException {
        if (recovered.compareAndSet(false, true)) {
            if (!indexShard.routingEntry().primary()) {
                throw new ElasticSearchIllegalStateException("Trying to recover when the shard is in backup state");
            }

            indexShard.recovering();

            StopWatch throttlingWaitTime = new StopWatch().start();
            // we know we are on a thread, we can spin till we can engage in recovery
            while (!recoveryThrottler.tryRecovery(shardId, "gateway")) {
                try {
                    Thread.sleep(recoveryThrottler.throttleInterval().millis());
                } catch (InterruptedException e) {
                    if (indexShard.ignoreRecoveryAttempt()) {
                        throw new IgnoreGatewayRecoveryException(shardId, "Interrupted while waiting for recovery, but we should ignore ...");
                    }
                    // we got interrupted, mark it as failed
                    throw new IndexShardGatewayRecoveryException(shardId, "Interrupted while waiting to recovery", e);
                }
            }
            throttlingWaitTime.stop();

            try {
                logger.debug("starting recovery from {}", shardGateway);
                StopWatch stopWatch = new StopWatch().start();
                IndexShardGateway.RecoveryStatus recoveryStatus = shardGateway.recover();

                lastIndexVersion = recoveryStatus.index().version();
                lastTranslogId = recoveryStatus.translog().translogId();
                lastTranslogSize = recoveryStatus.translog().numberOfOperations();

                // start the shard if the gateway has not started it already
                if (indexShard.state() != IndexShardState.STARTED) {
                    indexShard.start();
                }
                stopWatch.stop();
                if (logger.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("recovery completed from ").append(shardGateway).append(", took [").append(stopWatch.totalTime()).append("], throttling_wait [").append(throttlingWaitTime.totalTime()).append("]\n");
                    sb.append("    index    : number_of_files [").append(recoveryStatus.index().numberOfFiles()).append("] with total_size [").append(recoveryStatus.index().totalSize()).append("], throttling_wait [").append(recoveryStatus.index().throttlingWaitTime()).append("]\n");
                    sb.append("             : reusing_files [").append(recoveryStatus.index().numberOfExistingFiles()).append("] with total_size [").append(recoveryStatus.index().existingTotalSize()).append("]\n");
                    sb.append("    translog : translog_id [").append(recoveryStatus.translog().translogId()).append("], number_of_operations [").append(recoveryStatus.translog().numberOfOperations()).append("] with total_size[").append(recoveryStatus.translog().totalSize()).append("]");
                    logger.debug(sb.toString());
                }
                // refresh the shard
                indexShard.refresh(new Engine.Refresh(false));
                scheduleSnapshotIfNeeded();
            } finally {
                recoveryThrottler.recoveryDone(shardId, "gateway");
            }
        } else {
            throw new IgnoreGatewayRecoveryException(shardId, "Already recovered");
        }
    }

    /**
     * Snapshots the given shard into the gateway.
     */
    public synchronized void snapshot() throws IndexShardGatewaySnapshotFailedException {
        if (!indexShard.routingEntry().primary()) {
            return;
//            throw new IndexShardGatewaySnapshotNotAllowedException(shardId, "Snapshot not allowed on non primary shard");
        }
        if (indexShard.routingEntry().relocating()) {
            // do not snapshot when in the process of relocation of primaries so we won't get conflicts
            return;
        }
        try {
            IndexShardGateway.SnapshotStatus snapshotStatus = indexShard.snapshot(new Engine.SnapshotHandler<IndexShardGateway.SnapshotStatus>() {
                @Override public IndexShardGateway.SnapshotStatus snapshot(SnapshotIndexCommit snapshotIndexCommit, Translog.Snapshot translogSnapshot) throws EngineException {
                    if (lastIndexVersion != snapshotIndexCommit.getVersion() || lastTranslogId != translogSnapshot.translogId() || lastTranslogSize != translogSnapshot.size()) {

                        IndexShardGateway.SnapshotStatus snapshotStatus =
                                shardGateway.snapshot(new IndexShardGateway.Snapshot(snapshotIndexCommit, translogSnapshot, lastIndexVersion, lastTranslogId, lastTranslogSize));

                        lastIndexVersion = snapshotIndexCommit.getVersion();
                        lastTranslogId = translogSnapshot.translogId();
                        lastTranslogSize = translogSnapshot.size();
                        return snapshotStatus;
                    }
                    return IndexShardGateway.SnapshotStatus.NA;
                }
            });
            if (snapshotStatus != IndexShardGateway.SnapshotStatus.NA) {
                if (logger.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("snapshot completed to ").append(shardGateway).append(", took [").append(snapshotStatus.totalTime()).append("]\n");
                    sb.append("    index    : number_of_files [").append(snapshotStatus.index().numberOfFiles()).append("] with total_size [").append(snapshotStatus.index().totalSize()).append("], took [").append(snapshotStatus.index().time()).append("]\n");
                    sb.append("    translog : number_of_operations [").append(snapshotStatus.translog().numberOfOperations()).append("], took [").append(snapshotStatus.translog().time()).append("]");
                    logger.debug(sb.toString());
                }
            }
        } catch (IllegalIndexShardStateException e) {
            // ignore, that's fine
        } catch (IndexShardGatewaySnapshotFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new IndexShardGatewaySnapshotFailedException(shardId, "Failed to snapshot", e);
        }
    }

    public synchronized void close(boolean delete) {
        if (snapshotScheduleFuture != null) {
            snapshotScheduleFuture.cancel(true);
            snapshotScheduleFuture = null;
        }
        if (!delete && snapshotOnClose) {
            logger.debug("snapshotting on close ...");
            try {
                snapshot();
            } catch (Exception e) {
                logger.warn("failed to snapshot on close", e);
            }
        }
        // don't really delete the shard gateway if we are *not* primary,
        // the primary will close it
        if (!indexShard.routingEntry().primary()) {
            delete = false;
        }
        shardGateway.close(delete);
    }

    private synchronized void scheduleSnapshotIfNeeded() {
        if (!shardGateway.requiresSnapshotScheduling()) {
            return;
        }
        if (!indexShard.routingEntry().primary()) {
            // we only do snapshotting on the primary shard
            return;
        }
        if (!indexShard.routingEntry().started()) {
            // we only schedule when the cluster assumes we have started
            return;
        }
        if (snapshotScheduleFuture != null) {
            // we are already scheduling this one, ignore
            return;
        }
        if (snapshotInterval.millis() != -1) {
            // we need to schedule snapshot
            if (logger.isDebugEnabled()) {
                logger.debug("scheduling snapshot every [{}]", snapshotInterval);
            }
            snapshotScheduleFuture = threadPool.scheduleWithFixedDelay(new SnapshotRunnable(), snapshotInterval);
        }
    }

    private class SnapshotRunnable implements Runnable {
        @Override public void run() {
            try {
                snapshot();
            } catch (Exception e) {
                logger.warn("failed to snapshot (scheduled)", e);
            }
        }
    }
}
