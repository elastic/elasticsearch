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

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.CloseableIndexComponent;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.SnapshotFailedEngineException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.*;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.snapshots.IndexShardSnapshotAndRestoreService;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ScheduledFuture;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

/**
 *
 */
public class IndexShardGatewayService extends AbstractIndexShardComponent implements CloseableIndexComponent {

    private final boolean snapshotOnClose;

    private final ThreadPool threadPool;

    private final IndexSettingsService indexSettingsService;

    private final ClusterService clusterService;

    private final InternalIndexShard indexShard;

    private final IndexShardGateway shardGateway;

    private final IndexShardSnapshotAndRestoreService snapshotService;


    private volatile long lastIndexVersion;

    private volatile long lastTranslogId = -1;

    private volatile int lastTotalTranslogOperations;

    private volatile long lastTranslogLength;

    private volatile TimeValue snapshotInterval;

    private volatile ScheduledFuture snapshotScheduleFuture;

    private RecoveryState recoveryState;

    private IndexShardGateway.SnapshotLock snapshotLock;

    private final SnapshotRunnable snapshotRunnable = new SnapshotRunnable();

    private final ApplySettings applySettings = new ApplySettings();


    @Inject
    public IndexShardGatewayService(ShardId shardId, @IndexSettings Settings indexSettings, IndexSettingsService indexSettingsService,
                                    ThreadPool threadPool, IndexShard indexShard, IndexShardGateway shardGateway, IndexShardSnapshotAndRestoreService snapshotService,
                                    ClusterService clusterService) {
        super(shardId, indexSettings);
        this.threadPool = threadPool;
        this.indexSettingsService = indexSettingsService;
        this.indexShard = (InternalIndexShard) indexShard;
        this.shardGateway = shardGateway;
        this.snapshotService = snapshotService;
        this.recoveryState = new RecoveryState(shardId);
        this.clusterService = clusterService;

        this.snapshotOnClose = componentSettings.getAsBoolean("snapshot_on_close", true);
        this.snapshotInterval = componentSettings.getAsTime("snapshot_interval", TimeValue.timeValueSeconds(10));

        indexSettingsService.addListener(applySettings);
    }

    public static final String INDEX_GATEWAY_SNAPSHOT_INTERVAL = "index.gateway.snapshot_interval";

    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            TimeValue snapshotInterval = settings.getAsTime(INDEX_GATEWAY_SNAPSHOT_INTERVAL, IndexShardGatewayService.this.snapshotInterval);
            if (!snapshotInterval.equals(IndexShardGatewayService.this.snapshotInterval)) {
                logger.info("updating snapshot_interval from [{}] to [{}]", IndexShardGatewayService.this.snapshotInterval, snapshotInterval);
                IndexShardGatewayService.this.snapshotInterval = snapshotInterval;
                if (snapshotScheduleFuture != null) {
                    snapshotScheduleFuture.cancel(false);
                    snapshotScheduleFuture = null;
                }
                scheduleSnapshotIfNeeded();
            }
        }
    }

    /**
     * Should be called when the shard routing state has changed (note, after the state has been set on the shard).
     */
    public void routingStateChanged() {
        scheduleSnapshotIfNeeded();
    }

    public static interface RecoveryListener {
        void onRecoveryDone();

        void onIgnoreRecovery(String reason);

        void onRecoveryFailed(IndexShardGatewayRecoveryException e);
    }

    public RecoveryState recoveryState() {
        if (recoveryState.getTimer().startTime() > 0 && recoveryState.getStage() != RecoveryState.Stage.DONE) {
            recoveryState.getTimer().time(System.currentTimeMillis() - recoveryState.getTimer().startTime());
        }
        return recoveryState;
    }

    public SnapshotStatus snapshotStatus() {
        SnapshotStatus snapshotStatus = shardGateway.currentSnapshotStatus();
        if (snapshotStatus != null) {
            return snapshotStatus;
        }
        return shardGateway.lastSnapshotStatus();
    }

    /**
     * Recovers the state of the shard from the gateway.
     */
    public void recover(final boolean indexShouldExists, final RecoveryListener listener) throws IndexShardGatewayRecoveryException, IgnoreGatewayRecoveryException {
        if (indexShard.state() == IndexShardState.CLOSED) {
            // got closed on us, just ignore this recovery
            listener.onIgnoreRecovery("shard closed");
            return;
        }
        if (!indexShard.routingEntry().primary()) {
            listener.onRecoveryFailed(new IndexShardGatewayRecoveryException(shardId, "Trying to recover when the shard is in backup state", null));
            return;
        }
        try {
            if (indexShard.routingEntry().restoreSource() != null) {
                indexShard.recovering("from snapshot");
            } else {
                indexShard.recovering("from gateway");
            }
        } catch (IllegalIndexShardStateException e) {
            // that's fine, since we might be called concurrently, just ignore this, we are already recovering
            listener.onIgnoreRecovery("already in recovering process, " + e.getMessage());
            return;
        }

        threadPool.generic().execute(new Runnable() {
            @Override
            public void run() {
                recoveryState.getTimer().startTime(System.currentTimeMillis());
                recoveryState.setTargetNode(clusterService.localNode());
                recoveryState.setStage(RecoveryState.Stage.INIT);
                recoveryState.setPrimary(indexShard.routingEntry().primary());

                try {
                    if (indexShard.routingEntry().restoreSource() != null) {
                        logger.debug("restoring from {} ...", indexShard.routingEntry().restoreSource());
                        recoveryState.setType(RecoveryState.Type.SNAPSHOT);
                        recoveryState.setRestoreSource(indexShard.routingEntry().restoreSource());
                        snapshotService.restore(recoveryState);
                    } else {
                        logger.debug("starting recovery from {} ...", shardGateway);
                        recoveryState.setType(RecoveryState.Type.GATEWAY);
                        recoveryState.setSourceNode(clusterService.localNode());
                        shardGateway.recover(indexShouldExists, recoveryState);
                    }

                    lastIndexVersion = recoveryState.getIndex().version();
                    lastTranslogId = -1;
                    lastTranslogLength = 0;
                    lastTotalTranslogOperations = recoveryState.getTranslog().currentTranslogOperations();

                    // start the shard if the gateway has not started it already. Note that if the gateway
                    // moved shard to POST_RECOVERY, it may have been started as well if:
                    // 1) master sent a new cluster state indicating shard is initializing
                    // 2) IndicesClusterStateService#applyInitializingShard will send a shard started event
                    // 3) Master will mark shard as started and this will be processed locally.
                    IndexShardState shardState = indexShard.state();
                    if (shardState != IndexShardState.POST_RECOVERY && shardState != IndexShardState.STARTED) {
                        indexShard.postRecovery("post recovery from gateway");
                    }
                    // refresh the shard
                    indexShard.refresh(new Engine.Refresh("post_gateway").force(true));

                    recoveryState.getTimer().time(System.currentTimeMillis() - recoveryState.getTimer().startTime());
                    recoveryState.setStage(RecoveryState.Stage.DONE);

                    if (logger.isTraceEnabled()) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("recovery completed from ").append(shardGateway).append(", took [").append(timeValueMillis(recoveryState.getTimer().time())).append("]\n");
                        sb.append("    index    : files           [").append(recoveryState.getIndex().totalFileCount()).append("] with total_size [").append(new ByteSizeValue(recoveryState.getIndex().totalByteCount())).append("], took[").append(TimeValue.timeValueMillis(recoveryState.getIndex().time())).append("]\n");
                        sb.append("             : recovered_files [").append(recoveryState.getIndex().numberOfRecoveredFiles()).append("] with total_size [").append(new ByteSizeValue(recoveryState.getIndex().recoveredTotalSize())).append("]\n");
                        sb.append("             : reusing_files   [").append(recoveryState.getIndex().reusedFileCount()).append("] with total_size [").append(new ByteSizeValue(recoveryState.getIndex().reusedByteCount())).append("]\n");
                        sb.append("    start    : took [").append(TimeValue.timeValueMillis(recoveryState.getStart().time())).append("], check_index [").append(timeValueMillis(recoveryState.getStart().checkIndexTime())).append("]\n");
                        sb.append("    translog : number_of_operations [").append(recoveryState.getTranslog().currentTranslogOperations()).append("], took [").append(TimeValue.timeValueMillis(recoveryState.getTranslog().time())).append("]");
                        logger.trace(sb.toString());
                    } else if (logger.isDebugEnabled()) {
                        logger.debug("recovery completed from [{}], took [{}]", shardGateway, timeValueMillis(recoveryState.getTimer().time()));
                    }
                    listener.onRecoveryDone();
                    scheduleSnapshotIfNeeded();
                } catch (IndexShardGatewayRecoveryException e) {
                    if (indexShard.state() == IndexShardState.CLOSED) {
                        // got closed on us, just ignore this recovery
                        listener.onIgnoreRecovery("shard closed");
                        return;
                    }
                    if ((e.getCause() instanceof IndexShardClosedException) || (e.getCause() instanceof IndexShardNotStartedException)) {
                        // got closed on us, just ignore this recovery
                        listener.onIgnoreRecovery("shard closed");
                        return;
                    }
                    listener.onRecoveryFailed(e);
                } catch (IndexShardClosedException e) {
                    listener.onIgnoreRecovery("shard closed");
                } catch (IndexShardNotStartedException e) {
                    listener.onIgnoreRecovery("shard closed");
                } catch (Exception e) {
                    if (indexShard.state() == IndexShardState.CLOSED) {
                        // got closed on us, just ignore this recovery
                        listener.onIgnoreRecovery("shard closed");
                        return;
                    }
                    listener.onRecoveryFailed(new IndexShardGatewayRecoveryException(shardId, "failed recovery", e));
                }
            }
        });
    }

    /**
     * Snapshots the given shard into the gateway.
     */
    public synchronized void snapshot(final String reason) throws IndexShardGatewaySnapshotFailedException {
        if (!indexShard.routingEntry().primary()) {
            return;
//            throw new IndexShardGatewaySnapshotNotAllowedException(shardId, "Snapshot not allowed on non primary shard");
        }
        if (indexShard.routingEntry().relocating()) {
            // do not snapshot when in the process of relocation of primaries so we won't get conflicts
            return;
        }
        if (indexShard.state() == IndexShardState.CREATED) {
            // shard has just been created, ignore it and return
            return;
        }
        if (indexShard.state() == IndexShardState.RECOVERING) {
            // shard is recovering, don't snapshot
            return;
        }

        if (snapshotLock == null) {
            try {
                snapshotLock = shardGateway.obtainSnapshotLock();
            } catch (Exception e) {
                logger.warn("failed to obtain snapshot lock, ignoring snapshot", e);
                return;
            }
        }

        try {
            SnapshotStatus snapshotStatus = indexShard.snapshot(new Engine.SnapshotHandler<SnapshotStatus>() {
                @Override
                public SnapshotStatus snapshot(SnapshotIndexCommit snapshotIndexCommit, Translog.Snapshot translogSnapshot) throws EngineException {
                    if (lastIndexVersion != snapshotIndexCommit.getGeneration() || lastTranslogId != translogSnapshot.translogId() || lastTranslogLength < translogSnapshot.length()) {

                        logger.debug("snapshot ({}) to {} ...", reason, shardGateway);
                        SnapshotStatus snapshotStatus =
                                shardGateway.snapshot(new IndexShardGateway.Snapshot(snapshotIndexCommit, translogSnapshot, lastIndexVersion, lastTranslogId, lastTranslogLength, lastTotalTranslogOperations));

                        lastIndexVersion = snapshotIndexCommit.getGeneration();
                        lastTranslogId = translogSnapshot.translogId();
                        lastTranslogLength = translogSnapshot.length();
                        lastTotalTranslogOperations = translogSnapshot.estimatedTotalOperations();
                        return snapshotStatus;
                    }
                    return null;
                }
            });
            if (snapshotStatus != null) {
                if (logger.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("snapshot (").append(reason).append(") completed to ").append(shardGateway).append(", took [").append(TimeValue.timeValueMillis(snapshotStatus.time())).append("]\n");
                    sb.append("    index    : version [").append(lastIndexVersion).append("], number_of_files [").append(snapshotStatus.index().numberOfFiles()).append("] with total_size [").append(new ByteSizeValue(snapshotStatus.index().totalSize())).append("], took [").append(TimeValue.timeValueMillis(snapshotStatus.index().time())).append("]\n");
                    sb.append("    translog : id      [").append(lastTranslogId).append("], number_of_operations [").append(snapshotStatus.translog().expectedNumberOfOperations()).append("], took [").append(TimeValue.timeValueMillis(snapshotStatus.translog().time())).append("]");
                    logger.debug(sb.toString());
                }
            }
        } catch (SnapshotFailedEngineException e) {
            if (e.getCause() instanceof IllegalStateException) {
                // ignore, that's fine, snapshot has not started yet
            } else {
                throw new IndexShardGatewaySnapshotFailedException(shardId, "Failed to snapshot", e);
            }
        } catch (IllegalIndexShardStateException e) {
            // ignore, that's fine, snapshot has not started yet
        } catch (IndexShardGatewaySnapshotFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new IndexShardGatewaySnapshotFailedException(shardId, "Failed to snapshot", e);
        }
    }

    public void snapshotOnClose() {
        if (shardGateway.requiresSnapshot() && snapshotOnClose) {
            try {
                snapshot("shutdown");
            } catch (Exception e) {
                logger.warn("failed to snapshot on close", e);
            }
        }
    }

    @Override
    public synchronized void close() {
        indexSettingsService.removeListener(applySettings);
        if (snapshotScheduleFuture != null) {
            snapshotScheduleFuture.cancel(true);
            snapshotScheduleFuture = null;
        }
        shardGateway.close();
        if (snapshotLock != null) {
            snapshotLock.release();
        }
    }

    private synchronized void scheduleSnapshotIfNeeded() {
        if (!shardGateway.requiresSnapshot()) {
            return;
        }
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
            snapshotScheduleFuture = threadPool.schedule(snapshotInterval, ThreadPool.Names.SNAPSHOT, snapshotRunnable);
        }
    }

    private class SnapshotRunnable implements Runnable {
        @Override
        public synchronized void run() {
            try {
                snapshot("scheduled");
            } catch (Throwable e) {
                if (indexShard.state() == IndexShardState.CLOSED) {
                    return;
                }
                logger.warn("failed to snapshot (scheduled)", e);
            }
            // schedule it again
            if (indexShard.state() != IndexShardState.CLOSED) {
                snapshotScheduleFuture = threadPool.schedule(snapshotInterval, ThreadPool.Names.SNAPSHOT, this);
            }
        }
    }
}
