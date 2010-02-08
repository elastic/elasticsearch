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

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.*;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.StopWatch;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author kimchy (Shay Banon)
 */
public class IndexShardGatewayService extends AbstractIndexShardComponent {

    private final boolean snapshotOnClose;

    private final ThreadPool threadPool;

    private final InternalIndexShard indexShard;

    private final IndexShardGateway shardGateway;

    private final Store store;


    private volatile long lastIndexVersion;

    private volatile long lastTranslogId = -1;

    private volatile int lastTranslogSize;

    private final AtomicBoolean recovered = new AtomicBoolean();

    private final TimeValue snapshotInterval;

    private volatile ScheduledFuture snapshotScheduleFuture;

    @Inject public IndexShardGatewayService(ShardId shardId, @IndexSettings Settings indexSettings,
                                            ThreadPool threadPool, IndexShard indexShard, IndexShardGateway shardGateway,
                                            Store store) {
        super(shardId, indexSettings);
        this.threadPool = threadPool;
        this.indexShard = (InternalIndexShard) indexShard;
        this.shardGateway = shardGateway;
        this.store = store;

        this.snapshotOnClose = componentSettings.getAsBoolean("snapshotOnClose", true);
        this.snapshotInterval = componentSettings.getAsTime("snapshotInterval", TimeValue.timeValueSeconds(10));
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
            // clear the store, we are going to recover into it
            try {
                store.deleteContent();
            } catch (IOException e) {
                logger.debug("Failed to delete store before recovery from gateway", e);
            }
            indexShard.recovering();
            logger.debug("Starting recovery from {}", shardGateway);
            StopWatch stopWatch = new StopWatch().start();
            RecoveryStatus recoveryStatus = shardGateway.recover();

            // update the last up to date values
            indexShard.snapshot(new Engine.SnapshotHandler() {
                @Override public void snapshot(SnapshotIndexCommit snapshotIndexCommit, Translog.Snapshot translogSnapshot) throws EngineException {
                    lastIndexVersion = snapshotIndexCommit.getVersion();
                    lastTranslogId = translogSnapshot.translogId();
                    lastTranslogSize = translogSnapshot.size();
                }
            });

            // start the shard if the gateway has not started it already
            if (indexShard.state() != IndexShardState.STARTED) {
                indexShard.start();
            }
            stopWatch.stop();
            if (logger.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append("Recovery completed from ").append(shardGateway).append(", took [").append(stopWatch.totalTime()).append("]\n");
                sb.append("    Index    : numberOfFiles      [").append(recoveryStatus.index().numberOfFiles()).append("] with totalSize [").append(recoveryStatus.index().totalSize()).append("]\n");
                sb.append("    Translog : numberOfOperations [").append(recoveryStatus.translog().numberOfOperations()).append("] with totalSize [").append(recoveryStatus.translog().totalSize()).append("]");
                logger.debug(sb.toString());
            }
            // refresh the shard
            indexShard.refresh(false);
            scheduleSnapshotIfNeeded();
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
        indexShard.snapshot(new Engine.SnapshotHandler() {
            @Override public void snapshot(SnapshotIndexCommit snapshotIndexCommit, Translog.Snapshot translogSnapshot) throws EngineException {
                if (lastIndexVersion != snapshotIndexCommit.getVersion() || lastTranslogId != translogSnapshot.translogId() || lastTranslogSize != translogSnapshot.size()) {

                    shardGateway.snapshot(snapshotIndexCommit, translogSnapshot);

                    lastIndexVersion = snapshotIndexCommit.getVersion();
                    lastTranslogId = translogSnapshot.translogId();
                    lastTranslogSize = translogSnapshot.size();
                }
            }
        });
    }

    public void close() {
        if (snapshotScheduleFuture != null) {
            snapshotScheduleFuture.cancel(true);
            snapshotScheduleFuture = null;
        }
        if (snapshotOnClose) {
            logger.debug("Snapshotting on close ...");
            snapshot();
        }
        shardGateway.close();
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
                logger.debug("Scheduling snapshot every [{}]", snapshotInterval);
            }
            snapshotScheduleFuture = threadPool.scheduleWithFixedDelay(new SnapshotRunnable(), snapshotInterval);
        }
    }

    private class SnapshotRunnable implements Runnable {
        @Override public void run() {
            try {
                snapshot();
            } catch (Exception e) {
                logger.warn("Failed to snapshot", e);
            }
        }
    }
}
