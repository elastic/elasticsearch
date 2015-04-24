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
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.*;
import org.elasticsearch.index.snapshots.IndexShardSnapshotAndRestoreService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

/**
 *
 */
public class IndexShardGatewayService extends AbstractIndexShardComponent implements Closeable {

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final IndexShard indexShard;

    private final IndexShardGateway shardGateway;

    private final IndexShardSnapshotAndRestoreService snapshotService;

    @Inject
    public IndexShardGatewayService(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool,
                                    IndexShard indexShard, IndexShardGateway shardGateway, IndexShardSnapshotAndRestoreService snapshotService, ClusterService clusterService) {
        super(shardId, indexSettings);
        this.threadPool = threadPool;
        this.indexShard = indexShard;
        this.shardGateway = shardGateway;
        this.snapshotService = snapshotService;
        this.clusterService = clusterService;
    }

    /**
     * Should be called when the shard routing state has changed (note, after the state has been set on the shard).
     */
    public void routingStateChanged() {
    }

    public static interface RecoveryListener {
        void onRecoveryDone();

        void onIgnoreRecovery(String reason);

        void onRecoveryFailed(IndexShardGatewayRecoveryException e);
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
                indexShard.recovering("from snapshot", RecoveryState.Type.SNAPSHOT, indexShard.routingEntry().restoreSource());
            } else {
                indexShard.recovering("from gateway", RecoveryState.Type.GATEWAY, clusterService.localNode());
            }
        } catch (IllegalIndexShardStateException e) {
            // that's fine, since we might be called concurrently, just ignore this, we are already recovering
            listener.onIgnoreRecovery("already in recovering process, " + e.getMessage());
            return;
        }

        threadPool.generic().execute(new Runnable() {
            @Override
            public void run() {

                try {
                    final RecoveryState recoveryState = indexShard.recoveryState();
                    if (indexShard.routingEntry().restoreSource() != null) {
                        logger.debug("restoring from {} ...", indexShard.routingEntry().restoreSource());
                        snapshotService.restore(recoveryState);
                    } else {
                        logger.debug("starting recovery from {} ...", shardGateway);
                        shardGateway.recover(indexShouldExists, recoveryState);
                    }

                    // Check that the gateway didn't leave the shard in init or recovering stage. it is up to the gateway
                    // to call post recovery.
                    IndexShardState shardState = indexShard.state();
                    assert shardState != IndexShardState.CREATED && shardState != IndexShardState.RECOVERING : "recovery process of " + shardId + " didn't get to post_recovery. shardState [" + shardState + "]";

                    if (logger.isTraceEnabled()) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("recovery completed from ").append(shardGateway).append(", took [").append(timeValueMillis(recoveryState.getTimer().time())).append("]\n");
                        RecoveryState.Index index = recoveryState.getIndex();
                        sb.append("    index    : files           [").append(index.totalFileCount()).append("] with total_size [")
                                .append(new ByteSizeValue(index.totalBytes())).append("], took[")
                                .append(TimeValue.timeValueMillis(index.time())).append("]\n");
                        sb.append("             : recovered_files [").append(index.recoveredFileCount()).append("] with total_size [")
                                .append(new ByteSizeValue(index.recoveredBytes())).append("]\n");
                        sb.append("             : reusing_files   [").append(index.reusedFileCount()).append("] with total_size [")
                                .append(new ByteSizeValue(index.reusedBytes())).append("]\n");
                        sb.append("    verify_index    : took [").append(TimeValue.timeValueMillis(recoveryState.getVerifyIndex().time())).append("], check_index [")
                                .append(timeValueMillis(recoveryState.getVerifyIndex().checkIndexTime())).append("]\n");
                        sb.append("    translog : number_of_operations [").append(recoveryState.getTranslog().recoveredOperations())
                                .append("], took [").append(TimeValue.timeValueMillis(recoveryState.getTranslog().time())).append("]");
                        logger.trace(sb.toString());
                    } else if (logger.isDebugEnabled()) {
                        logger.debug("recovery completed from [{}], took [{}]", shardGateway, timeValueMillis(recoveryState.getTimer().time()));
                    }
                    listener.onRecoveryDone();
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

    @Override
    public synchronized void close() throws IOException {
        shardGateway.close();
    }
}
