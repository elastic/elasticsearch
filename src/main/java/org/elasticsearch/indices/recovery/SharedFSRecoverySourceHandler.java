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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A recovery handler that skips phase 1 as well as sending the snapshot. During phase 3 the shard is marked
 * as relocated an closed to ensure that the engine is closed and the target can acquire the IW write lock.
 */
public class SharedFSRecoverySourceHandler extends RecoverySourceHandler {

    private final IndexShard shard;
    private final StartRecoveryRequest request;
    private final AtomicBoolean engineClosed = new AtomicBoolean(false);

    public SharedFSRecoverySourceHandler(IndexShard shard, StartRecoveryRequest request,
                                         RecoverySettings recoverySettings, TransportService transportService,
                                         ClusterService clusterService, IndicesService indicesService,
                                         MappingUpdatedAction mappingUpdatedAction, ESLogger logger) {
        super(shard, request, recoverySettings, transportService, clusterService, indicesService, mappingUpdatedAction, logger);
        this.shard = shard;
        this.request = request;
    }

    @Override
    public void phase1(SnapshotIndexCommit snapshot) throws ElasticsearchException {
        logger.trace("{} recovery [phase1] to {}: skipping phase 1 for shared filesystem",
                request.shardId(), request.targetNode());
        // if we relocate we need to close the engine in order to open a new
        // IndexWriter on the other end of the relocation
        if (isPrimaryRelocation()) {
            logger.debug("[phase1] closing engine on primary for shared filesystem recovery");
            shard.engine().flush(true, true);
            try {
                shard.engine().close();
                engineClosed.set(true);
            } catch (IOException e) {
                logger.warn("close engine failed", e);
                shard.failShard("failed to close engine (phase1)", e);
            }
        }
    }

    @Override
    public void phase2(Translog.Snapshot snapshot) throws ElasticsearchException {
        try {
            super.phase2(snapshot);
        } catch (Throwable t) {
            onRecoveryFailure(t);
            throw t;
        }
    }

    @Override
    public void phase3(Translog.Snapshot snapshot) throws ElasticsearchException {
        try {
            super.phase3(snapshot);
        } catch (Throwable t) {
            onRecoveryFailure(t);
            throw t;
        }
    }

    @Override
    protected int sendSnapshot(Translog.Snapshot snapshot) throws ElasticsearchException {
        logger.trace("{} skipping recovery of translog snapshot on shared filesystem to: {}",
                shard.shardId(), request.targetNode());
        return 0;
    }

    private boolean isPrimaryRelocation() {
        return request.recoveryType() == RecoveryState.Type.RELOCATION && shard.routingEntry().primary();
    }

    public void onRecoveryFailure(Throwable t) {
        if (isPrimaryRelocation() && engineClosed.get()) {
            // If the relocation fails then the primary is closed and can't be
            // used anymore... (because it's closed) that's a problem, so in
            // that case, fail the shard to reallocate a new IndexShard and
            // create a new IndexWriter
            logger.info("recovery failed for primary shadow shard, failing shard");
            // pass the failure as null, as we want to ensure the store is not marked as corrupted
            shard.failShard("primary relocation failed on shared filesystem caused by: [" + t.getMessage() + "]", null);
        } else {
            logger.info("recovery failed on shared filesystem", t);
        }
    }
}
