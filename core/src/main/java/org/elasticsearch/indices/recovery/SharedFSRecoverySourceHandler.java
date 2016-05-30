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

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;

/**
 * A recovery handler that skips phase 1 as well as sending the snapshot. During phase 3 the shard is marked
 * as relocated an closed to ensure that the engine is closed and the target can acquire the IW write lock.
 */
public class SharedFSRecoverySourceHandler extends RecoverySourceHandler {

    private final IndexShard shard;
    private final StartRecoveryRequest request;

    public SharedFSRecoverySourceHandler(IndexShard shard, RecoveryTargetHandler recoveryTarget, StartRecoveryRequest request, ESLogger
            logger) {
        super(shard, recoveryTarget, request, -1, logger);
        this.shard = shard;
        this.request = request;
    }

    @Override
    public RecoveryResponse recoverToTarget() throws IOException {
        boolean engineClosed = false;
        try {
            logger.trace("{} recovery [phase1] to {}: skipping phase 1 for shared filesystem", request.shardId(), request.targetNode());
            if (isPrimaryRelocation()) {
                logger.debug("[phase1] closing engine on primary for shared filesystem recovery");
                try {
                    // if we relocate we need to close the engine in order to open a new
                    // IndexWriter on the other end of the relocation
                    engineClosed = true;
                    shard.flushAndCloseEngine();
                } catch (IOException e) {
                    logger.warn("close engine failed", e);
                    shard.failShard("failed to close engine (phase1)", e);
                }
            }
            prepareTargetForTranslog(0);
            finalizeRecovery();
            return response;
        } catch (Throwable t) {
            if (engineClosed) {
                // If the relocation fails then the primary is closed and can't be
                // used anymore... (because it's closed) that's a problem, so in
                // that case, fail the shard to reallocate a new IndexShard and
                // create a new IndexWriter
                logger.info("recovery failed for primary shadow shard, failing shard");
                // pass the failure as null, as we want to ensure the store is not marked as corrupted
                shard.failShard("primary relocation failed on shared filesystem", t);
            } else {
                logger.info("recovery failed on shared filesystem", t);
            }
            throw t;
        }
    }

    @Override
    protected int sendSnapshot(Translog.Snapshot snapshot) {
        logger.trace("{} skipping recovery of translog snapshot on shared filesystem to: {}",
                shard.shardId(), request.targetNode());
        return 0;
    }
}
