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

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A recovery handler that skips phase one as well as sending the translog snapshot.
 */
public class SharedFSRecoverySourceHandler extends RecoverySourceHandler {

    private final IndexShard shard;
    private final StartRecoveryRequest request;

    SharedFSRecoverySourceHandler(IndexShard shard, RecoveryTargetHandler recoveryTarget, StartRecoveryRequest request,
                                  Supplier<Long> currentClusterStateVersionSupplier,
                                  Function<String, Releasable> delayNewRecoveries, Settings nodeSettings) {
        super(shard, recoveryTarget, request, currentClusterStateVersionSupplier, delayNewRecoveries, -1, nodeSettings);
        this.shard = shard;
        this.request = request;
    }

    @Override
    public RecoveryResponse recoverToTarget() throws IOException {
        boolean engineClosed = false;
        try {
            logger.trace("recovery [phase1]: skipping phase1 for shared filesystem");
            final long maxUnsafeAutoIdTimestamp = shard.segmentStats(false).getMaxUnsafeAutoIdTimestamp();
            if (request.isPrimaryRelocation()) {
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
            prepareTargetForTranslog(0, maxUnsafeAutoIdTimestamp);
            finalizeRecovery();
            return response;
        } catch (Exception e) {
            if (engineClosed) {
                // If the relocation fails then the primary is closed and can't be
                // used anymore... (because it's closed) that's a problem, so in
                // that case, fail the shard to reallocate a new IndexShard and
                // create a new IndexWriter
                logger.info("recovery failed for primary shadow shard, failing shard");
                // pass the failure as null, as we want to ensure the store is not marked as corrupted
                shard.failShard("primary relocation failed on shared filesystem", e);
            } else {
                logger.info("recovery failed on shared filesystem", e);
            }
            throw e;
        }
    }

    @Override
    protected int sendSnapshot(final long startingSeqNo, final Translog.Snapshot snapshot) {
        logger.trace("skipping recovery of translog snapshot on shared filesystem");
        return 0;
    }

}
