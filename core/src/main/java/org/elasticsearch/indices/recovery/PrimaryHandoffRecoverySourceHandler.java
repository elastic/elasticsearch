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

import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardRelocatedException;
import org.elasticsearch.index.shard.IndexShardState;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * RecoverySourceHandler handles the three phases of shard recovery, which is
 * everything relating to copying the segment files as well as sending translog
 * operations across the wire once the segments have been copied.
 *
 * Note: There is always one source handler per recovery that handles all the
 * file and translog transfer. This handler is completely isolated from other recoveries
 * while the {@link RateLimiter} passed via {@link RecoverySettings} is shared across recoveries
 * originating from this nodes to throttle the number bytes send during file transfer. The transaction log
 * phase bypasses the rate limiter entirely.
 */
public class PrimaryHandoffRecoverySourceHandler extends RecoverySourceHandler {

    // Request containing source and target node information
    private final StartPrimaryHandoffRequest request;
    private final Supplier<Long> currentClusterStateVersionSupplier;
    private final Function<String, Releasable> delayNewRecoveries;
    private final PrimaryHandoffRecoveryTargetHandler recoveryTarget;

    public PrimaryHandoffRecoverySourceHandler(final IndexShard shard,
                                               PrimaryHandoffRecoveryTargetHandler recoveryTarget,
                                               final StartPrimaryHandoffRequest request,
                                               final Supplier<Long> currentClusterStateVersionSupplier,
                                               Function<String, Releasable> delayNewRecoveries,
                                               final Settings nodeSettings) {
        super(shard, nodeSettings, request.targetNode());
        this.recoveryTarget = recoveryTarget;
        this.request = request;
        this.currentClusterStateVersionSupplier = currentClusterStateVersionSupplier;
        this.delayNewRecoveries = delayNewRecoveries;
    }

    @Override
    public RecoveryResponse recoverToTarget() throws IOException {

        // engine was just started at the end of phase1
        if (shard.state() == IndexShardState.RELOCATED) {
            assert false : "recovery target should not retry primary relocation if previous " +
                "attempt made it past finalization step";
            //
            // The primary shard has been relocated while we copied files. This means that we can't
            // guarantee any more that all operations that were replicated during the file copy
            // (when the target engine was not yet opened) will be present in the local translog
            // and thus will be resent on phase2. The reason is that an operation replicated by
            // the sent to the recovery target and the local shard (old primary) concurrently,
            // meaning it may have arrived at the recovery target before we opened the engine and
            // is still in-flight on the local shard.
            //
            // Checking the relocated status here, after we opened the engine on the target, is safe
            // because primary relocation waits for all ongoing operations to complete and be
            // fully replicated. Therefore all future operation by the new primary are guaranteed
            // to reach the target shard when its engine is open.
            throw new IndexShardRelocatedException(request.shardId());
        } else if (shard.state() == IndexShardState.CLOSED) {
            throw new IndexShardClosedException(request.shardId());
        }
        StopWatch stopWatch = new StopWatch().start();
        logger.trace("finalizing recovery");

        // in case of primary relocation we have to ensure that the cluster state on the primary
        // relocation target has all replica shards that have recovered or are still recovering
        // from the current primary, otherwise replication actions will not be send to these
        // replicas. To accomplish this, first block new recoveries, then take version of latest
        // cluster state. This means that no new recovery can be completed based on information
        // of a newer cluster state than the current one.
        try (Releasable ignored =
                 delayNewRecoveries.apply("primary relocation hand-off in progress or completed " +
                     "for " + shard.shardId())) {
            final long currentClusterStateVersion = currentClusterStateVersionSupplier.get();
            logger.trace("waiting on remote node to have cluster state with version [{}]",
                currentClusterStateVersion);
            cancellableThreads.execute(
                () -> recoveryTarget.ensureClusterStateVersion(currentClusterStateVersion));

            logger.trace("performing relocation hand-off");
            cancellableThreads.execute(() -> shard.relocated("to " + request.targetNode()));
        }
        // if the recovery process fails after setting the shard state to RELOCATED, both
        // relocation source and target are failed (see {@link IndexShard#updateRoutingEntry}).
        stopWatch.stop();
        final String message = "finalizing recovery took [" + stopWatch.totalTime() + "]";
        logger.trace(message);
        response.apprendTraceSummary(message);
        return response;
    }
}
