/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ReaderContext;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class PITRelocationService implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(PITRelocationService.class);
    private final Map<ShardId, Set<Runnable>> relocatingContexts = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
    private final SetOnce<Function<ShardId, List<ReaderContext>>> activeReaderContextProvider = new SetOnce<>();
    private boolean pitRelocationEnabled = true;

    public PITRelocationService() {}

    public void setActiveReaderContextProvider(Function<ShardId, List<ReaderContext>> activeReaderContextProvider) {
        this.activeReaderContextProvider.set(activeReaderContextProvider);
    }

    public void setPitRelocationEnabled(boolean pitRelocationEnabled) {
        this.pitRelocationEnabled = pitRelocationEnabled;
    }

    @Override
    public void afterIndexShardRecovery(IndexShard indexShard, ActionListener<Void> listener) {
        ActionListener.run(listener, l -> {
            if (pitRelocationEnabled) {
                if (relocatingContexts.isEmpty() == false) {
                    ShardId shardId = indexShard.shardId();
                    logger.debug("afterIndexShardStarted [{}], relocatingContexts keys: [{}]", shardId, relocatingContexts.keySet());
                    Set<Runnable> cleanupRelocationContexts = relocatingContexts.remove(shardId);
                    if (cleanupRelocationContexts != null) {
                        for (Runnable contextSupplier : cleanupRelocationContexts) {
                            contextSupplier.run();
                        }
                    }
                }
            }
            l.onResponse(null);
        });
    }

    public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (pitRelocationEnabled) {
            logger.debug("afterIndexShardClosed [{}]", shardId);
            // on shard close, free all PIT contexts related to this shard. This is important on the source node of a PIT relocation
            // because otherwise we will never route searches to the new location of the PIT
            Function<ShardId, List<ReaderContext>> shardIdListFunction = this.activeReaderContextProvider.get();
            if (shardIdListFunction != null) {
                List<ReaderContext> activePITContexts = shardIdListFunction.apply(shardId);
                for (ReaderContext context : activePITContexts) {
                    context.forceExpired();
                    logger.debug("forcing PIT context [{}] on shard [{}] to expire in next Reaper run.", context.id(), shardId);
                }
            }
            // also remove potential relocating contexts on the target node. This is important when the relocation fails
            relocatingContexts.remove(shardId);
        }
    }

    public void addRelocatingContext(ShardId shardId, Runnable readerContextCreation) {
        if (pitRelocationEnabled) {
            this.relocatingContexts.computeIfAbsent(shardId, k -> ConcurrentCollections.newConcurrentSet()).add(readerContextCreation);
            logger.debug("addRelocatingContext [{}], relocatingContexts: [{}]", shardId, relocatingContexts);
        }
    }

}
