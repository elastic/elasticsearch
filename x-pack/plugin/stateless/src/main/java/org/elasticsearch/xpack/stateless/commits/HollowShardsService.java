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

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.IndexShardCacheWarmer;
import co.elastic.elasticsearch.stateless.engine.HollowIndexEngine;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Functionality around the hollowing of inactive shards to reduce their memory footprint.
 *
 * A regular unhollow indexing shard will be relocated as a hollow shard if it deemed hollowable according to the criteria set by
 * {@link HollowShardsService#isHollowableIndexShard(IndexShard)}.
 *
 * This class manages ingestion blockers which are installed for hollow shards to block ingestion. Any ingestion will trigger the
 * unhollowing of the shard which will uninstall the blocker and let ingestion pass through. An ingestion blocker is also removed when
 * a shard closes.
 *
 * The hollow process is implemented in {@link TransportStatelessPrimaryRelocationAction} and is summarized as follows:
 * <ul>
 * <li> The source indexing shard acquires all primary permits when marked as relocated, and installs an ingestion blocker with
 *      {@link HollowShardsService#addHollowShard(IndexShard)}.</li>
 * <li> The source indexing shard resets the engine, which flushes a hollow commit, and switches to the {@link HollowIndexEngine}</li>
 * <li> The target indexing shard recovers with a {@link HollowIndexEngine}.</li>
 * <li> The target indexing shard installs an ingestion blocker.</li>
 * </ul>
 */
public class HollowShardsService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(HollowShardsService.class);

    /**
     * Enables the super thin indexing shards feature in order to hollow inactive indexing shards and decrease their memory footprint.
     */
    public static final Setting<Boolean> STATELESS_HOLLOW_INDEX_SHARDS_ENABLED = Setting.boolSetting(
        "stateless.hollow_index_shards.enabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * How long a data stream non-write index should not have received ingestion for, before being considered for hollowing.
     */
    public static final Setting<TimeValue> SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL = Setting.positiveTimeSetting(
        "stateless.hollow_index_shards.ingestion.ds_non_write_ttl",
        TimeValue.timeValueMinutes(15),
        Setting.Property.NodeScope
    );

    /**
     * How long a regular index, or the write index of a data stream, should not have received ingestion for, before being
     * considered for hollowing.
     */
    public static final Setting<TimeValue> SETTING_HOLLOW_INGESTION_TTL = Setting.positiveTimeSetting(
        "stateless.hollow_index_shards.ingestion.ttl",
        TimeValue.timeValueDays(3),
        Setting.Property.NodeScope
    );

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final IndexShardCacheWarmer indexShardCacheWarmer;
    private final ThreadPool threadPool;

    private final LongSupplier relativeTimeSupplierInMillis;
    private final boolean featureEnabled;
    private final TimeValue ingestionDataStreamNonWriteTtl;
    private final TimeValue ingestionTtl;

    private record HollowShardInfo(SubscribableListener<Void> listener, AtomicBoolean unhollowing) {};

    private final Map<ShardId, HollowShardInfo> hollowShards = new ConcurrentHashMap<>();

    public HollowShardsService(
        Settings settings,
        ClusterService clusterService,
        IndicesService indicesService,
        IndexShardCacheWarmer indexShardCacheWarmer,
        ThreadPool threadPool
    ) {
        this(
            settings,
            clusterService,
            indicesService,
            indexShardCacheWarmer,
            threadPool,
            clusterService.threadPool()::relativeTimeInMillis
        );
    }

    public HollowShardsService(
        Settings settings,
        ClusterService clusterService,
        IndicesService indicesService,
        IndexShardCacheWarmer indexShardCacheWarmer,
        ThreadPool threadPool,
        LongSupplier relativeTimeSupplierInMillis
    ) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.indexShardCacheWarmer = indexShardCacheWarmer;
        this.relativeTimeSupplierInMillis = relativeTimeSupplierInMillis;
        this.threadPool = threadPool;
        this.featureEnabled = HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.get(settings);
        this.ingestionDataStreamNonWriteTtl = HollowShardsService.SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL.get(settings);
        this.ingestionTtl = HollowShardsService.SETTING_HOLLOW_INGESTION_TTL.get(settings);
        if (featureEnabled) {
            logger.info("Hollow index shards enabled with TTL {} and DS non-write TTL", ingestionTtl, ingestionDataStreamNonWriteTtl);
        } else {
            logger.debug(() -> "Hollow index shards disabled");
        }
    }

    public boolean isFeatureEnabled() {
        return featureEnabled;
    }

    public boolean isHollowableIndexShard(IndexShard indexShard) {
        return isHollowableIndexShard(indexShard, true);
    }

    public boolean isHollowableIndexShard(IndexShard indexShard, boolean checkPrimaryPermits) {
        boolean noActiveOperations = checkPrimaryPermits ? indexShard.getActiveOperationsCount() == 0 : true;
        // TODO consider that ingestion is not blocked. We should not hollow a shard that is being unhollowed due to new blocked ingestion.
        boolean ingestionNotBlocked = isHollowShard(indexShard.shardId()) == false;
        if (featureEnabled && indexShard.isSystem() == false && noActiveOperations && ingestionNotBlocked) {
            final var engine = indexShard.getEngineOrNull();
            if (engine instanceof IndexEngine indexEngine) {
                final var index = indexShard.shardId().getIndex();
                final var indexAbstraction = clusterService.state().metadata().getIndicesLookup().get(index.getName());
                if (indexAbstraction != null) {
                    final var dataStream = indexAbstraction.getParentDataStream();
                    final boolean dsNonWrite = dataStream != null
                        && Objects.equals(dataStream.getWriteIndex(), index) == false
                        && Objects.equals(dataStream.getWriteFailureIndex(), index) == false;
                    final TimeValue ttl = dsNonWrite ? ingestionDataStreamNonWriteTtl : ingestionTtl;
                    return engineHasNoIngestion(indexEngine, ttl);
                }
            }
        }
        return false;
    }

    protected boolean engineHasNoIngestion(Engine engine, TimeValue ttl) {
        final long lastWriteMillis = TimeValue.nsecToMSec(engine.getLastWriteNanos());
        final long now = relativeTimeSupplierInMillis.getAsLong();
        return now - lastWriteMillis > ttl.millis();
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() throws IOException {}

    /**
     * Adds a hollow shard and installs an ingestion blocker for a hollow shard to block any ingestion.
     *
     * @param indexShard the hollow index shard for which ingestion will be blocked
     */
    public void addHollowShard(IndexShard indexShard) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
        final var shardId = indexShard.shardId();
        logger.debug(() -> "installing ingestion blocker for shard " + shardId);
        // We only install the blocker when all primary permits are held during primary relocation or when a hollow shard
        // is recovering (before any ingestion is processed)
        assert indexShard.state() == IndexShardState.POST_RECOVERY || indexShard.getActiveOperationsCount() == IndexShard.OPERATIONS_BLOCKED
            : "can only be done in post recovery or when all primary permits are blocked. current state is "
                + indexShard.state()
                + " and active operations count "
                + indexShard.getActiveOperationsCount()
                + " for shard "
                + shardId;
        var existingBlocker = hollowShards.put(shardId, new HollowShardInfo(new SubscribableListener<>(), new AtomicBoolean(false)));
        assert existingBlocker == null : "already hollow shard " + shardId;
    }

    /**
     * Removes a hollow shard and uninstalls any ingestion blocker for it, releasing any blocked ingestion to proceed.
     *
     * @param indexShard the index shard for which ingestion will be released
     */
    public void removeHollowShard(IndexShard indexShard) {
        final var shardId = indexShard.shardId();
        var existingBlocker = hollowShards.remove(shardId);
        if (existingBlocker != null) {
            logger.debug(() -> "uninstalling ingestion blocker for shard " + shardId);
            // A hollow shard blocks ingestion, thus it cannot have new non-persisted operations.
            assert indexShard.state() == IndexShardState.CLOSED
                || indexShard.getLocalCheckpoint() == indexShard.getEngineOrNull().getMaxSeqNo()
                : "uninstalling ingestion blocker for shard "
                    + shardId
                    + " which is either not closed (state "
                    + indexShard.state()
                    + ") or it has non-persisted ops (local checkpoint "
                    + indexShard.getLocalCheckpoint()
                    + " and max seq no "
                    + indexShard.getEngineOrNull().getMaxSeqNo()
                    + ")";
            existingBlocker.listener.onResponse(null);
        }
    }

    /**
     * Processes an ingestion operation. If the shard is hollow, the operation will be blocked until the shard is unhollowed and the
     * ingestion blocker is uninstalled.
     *
     * @param shardId the shard for which the ingestion operation is being processed
     * @param listenerSupplier supplies the listener to be notified when the ingestion operation can proceed. In case the operation can
     *                         proceed immediately, the supplier is not called at all.
     */
    public void onIngestion(ShardId shardId, Supplier<ActionListener<Void>> listenerSupplier) {
        var ingestionBlocker = hollowShards.get(shardId);
        if (ingestionBlocker != null) {
            logger.debug(() -> "adding ingestion operation for shard " + shardId + " to the ingestion blocker");
            ingestionBlocker.listener.addListener(listenerSupplier.get());
            unhollow(shardId);
        }
    }

    public void ensureHollowShard(ShardId shardId, boolean hollow) {
        ensureHollowShard(shardId, hollow, "shard should be " + (hollow ? "hollow" : "unhollow"));
    }

    public void ensureHollowShard(ShardId shardId, boolean hollow, String message) {
        final boolean isCurrentlyBlocked = hollowShards.get(shardId) != null;
        if (hollow != isCurrentlyBlocked) {
            assert false : message;
            throw new IllegalStateException(message);
        }
    }

    public boolean isHollowShard(ShardId shardId) {
        return hollowShards.get(shardId) != null;
    }

    protected void unhollow(ShardId shardId) {
        var hollowShardInfo = hollowShards.get(shardId);
        if (hollowShardInfo != null && hollowShardInfo.unhollowing.compareAndSet(false, true)) {
            threadPool.generic().execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    var indexShard = indicesService.getShardOrNull(shardId);
                    assert indexShard != null : shardId;
                    assert indexShard.getEngineOrNull() instanceof HollowIndexEngine
                        : "Shouldn't unhollow if the shard has already been unhollowed";
                    // We acquire a permit to ensure that unhollowing does not race with a relocation.
                    indexShard.acquirePrimaryOperationPermit(ActionListener.wrap(primaryPermit -> {
                        try {
                            // Similar to the stateless recovery process without activating primary context
                            // (since we the shard is already in the primary mode and has checkpoint information)
                            logger.info("{} unhollowing shard (reason: ingestion)", shardId);
                            // Pre-warm the cache for the new index engine
                            indexShardCacheWarmer.preWarmIndexShardCache(indexShard, false);
                            indexShard.resetEngine();
                            var engine = indexShard.getEngineOrNull();
                            assert engine instanceof IndexEngine : "After unhollowing we should switch to IndexEngine";
                            var newEngine = (IndexEngine) engine;
                            assert newEngine.isLastCommitHollow() : "After unhollowing the last commit should be hollow";

                            newEngine.skipTranslogRecovery();
                            newEngine.refresh("post_unhollow");

                            logger.debug("Flushing shard [{}] to produce a blob with a local translog node id", shardId);
                            newEngine.flush(true, true, ActionListener.releaseAfter(ActionListener.wrap(flushResult -> {
                                assert flushResult.flushPerformed() : "Flush wasn't performed";
                                removeHollowShard(indexShard);
                                assert newEngine.isLastCommitHollow() == false : "After a flush the last commit should not be hollow";
                            }, e -> failUnhollowing(shardId, e)), primaryPermit));
                        } catch (Exception e) {
                            primaryPermit.close();
                            throw e;
                        }
                    }, e -> failUnhollowing(shardId, e)), EsExecutors.DIRECT_EXECUTOR_SERVICE);
                }

                @Override
                public void onFailure(Exception e) {
                    failUnhollowing(shardId, e);
                }

                private void failUnhollowing(ShardId shardId, Exception e) {
                    logger.error("Unable to unhollow shard [" + shardId + "]", e);
                    var indexShard = indicesService.getShardOrNull(shardId);
                    if (indexShard != null) {
                        // This should close the shard which uninstalls the ingestion blocker
                        indexShard.failShard("Unable to unhollow shard", e);
                    }
                }
            });
        }
    }
}
