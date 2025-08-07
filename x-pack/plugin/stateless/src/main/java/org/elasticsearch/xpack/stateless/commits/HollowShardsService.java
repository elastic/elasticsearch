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
import co.elastic.elasticsearch.stateless.engine.HollowShardsMetrics;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction;

import org.apache.lucene.store.AlreadyClosedException;
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
import org.elasticsearch.index.shard.IndexShardClosedException;
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
 *      {@link HollowShardsService#addHollowShard(IndexShard,String)}.</li>
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
    private final HollowShardsMetrics metrics;

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
        ThreadPool threadPool,
        HollowShardsMetrics metrics
    ) {
        this(
            settings,
            clusterService,
            indicesService,
            indexShardCacheWarmer,
            threadPool,
            metrics,
            clusterService.threadPool()::relativeTimeInMillis
        );
    }

    public HollowShardsService(
        Settings settings,
        ClusterService clusterService,
        IndicesService indicesService,
        IndexShardCacheWarmer indexShardCacheWarmer,
        ThreadPool threadPool,
        HollowShardsMetrics metrics,
        LongSupplier relativeTimeSupplierInMillis
    ) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.indexShardCacheWarmer = indexShardCacheWarmer;
        this.relativeTimeSupplierInMillis = relativeTimeSupplierInMillis;
        this.metrics = metrics;
        this.threadPool = threadPool;
        this.featureEnabled = HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.get(settings);
        this.ingestionDataStreamNonWriteTtl = HollowShardsService.SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL.get(settings);
        this.ingestionTtl = HollowShardsService.SETTING_HOLLOW_INGESTION_TTL.get(settings);
        if (featureEnabled) {
            logger.info("Hollow index shards enabled with TTL {} and DS non-write TTL {}", ingestionTtl, ingestionDataStreamNonWriteTtl);
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
                final var indexAbstraction = clusterService.state().metadata().getProject().getIndicesLookup().get(index.getName());
                if (indexAbstraction != null) {
                    final var dataStream = indexAbstraction.getParentDataStream();
                    final boolean dsNonWrite = dataStream != null
                        && Objects.equals(dataStream.getWriteIndex(), index) == false
                        && Objects.equals(dataStream.getWriteFailureIndex(), index) == false;
                    final TimeValue ttl = dsNonWrite ? ingestionDataStreamNonWriteTtl : ingestionTtl;
                    return engineHasNoIngestion(indexEngine, ttl) && indexEngine.hasQueuedOrRunningMerges() == false;
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
    protected void doClose() throws IOException {
        assert hollowShards.isEmpty() : "no ingestion blocker should be installed at this point";
    }

    /**
     * Adds a hollow shard and installs an ingestion blocker for a hollow shard to block any ingestion.
     *
     * @param indexShard the hollow index shard for which ingestion will be blocked
     */
    public void addHollowShard(IndexShard indexShard, String reason) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
        final var shardId = indexShard.shardId();
        logger.info(() -> "installing ingestion blocker for shard " + shardId + " due to " + reason);

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
        metrics.incrementHollowShardCount();

        // If shard got closed in the meantime, it might have missed removing the ingestion blocker we just installed, so ensure
        // we remove the blocker. And then throw.
        if (indexShard.state() == IndexShardState.CLOSED) {
            removeHollowShard(indexShard, "closed while adding ingestion blocker");
            throw new IndexShardClosedException(shardId);
        }
    }

    /**
     * Removes a hollow shard and uninstalls any ingestion blocker for it, releasing any blocked ingestion to proceed.
     *
     * @param indexShard the index shard for which ingestion will be released
     * @param reason     the reason the hollow shard is being removed
     */
    public void removeHollowShard(IndexShard indexShard, String reason) {
        final var shardId = indexShard.shardId();
        removeHollowShard(shardId, reason, () -> {
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
        });
    }

    private void removeHollowShard(ShardId shardId, String reason, Runnable onRemove) {
        var existingBlocker = hollowShards.remove(shardId);
        if (existingBlocker != null) {
            onRemove.run();
            logger.info("{} removed hollow shard due to {}", shardId, reason);
            existingBlocker.listener.onResponse(null);
            metrics.decrementHollowShardCount();
        }
    }

    /**
     * Processes a mutable operations (e.g., ingestion, force merges) operation.
     * If the shard is hollow, the operation will be blocked until the shard is
     * unhollowed and the ingestion blocker is uninstalled.
     *
     * @param indexShard     the shard for which the mutable operation is being processed
     * @param permitAcquired whether the operation has acquired an operation permit on the shard
     * @param listener       the listener to be notified when the mutable operation can proceed
     */
    public void onMutableOperation(IndexShard indexShard, boolean permitAcquired, ActionListener<Void> listener) {
        // Unhollowing requires a primary permit. We use the permitAcquired flag to determine if the primary permit has been acquired.
        // If it's not, we take our own primary permit here before unhollowing.
        // Ingestion already holds a primary permit, so there is no need to take another one, and it would be dangerous to take yet
        // another one as it could create a deadlock: ingestion comes to a hollow shard, takes a primary permit, comes into this code,
        // at the same time relocation starts which takes all primary permits and delays future ones, and then we try to take another
        // primary permit here which will never be granted. Relocation would wait for the ingestion, and ingestion would wait here
        // without initiating the unhollowing which could let ingestion go through.
        // Finally, for ingestion, it is important that ensureMutable is called after it has got a primary permit. That is because a
        // competing relocation (that has got all primary permits) will ensure that ingestion waits before getting to this function.
        // If the relocation fails, the shard would be hollowed, and this function will ensure ingestion waits at the ingestion
        // blocker until unhollowing (which needs to be initiated by this function) completes.
        var shardId = indexShard.shardId();
        var ingestionBlocker = hollowShards.get(shardId);
        if (ingestionBlocker != null) {
            if (permitAcquired) {
                logger.debug(() -> "adding ingestion operation for shard " + shardId + " to the ingestion blocker");
                assert indexShard.getActiveOperationsCount() > 0 : indexShard.getActiveOperationsCount();
                ingestionBlocker.listener.addListener(listener);
                unhollow(shardId);
            } else {
                logger.debug(() -> "acquiring primary permit for shard " + shardId + " for unhollowing");
                indexShard.acquirePrimaryOperationPermit(ActionListener.wrap(primaryPermit -> {
                    logger.debug(() -> "acquired primary permit for shard " + shardId + " and adding to the ingestion blocker");
                    ingestionBlocker.listener.addListener(ActionListener.releaseBefore(primaryPermit, listener));
                    unhollow(shardId);
                }, e -> listener.onFailure(e)), EsExecutors.DIRECT_EXECUTOR_SERVICE);
            }
        } else {
            listener.onResponse(null);
        }
    }

    public void ensureHollowShard(ShardId shardId, boolean hollow) {
        ensureHollowShard(shardId, hollow, "shard " + shardId + " should be " + (hollow ? "hollow" : "unhollow"));
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
                    long startTime = relativeTimeSupplierInMillis.getAsLong();
                    final var indexService = indicesService.indexServiceSafe(shardId.getIndex());
                    final var indexShard = indexService.getShard(shardId.id());
                    assert indexShard.getEngineOrNull() == null || indexShard.getEngineOrNull() instanceof HollowIndexEngine
                        : "Shouldn't unhollow if the shard has already been unhollowed";
                    // We would like unhollowing to be done under a primary permit, so that it does not race with a relocation.
                    // This is out of precaution due to the following raised concerns:
                    // * To have a consistent behaviour with all the operations that trigger unhollowing.
                    // * Unhollowing resets the engine, and then flushes outside of the engine write lock, and we want to be sure it
                    // does not compete with any hollowing initiated by a concurrent relocation (which should not be theoretically
                    // possible as relocation should not re-hollow an already hollow shard, but we want to be sure).
                    // TODO: read the above, and consider whether unhollowing can happen without a primary permit. (ES-11416)
                    assert indexShard.getActiveOperationsCount() > 0
                        : "unhollowing requires a permit but they are " + indexShard.getActiveOperationsCount();

                    // Similar to the stateless recovery process without activating primary context
                    // (since we the shard is already in the primary mode and has checkpoint information)
                    logger.info("{} unhollowing shard (reason: ingestion)", shardId);
                    // Pre-warm the cache for the new index engine
                    indexShardCacheWarmer.preWarmIndexShardCache(indexShard, false);
                    indexShard.resetEngine(engine -> {
                        assert assertIndexEngineLastCommitHollow(shardId, engine, true);
                        assert engine.getEngineConfig().getEngineResetLock().isWriteLockedByCurrentThread() : shardId;
                        engine.refresh("unhollowing"); // warms up reader managers
                        engine.skipTranslogRecovery(); // allows new flushes
                    });

                    logger.debug("Flushing shard [{}] to produce a blob with a local translog node id", shardId);
                    indexShard.withEngine(engine -> {
                        engine.flush(true, true, ActionListener.wrap(flushResult -> {
                            assert flushResult.skippedDueToCollision() == false : "Flush was skipped";
                            removeHollowShard(indexShard, "unhollowing gen " + flushResult.generation());
                            metrics.unhollowSuccessCounter().increment();
                            metrics.unhollowTimeMs().record(relativeTimeSupplierInMillis.getAsLong() - startTime);
                            assert assertIndexEngineLastCommitHollow(shardId, engine, false);
                        }, e -> failedUnhollowing(shardId, e)));
                        return null;
                    });
                }

                @Override
                public void onFailure(Exception e) {
                    failedUnhollowing(shardId, e);
                }

                private void failedUnhollowing(ShardId shardId, Exception e) {
                    // An unhollowing may fail with legitimate reasons, e.g., if there is an index deletion or shard closure.
                    // It may take a bit of time until the shard is completely closed and the ingestion blocker is removed.
                    // If unhollowing fails for other reasons, we fail the shard as a last resort to release the ingestion blocker.
                    logger.debug("unhollowing failed on shard " + shardId, e);
                    var indexShard = indicesService.getShardOrNull(shardId);
                    if (indexShard == null || indexShard.state() == IndexShardState.CLOSED) {
                        logger.warn("removing ingestion blocker after unhollowing failure of closed " + shardId, e);
                        removeHollowShard(shardId, "unhollowing failure", () -> {});
                    } else {
                        assert indexShard.isRelocatedPrimary() == false : "relocation should not be possible during unhollowing";
                        // As a last resort, fail the shard to ultimately uninstall the ingestion blocker.
                        try {
                            indexShard.failShard("unable to unhollow shard", e);
                        } catch (AlreadyClosedException ace) {
                            // ignore, shard is already closed
                        }
                    }
                }
            });
        }
    }

    private static boolean assertIndexEngineLastCommitHollow(ShardId shardId, Engine engine, boolean expectLastCommitHollow) {
        assert engine instanceof IndexEngine : shardId + ": expect IndexEngine but got " + engine.getClass();
        final var isLastCommitHollow = ((IndexEngine) engine).isLastCommitHollow();
        assert isLastCommitHollow == expectLastCommitHollow
            : shardId + ": expect last commit hollow to be [" + expectLastCommitHollow + "] but got [" + isLastCommitHollow + ']';
        return true;
    }
}
