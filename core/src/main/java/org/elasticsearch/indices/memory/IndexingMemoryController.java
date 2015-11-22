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

package org.elasticsearch.indices.memory;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.EngineClosedException;
import org.elasticsearch.index.engine.FlushNotAllowedEngineException;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.*;
import java.util.concurrent.ScheduledFuture;

public class IndexingMemoryController extends AbstractLifecycleComponent<IndexingMemoryController> implements IndexEventListener {

    /** How much heap (% or bytes) we will share across all actively indexing shards on this node (default: 10%). */
    public static final String INDEX_BUFFER_SIZE_SETTING = "indices.memory.index_buffer_size";

    /** Only applies when <code>indices.memory.index_buffer_size</code> is a %, to set a floor on the actual size in bytes (default: 48 MB). */
    public static final String MIN_INDEX_BUFFER_SIZE_SETTING = "indices.memory.min_index_buffer_size";

    /** Only applies when <code>indices.memory.index_buffer_size</code> is a %, to set a ceiling on the actual size in bytes (default: not set). */
    public static final String MAX_INDEX_BUFFER_SIZE_SETTING = "indices.memory.max_index_buffer_size";

    /** Sets a floor on the per-shard index buffer size (default: 4 MB). */
    public static final String MIN_SHARD_INDEX_BUFFER_SIZE_SETTING = "indices.memory.min_shard_index_buffer_size";

    /** Sets a ceiling on the per-shard index buffer size (default: 512 MB). */
    public static final String MAX_SHARD_INDEX_BUFFER_SIZE_SETTING = "indices.memory.max_shard_index_buffer_size";

    /** How much heap (% or bytes) we will share across all actively indexing shards for the translog buffer (default: 1%). */
    public static final String TRANSLOG_BUFFER_SIZE_SETTING = "indices.memory.translog_buffer_size";

    /** Only applies when <code>indices.memory.translog_buffer_size</code> is a %, to set a floor on the actual size in bytes (default: 256 KB). */
    public static final String MIN_TRANSLOG_BUFFER_SIZE_SETTING = "indices.memory.min_translog_buffer_size";

    /** Only applies when <code>indices.memory.translog_buffer_size</code> is a %, to set a ceiling on the actual size in bytes (default: not set). */
    public static final String MAX_TRANSLOG_BUFFER_SIZE_SETTING = "indices.memory.max_translog_buffer_size";

    /** Sets a floor on the per-shard translog buffer size (default: 2 KB). */
    public static final String MIN_SHARD_TRANSLOG_BUFFER_SIZE_SETTING = "indices.memory.min_shard_translog_buffer_size";

    /** Sets a ceiling on the per-shard translog buffer size (default: 64 KB). */
    public static final String MAX_SHARD_TRANSLOG_BUFFER_SIZE_SETTING = "indices.memory.max_shard_translog_buffer_size";

    /** How frequently we check shards to find inactive ones (default: 30 seconds). */
    public static final String SHARD_INACTIVE_INTERVAL_TIME_SETTING = "indices.memory.interval";

    /** Once a shard becomes inactive, we reduce the {@code IndexWriter} buffer to this value (500 KB) to let active shards use the heap instead. */
    public static final ByteSizeValue INACTIVE_SHARD_INDEXING_BUFFER = ByteSizeValue.parseBytesSizeValue("500kb", "INACTIVE_SHARD_INDEXING_BUFFER");

    /** Once a shard becomes inactive, we reduce the {@code Translog} buffer to this value (1 KB) to let active shards use the heap instead. */
    public static final ByteSizeValue INACTIVE_SHARD_TRANSLOG_BUFFER = ByteSizeValue.parseBytesSizeValue("1kb", "INACTIVE_SHARD_TRANSLOG_BUFFER");

    private final ThreadPool threadPool;
    private final IndicesService indicesService;

    private final ByteSizeValue indexingBuffer;
    private final ByteSizeValue minShardIndexBufferSize;
    private final ByteSizeValue maxShardIndexBufferSize;

    private final ByteSizeValue translogBuffer;
    private final ByteSizeValue minShardTranslogBufferSize;
    private final ByteSizeValue maxShardTranslogBufferSize;

    private final TimeValue interval;

    private volatile ScheduledFuture scheduler;

    private static final EnumSet<IndexShardState> CAN_UPDATE_INDEX_BUFFER_STATES = EnumSet.of(
            IndexShardState.RECOVERING, IndexShardState.POST_RECOVERY, IndexShardState.STARTED, IndexShardState.RELOCATED);

    private final ShardsIndicesStatusChecker statusChecker;

    @Inject
    public IndexingMemoryController(Settings settings, ThreadPool threadPool, IndicesService indicesService) {
        this(settings, threadPool, indicesService, JvmInfo.jvmInfo().getMem().getHeapMax().bytes());
    }

    // for testing
    protected IndexingMemoryController(Settings settings, ThreadPool threadPool, IndicesService indicesService, long jvmMemoryInBytes) {
        super(settings);
        this.threadPool = threadPool;
        this.indicesService = indicesService;

        ByteSizeValue indexingBuffer;
        String indexingBufferSetting = this.settings.get(INDEX_BUFFER_SIZE_SETTING, "10%");
        if (indexingBufferSetting.endsWith("%")) {
            double percent = Double.parseDouble(indexingBufferSetting.substring(0, indexingBufferSetting.length() - 1));
            indexingBuffer = new ByteSizeValue((long) (((double) jvmMemoryInBytes) * (percent / 100)));
            ByteSizeValue minIndexingBuffer = this.settings.getAsBytesSize(MIN_INDEX_BUFFER_SIZE_SETTING, new ByteSizeValue(48, ByteSizeUnit.MB));
            ByteSizeValue maxIndexingBuffer = this.settings.getAsBytesSize(MAX_INDEX_BUFFER_SIZE_SETTING, null);

            if (indexingBuffer.bytes() < minIndexingBuffer.bytes()) {
                indexingBuffer = minIndexingBuffer;
            }
            if (maxIndexingBuffer != null && indexingBuffer.bytes() > maxIndexingBuffer.bytes()) {
                indexingBuffer = maxIndexingBuffer;
            }
        } else {
            indexingBuffer = ByteSizeValue.parseBytesSizeValue(indexingBufferSetting, INDEX_BUFFER_SIZE_SETTING);
        }
        this.indexingBuffer = indexingBuffer;
        this.minShardIndexBufferSize = this.settings.getAsBytesSize(MIN_SHARD_INDEX_BUFFER_SIZE_SETTING, new ByteSizeValue(4, ByteSizeUnit.MB));
        // LUCENE MONITOR: Based on this thread, currently (based on Mike), having a large buffer does not make a lot of sense: https://issues.apache.org/jira/browse/LUCENE-2324?focusedCommentId=13005155&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13005155
        this.maxShardIndexBufferSize = this.settings.getAsBytesSize(MAX_SHARD_INDEX_BUFFER_SIZE_SETTING, new ByteSizeValue(512, ByteSizeUnit.MB));

        ByteSizeValue translogBuffer;
        String translogBufferSetting = this.settings.get(TRANSLOG_BUFFER_SIZE_SETTING, "1%");
        if (translogBufferSetting.endsWith("%")) {
            double percent = Double.parseDouble(translogBufferSetting.substring(0, translogBufferSetting.length() - 1));
            translogBuffer = new ByteSizeValue((long) (((double) jvmMemoryInBytes) * (percent / 100)));
            ByteSizeValue minTranslogBuffer = this.settings.getAsBytesSize(MIN_TRANSLOG_BUFFER_SIZE_SETTING, new ByteSizeValue(256, ByteSizeUnit.KB));
            ByteSizeValue maxTranslogBuffer = this.settings.getAsBytesSize(MAX_TRANSLOG_BUFFER_SIZE_SETTING, null);

            if (translogBuffer.bytes() < minTranslogBuffer.bytes()) {
                translogBuffer = minTranslogBuffer;
            }
            if (maxTranslogBuffer != null && translogBuffer.bytes() > maxTranslogBuffer.bytes()) {
                translogBuffer = maxTranslogBuffer;
            }
        } else {
            translogBuffer = ByteSizeValue.parseBytesSizeValue(translogBufferSetting, TRANSLOG_BUFFER_SIZE_SETTING);
        }
        this.translogBuffer = translogBuffer;
        this.minShardTranslogBufferSize = this.settings.getAsBytesSize(MIN_SHARD_TRANSLOG_BUFFER_SIZE_SETTING, new ByteSizeValue(2, ByteSizeUnit.KB));
        this.maxShardTranslogBufferSize = this.settings.getAsBytesSize(MAX_SHARD_TRANSLOG_BUFFER_SIZE_SETTING, new ByteSizeValue(64, ByteSizeUnit.KB));

        // we need to have this relatively small to move a shard from inactive to active fast (enough)
        this.interval = this.settings.getAsTime(SHARD_INACTIVE_INTERVAL_TIME_SETTING, TimeValue.timeValueSeconds(30));

        this.statusChecker = new ShardsIndicesStatusChecker();

        logger.debug("using indexing buffer size [{}], with {} [{}], {} [{}], {} [{}]",
                this.indexingBuffer,
                MIN_SHARD_INDEX_BUFFER_SIZE_SETTING, this.minShardIndexBufferSize,
                MAX_SHARD_INDEX_BUFFER_SIZE_SETTING, this.maxShardIndexBufferSize,
                SHARD_INACTIVE_INTERVAL_TIME_SETTING, this.interval);
    }

    @Override
    protected void doStart() {
        // it's fine to run it on the scheduler thread, no busy work
        this.scheduler = threadPool.scheduleWithFixedDelay(statusChecker, interval);
    }

    @Override
    protected void doStop() {
        FutureUtils.cancel(scheduler);
        scheduler = null;
    }

    @Override
    protected void doClose() {
    }

    /**
     * returns the current budget for the total amount of indexing buffers of
     * active shards on this node
     */
    public ByteSizeValue indexingBufferSize() {
        return indexingBuffer;
    }

    /**
     * returns the current budget for the total amount of translog buffers of
     * active shards on this node
     */
    public ByteSizeValue translogBufferSize() {
        return translogBuffer;
    }


    protected List<ShardId> availableShards() {
        ArrayList<ShardId> list = new ArrayList<>();

        for (IndexService indexService : indicesService) {
            for (IndexShard indexShard : indexService) {
                if (shardAvailable(indexShard)) {
                    list.add(indexShard.shardId());
                }
            }
        }
        return list;
    }

    /** returns true if shard exists and is availabe for updates */
    protected boolean shardAvailable(ShardId shardId) {
        return shardAvailable(getShard(shardId));
    }

    /** returns true if shard exists and is availabe for updates */
    protected boolean shardAvailable(@Nullable IndexShard shard) {
        // shadow replica doesn't have an indexing buffer
        return shard != null && shard.canIndex() && CAN_UPDATE_INDEX_BUFFER_STATES.contains(shard.state());
    }

    /** gets an {@link IndexShard} instance for the given shard. returns null if the shard doesn't exist */
    protected IndexShard getShard(ShardId shardId) {
        IndexService indexService = indicesService.indexService(shardId.index().name());
        if (indexService != null) {
            IndexShard indexShard = indexService.getShardOrNull(shardId.id());
            return indexShard;
        }
        return null;
    }

    /** set new indexing and translog buffers on this shard.  this may cause the shard to refresh to free up heap. */
    protected void updateShardBuffers(ShardId shardId, ByteSizeValue shardIndexingBufferSize, ByteSizeValue shardTranslogBufferSize) {
        final IndexShard shard = getShard(shardId);
        if (shard != null) {
            try {
                shard.updateBufferSize(shardIndexingBufferSize, shardTranslogBufferSize);
            } catch (EngineClosedException e) {
                // ignore
            } catch (FlushNotAllowedEngineException e) {
                // ignore
            } catch (Exception e) {
                logger.warn("failed to set shard {} index buffer to [{}]", e, shardId, shardIndexingBufferSize);
            }
        }
    }

    /** returns {@link IndexShard#getActive} if the shard exists, else null */
    protected Boolean getShardActive(ShardId shardId) {
        final IndexShard indexShard = getShard(shardId);
        if (indexShard == null) {
            return null;
        }
        return indexShard.getActive();
    }

    /** check if any shards active status changed, now. */
    public void forceCheck() {
        statusChecker.run();
    }

    class ShardsIndicesStatusChecker implements Runnable {

        // True if the shard was active last time we checked
        private final Map<ShardId,Boolean> shardWasActive = new HashMap<>();

        @Override
        public synchronized void run() {
            EnumSet<ShardStatusChangeType> changes = purgeDeletedAndClosedShards();

            updateShardStatuses(changes);

            if (changes.isEmpty() == false) {
                // Something changed: recompute indexing buffers:
                calcAndSetShardBuffers("[" + changes + "]");
            }
        }

        /**
         * goes through all existing shards and check whether there are changes in their active status
         */
        private void updateShardStatuses(EnumSet<ShardStatusChangeType> changes) {
            for (ShardId shardId : availableShards()) {

                // Is the shard active now?
                Boolean isActive = getShardActive(shardId);

                if (isActive == null) {
                    // shard was closed..
                    continue;
                }

                // Was the shard active last time we checked?
                Boolean wasActive = shardWasActive.get(shardId);
                if (wasActive == null) {
                    // First time we are seeing this shard
                    shardWasActive.put(shardId, isActive);
                    changes.add(ShardStatusChangeType.ADDED);
                } else if (isActive) {
                    // Shard is active now
                    if (wasActive == false) {
                        // Shard became active itself, since we last checked (due to new indexing op arriving)
                        changes.add(ShardStatusChangeType.BECAME_ACTIVE);
                        logger.debug("marking shard {} as active indexing wise", shardId);
                        shardWasActive.put(shardId, true);
                    } else if (checkIdle(shardId) == Boolean.TRUE) {
                        // Make shard inactive now
                        changes.add(ShardStatusChangeType.BECAME_INACTIVE);

                        shardWasActive.put(shardId, false);
                    }
                }
            }
        }

        /**
         * purge any existing statuses that are no longer updated
         *
         * @return the changes applied
         */
        private EnumSet<ShardStatusChangeType> purgeDeletedAndClosedShards() {
            EnumSet<ShardStatusChangeType> changes = EnumSet.noneOf(ShardStatusChangeType.class);

            Iterator<ShardId> statusShardIdIterator = shardWasActive.keySet().iterator();
            while (statusShardIdIterator.hasNext()) {
                ShardId shardId = statusShardIdIterator.next();
                if (shardAvailable(shardId) == false) {
                    changes.add(ShardStatusChangeType.DELETED);
                    statusShardIdIterator.remove();
                }
            }
            return changes;
        }

        private void calcAndSetShardBuffers(String reason) {

            // Count how many shards are now active:
            int activeShardCount = 0;
            for (Map.Entry<ShardId,Boolean> ent : shardWasActive.entrySet()) {
                if (ent.getValue()) {
                    activeShardCount++;
                }
            }

            // TODO: we could be smarter here by taking into account how RAM the IndexWriter on each shard
            // is actually using (using IW.ramBytesUsed), so that small indices (e.g. Marvel) would not
            // get the same indexing buffer as large indices.  But it quickly gets tricky...
            if (activeShardCount == 0) {
                logger.debug("no active shards (reason={})", reason);
                return;
            }

            ByteSizeValue shardIndexingBufferSize = new ByteSizeValue(indexingBuffer.bytes() / activeShardCount);
            if (shardIndexingBufferSize.bytes() < minShardIndexBufferSize.bytes()) {
                shardIndexingBufferSize = minShardIndexBufferSize;
            }
            if (shardIndexingBufferSize.bytes() > maxShardIndexBufferSize.bytes()) {
                shardIndexingBufferSize = maxShardIndexBufferSize;
            }

            ByteSizeValue shardTranslogBufferSize = new ByteSizeValue(translogBuffer.bytes() / activeShardCount);
            if (shardTranslogBufferSize.bytes() < minShardTranslogBufferSize.bytes()) {
                shardTranslogBufferSize = minShardTranslogBufferSize;
            }
            if (shardTranslogBufferSize.bytes() > maxShardTranslogBufferSize.bytes()) {
                shardTranslogBufferSize = maxShardTranslogBufferSize;
            }

            logger.debug("recalculating shard indexing buffer (reason={}), total is [{}] with [{}] active shards, each shard set to indexing=[{}], translog=[{}]", reason, indexingBuffer, activeShardCount, shardIndexingBufferSize, shardTranslogBufferSize);

            for (Map.Entry<ShardId,Boolean> ent : shardWasActive.entrySet()) {
                if (ent.getValue()) {
                    // This shard is active
                    updateShardBuffers(ent.getKey(), shardIndexingBufferSize, shardTranslogBufferSize);
                }
            }
        }
    }

    protected long currentTimeInNanos() {
        return System.nanoTime();
    }

    /** ask this shard to check now whether it is inactive, and reduces its indexing and translog buffers if so.  returns Boolean.TRUE if
     *  it did deactive, Boolean.FALSE if it did not, and null if the shard is unknown */
    protected Boolean checkIdle(ShardId shardId) {
        String ignoreReason; // eclipse compiler does not know it is really final
        final IndexShard shard = getShard(shardId);
        if (shard != null) {
            try {
                if (shard.checkIdle()) {
                    logger.debug("marking shard {} as inactive (inactive_time[{}]) indexing wise",
                            shardId,
                            shard.getInactiveTime());
                    return Boolean.TRUE;
                }
                return Boolean.FALSE;
            } catch (EngineClosedException e) {
                // ignore
                ignoreReason = "EngineClosedException";
            } catch (FlushNotAllowedEngineException e) {
                // ignore
                ignoreReason = "FlushNotAllowedEngineException";
            }
        } else {
            ignoreReason = "shard not found";
        }
        if (ignoreReason != null) {
            logger.trace("ignore [{}] while marking shard {} as inactive", ignoreReason, shardId);
        }
        return null;
    }

    private static enum ShardStatusChangeType {
        ADDED, DELETED, BECAME_ACTIVE, BECAME_INACTIVE
    }

    @Override
    public void onShardActive(IndexShard indexShard) {
        // At least one shard used to be inactive ie. a new write operation just showed up.
        // We try to fix the shards indexing buffer immediately. We could do this async instead, but cost should
        // be low, and it's rare this happens.
        forceCheck();
    }
}
