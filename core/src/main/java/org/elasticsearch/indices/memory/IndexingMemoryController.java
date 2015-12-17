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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

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
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.threadpool.ThreadPool;

public class IndexingMemoryController extends AbstractLifecycleComponent<IndexingMemoryController> {

    /** How much heap (% or bytes) we will share across all actively indexing shards on this node (default: 10%). */
    public static final String INDEX_BUFFER_SIZE_SETTING = "indices.memory.index_buffer_size";

    /** Only applies when <code>indices.memory.index_buffer_size</code> is a %, to set a floor on the actual size in bytes (default: 48 MB). */
    public static final String MIN_INDEX_BUFFER_SIZE_SETTING = "indices.memory.min_index_buffer_size";

    /** Only applies when <code>indices.memory.index_buffer_size</code> is a %, to set a ceiling on the actual size in bytes (default: not set). */
    public static final String MAX_INDEX_BUFFER_SIZE_SETTING = "indices.memory.max_index_buffer_size";

    /** If we see no indexing operations after this much time for a given shard, we consider that shard inactive (default: 5 minutes). */
    public static final String SHARD_INACTIVE_TIME_SETTING = "indices.memory.shard_inactive_time";

    /** Default value (5 minutes) for indices.memory.shard_inactive_time */
    public static final TimeValue SHARD_DEFAULT_INACTIVE_TIME = TimeValue.timeValueMinutes(5);

    /** How frequently we check indexing memory usage (default: 5 seconds). */
    public static final String SHARD_MEMORY_INTERVAL_TIME_SETTING = "indices.memory.interval";

    /** Hardwired translog buffer size */
    public static final ByteSizeValue SHARD_TRANSLOG_BUFFER = ByteSizeValue.parseBytesSizeValue("8kb", "SHARD_TRANSLOG_BUFFER");

    private final ThreadPool threadPool;
    private final IndicesService indicesService;

    private final ByteSizeValue indexingBuffer;

    private final TimeValue inactiveTime;
    private final TimeValue interval;

    /** Contains shards currently being throttled because we can't write segments quickly enough */
    private final Set<IndexShard> throttled = new HashSet<>();

    private volatile ScheduledFuture scheduler;

    private static final EnumSet<IndexShardState> CAN_WRITE_INDEX_BUFFER_STATES = EnumSet.of(
            IndexShardState.RECOVERING, IndexShardState.POST_RECOVERY, IndexShardState.STARTED, IndexShardState.RELOCATED);

    private final ShardsIndicesStatusChecker statusChecker;

    /** Maps each shard to how many bytes it is currently, asynchronously, writing to disk */
    private final Map<IndexShard,Long> writingBytes = new ConcurrentHashMap<>();

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

        this.inactiveTime = this.settings.getAsTime(SHARD_INACTIVE_TIME_SETTING, SHARD_DEFAULT_INACTIVE_TIME);
        // we need to have this relatively small to free up heap quickly enough
        this.interval = this.settings.getAsTime(SHARD_MEMORY_INTERVAL_TIME_SETTING, TimeValue.timeValueSeconds(5));

        this.statusChecker = new ShardsIndicesStatusChecker();

        logger.debug("using indexing buffer size [{}] with {} [{}], {} [{}]",
                     this.indexingBuffer,
                     SHARD_INACTIVE_TIME_SETTING, this.inactiveTime,
                     SHARD_MEMORY_INTERVAL_TIME_SETTING, this.interval);
    }

    /** Shard calls this when it starts writing its indexing buffer to disk to notify us */
    public void addWritingBytes(IndexShard shard, long numBytes) {
        writingBytes.put(shard, numBytes);
        logger.debug("add [{}] writing bytes for shard [{}]", new ByteSizeValue(numBytes), shard.shardId());
    }

    /** Shard calls when it's done writing these bytes to disk */
    public void removeWritingBytes(IndexShard shard, long numBytes) {
        writingBytes.remove(shard);
        logger.debug("clear [{}] writing bytes for shard [{}]", new ByteSizeValue(numBytes), shard.shardId());

        // Since some bytes just freed up, now we check again to give throttling a chance to stop:
        forceCheck();
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

    protected List<IndexShard> availableShards() {
        List<IndexShard> availableShards = new ArrayList<>();

        for (IndexService indexService : indicesService) {
            for (IndexShard shard : indexService) {
                // shadow replica doesn't have an indexing buffer
                if (shard.canIndex() && CAN_WRITE_INDEX_BUFFER_STATES.contains(shard.state())) {
                    availableShards.add(shard);
                }
            }
        }
        return availableShards;
    }

    /** returns how much heap this shard is using for its indexing buffer */
    protected long getIndexBufferRAMBytesUsed(IndexShard shard) {
        return shard.getIndexBufferRAMBytesUsed();
    }

    /** ask this shard to refresh, in the background, to free up heap */
    protected void writeIndexingBufferAsync(IndexShard shard) {
        shard.writeIndexingBufferAsync();
    }

    /** used by tests to check if any shards active status changed, now. */
    public void forceCheck() {
        statusChecker.run();
    }

    /** called by IndexShard to record that this many bytes were written to translog */
    public void bytesWritten(int bytes) {
        statusChecker.bytesWritten(bytes);
    }

    /** Asks this shard to throttle indexing to one thread */
    protected void activateThrottling(IndexShard shard) {
        shard.activateThrottling();
    }

    /** Asks this shard to stop throttling indexing to one thread */
    protected void deactivateThrottling(IndexShard shard) {
        shard.deactivateThrottling();
    }

    static final class ShardAndBytesUsed implements Comparable<ShardAndBytesUsed> {
        final long bytesUsed;
        final IndexShard shard;

        public ShardAndBytesUsed(long bytesUsed, IndexShard shard) {
            this.bytesUsed = bytesUsed;
            this.shard = shard;
        }

        @Override
        public int compareTo(ShardAndBytesUsed other) {
            // Sort larger shards first:
            return Long.compare(other.bytesUsed, bytesUsed);
        }
    }

    class ShardsIndicesStatusChecker implements Runnable {

        long bytesWrittenSinceCheck;

        /** Shard calls this on each indexing/delete op */
        public synchronized void bytesWritten(int bytes) {
            bytesWrittenSinceCheck += bytes;
            if (bytesWrittenSinceCheck > indexingBuffer.bytes()/30) {
                // NOTE: this is only an approximate check, because bytes written is to the translog, vs indexing memory buffer which is
                // typically smaller but can be larger in extreme cases (many unique terms).  This logic is here only as a safety against
                // thread starvation or too infrequent checking, to ensure we are still checking periodically, in proportion to bytes
                // processed by indexing:
                run();
            }
        }

        @Override
        public synchronized void run() {

            // NOTE: even if we hit an errant exc here, our ThreadPool.scheduledWithFixedDelay will log the exception and re-invoke us
            // again, on schedule

            // First pass to sum up how much heap all shards' indexing buffers are using now, and how many bytes they are currently moving
            // to disk:
            long totalBytesUsed = 0;
            long totalBytesWriting = 0;
            for (IndexShard shard : availableShards()) {

                // Give shard a chance to transition to inactive so sync'd flush can happen:
                checkIdle(shard, inactiveTime.nanos());

                // How many bytes this shard is currently (async'd) moving from heap to disk:
                Long shardWritingBytes = writingBytes.get(shard);

                // How many heap bytes this shard is currently using
                long shardBytesUsed = getIndexBufferRAMBytesUsed(shard);

                if (shardWritingBytes != null) {
                    shardBytesUsed -= shardWritingBytes;
                    totalBytesWriting += shardWritingBytes;

                    // If the refresh completed just after we pulled shardWritingBytes and before we pulled shardBytesUsed, then we could
                    // have a negative value here.  So we just skip this shard since that means it's now using very little heap:
                    if (shardBytesUsed < 0) {
                        continue;
                    }
                }

                totalBytesUsed += shardBytesUsed;
            }

            if (logger.isTraceEnabled()) {
                logger.trace("total indexing heap bytes used [{}] vs {} [{}], currently writing bytes [{}]",
                             new ByteSizeValue(totalBytesUsed), INDEX_BUFFER_SIZE_SETTING, indexingBuffer, new ByteSizeValue(totalBytesWriting));
            }

            // If we are using more than 50% of our budget across both indexing buffer and bytes we are still moving to disk, then we now
            // throttle the top shards to send back-pressure to ongoing indexing:
            boolean doThrottle = (totalBytesWriting + totalBytesUsed) > 1.5 * indexingBuffer.bytes();

            if (totalBytesUsed > indexingBuffer.bytes()) {
                // OK we are now over-budget; fill the priority queue and ask largest shard(s) to refresh:
                logger.debug("now write some indexing buffers: total indexing heap bytes used [{}] vs {} [{}], currently writing bytes [{}]",
                             new ByteSizeValue(totalBytesUsed), INDEX_BUFFER_SIZE_SETTING, indexingBuffer, new ByteSizeValue(totalBytesWriting));
                PriorityQueue<ShardAndBytesUsed> queue = new PriorityQueue<>();

                for (IndexShard shard : availableShards()) {
                    // How many bytes this shard is currently (async'd) moving from heap to disk:
                    Long shardWritingBytes = writingBytes.get(shard);

                    // How many heap bytes this shard is currently using
                    long shardBytesUsed = getIndexBufferRAMBytesUsed(shard);

                    if (shardWritingBytes != null) {
                        // Only count up bytes not already being refreshed:
                        shardBytesUsed -= shardWritingBytes;

                        // If the refresh completed just after we pulled shardWritingBytes and before we pulled shardBytesUsed, then we could
                        // have a negative value here.  So we just skip this shard since that means it's now using very little heap:
                        if (shardBytesUsed < 0) {
                            continue;
                        }
                    }

                    if (shardBytesUsed > 0) {
                        if (logger.isTraceEnabled()) {
                            if (shardWritingBytes != null) {
                                logger.trace("shard [{}] is using [{}] heap, writing [{}] heap", shard.shardId(), shardBytesUsed, shardWritingBytes);
                            } else {
                                logger.trace("shard [{}] is using [{}] heap, not writing any bytes", shard.shardId(), shardBytesUsed);
                            }
                        }
                        queue.add(new ShardAndBytesUsed(shardBytesUsed, shard));
                    }
                }

                while (totalBytesUsed > indexingBuffer.bytes() && queue.isEmpty() == false) {
                    ShardAndBytesUsed largest = queue.poll();
                    logger.debug("write indexing buffer to disk for shard [{}] to free up its [{}] indexing buffer", largest.shard.shardId(), new ByteSizeValue(largest.bytesUsed));
                    writeIndexingBufferAsync(largest.shard);
                    totalBytesUsed -= largest.bytesUsed;
                    if (doThrottle && throttled.contains(largest.shard) == false) {
                        logger.info("now throttling indexing for shard [{}]: segment writing can't keep up", largest.shard.shardId());
                        throttled.add(largest.shard);
                        activateThrottling(largest.shard);
                    }
                }
            }

            if (doThrottle == false) {
                for(IndexShard shard : throttled) {
                    logger.info("stop throttling indexing for shard [{}]", shard.shardId());
                    deactivateThrottling(shard);
                }
                throttled.clear();
            }

            bytesWrittenSinceCheck = 0;
        }
    }

    /**
     * ask this shard to check now whether it is inactive, and reduces its indexing buffer if so.
     */
    protected void checkIdle(IndexShard shard, long inactiveTimeNS) {
        try {
            shard.checkIdle(inactiveTimeNS);
        } catch (EngineClosedException | FlushNotAllowedEngineException e) {
            logger.trace("ignore exception while checking if shard {} is inactive", e, shard.shardId());
        }
    }
}
