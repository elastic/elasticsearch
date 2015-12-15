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
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.threadpool.ThreadPool;

// nocommit what is IndexEventListener
public class IndexingMemoryController extends AbstractLifecycleComponent<IndexingMemoryController> implements IndexEventListener {

    /** How much heap (% or bytes) we will share across all actively indexing shards on this node (default: 10%). */
    public static final String INDEX_BUFFER_SIZE_SETTING = "indices.memory.index_buffer_size";

    /** Only applies when <code>indices.memory.index_buffer_size</code> is a %, to set a floor on the actual size in bytes (default: 48 MB). */
    public static final String MIN_INDEX_BUFFER_SIZE_SETTING = "indices.memory.min_index_buffer_size";

    /** Only applies when <code>indices.memory.index_buffer_size</code> is a %, to set a ceiling on the actual size in bytes (default: not set). */
    public static final String MAX_INDEX_BUFFER_SIZE_SETTING = "indices.memory.max_index_buffer_size";

    /** If we see no indexing operations after this much time for a given shard, we consider that shard inactive (default: 5 minutes). */
    public static final String SHARD_INACTIVE_TIME_SETTING = "indices.memory.shard_inactive_time";

    /** How frequently we check indexing memory usage (default: 5 seconds). */
    public static final String SHARD_MEMORY_INTERVAL_TIME_SETTING = "indices.memory.interval";

    /** Hardwired translog buffer size */
    public static final ByteSizeValue SHARD_TRANSLOG_BUFFER = ByteSizeValue.parseBytesSizeValue("8kb", "SHARD_TRANSLOG_BUFFER");

    private final ThreadPool threadPool;
    private final IndicesService indicesService;

    private final ByteSizeValue indexingBuffer;

    private final TimeValue inactiveTime;
    private final TimeValue interval;

    private volatile ScheduledFuture scheduler;

    private static final EnumSet<IndexShardState> CAN_UPDATE_INDEX_BUFFER_STATES = EnumSet.of(
            IndexShardState.RECOVERING, IndexShardState.POST_RECOVERY, IndexShardState.STARTED, IndexShardState.RELOCATED);

    private final ShardsIndicesStatusChecker statusChecker;

    /** How many bytes we are currently moving to disk by the engine to refresh */
    private final AtomicLong bytesRefreshingNow = new AtomicLong();

    private final Map<IndexShard,Long> refreshingBytes = new ConcurrentHashMap<>();

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

        this.inactiveTime = this.settings.getAsTime(SHARD_INACTIVE_TIME_SETTING, TimeValue.timeValueMinutes(5));
        // we need to have this relatively small to free up heap quickly enough
        this.interval = this.settings.getAsTime(SHARD_MEMORY_INTERVAL_TIME_SETTING, TimeValue.timeValueSeconds(5));

        this.statusChecker = new ShardsIndicesStatusChecker();

        logger.debug("using indexing buffer size [{}] with {} [{}], {} [{}]",
                     this.indexingBuffer,
                     SHARD_INACTIVE_TIME_SETTING, this.inactiveTime,
                     SHARD_MEMORY_INTERVAL_TIME_SETTING, this.interval);
    }

    /** Shard calls this to notify us that this many bytes are being asynchronously moved from RAM to disk */
    public void addWritingBytes(IndexShard shard, long numBytes) {
        refreshingBytes.put(shard, numBytes);
    }

    /** Shard calls this to notify us that this many bytes are are done being asynchronously moved from RAM to disk */
    public void removeWritingBytes(IndexShard shard, long numBytes) {
        // nocommit this can fail, if two refreshes are running "concurrently"
        Long result = refreshingBytes.remove(shard);
        assert result != null;
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
                if (shardAvailable(shard)) {
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

    /** returns true if shard exists and is availabe for updates */
    protected boolean shardAvailable(IndexShard shard) {
        // shadow replica doesn't have an indexing buffer
        return shard.canIndex() && CAN_UPDATE_INDEX_BUFFER_STATES.contains(shard.state());
    }

    /** check if any shards active status changed, now. */
    public void forceCheck() {
        statusChecker.run();
    }

    long startMS = System.currentTimeMillis();

    /** called by IndexShard to record that this many bytes were written to translog */
    public void bytesWritten(int bytes) {
        statusChecker.bytesWritten(bytes);
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
            if (bytesWrittenSinceCheck > indexingBuffer.bytes()/20) {
                // NOTE: this is only an approximate check, because bytes written is to the translog, vs indexing memory buffer which is
                // typically smaller but can be larger in extreme cases (many unique terms).  This logic is here only as a safety against
                // thread starvation or too infrequent checking, to ensure we are still checking periodically, in proportion to bytes
                // processed by indexing:
                System.out.println(((System.currentTimeMillis() - startMS)/1000.0) + ": NOW CHECK xlog=" + bytesWrittenSinceCheck);
                run();
            }
        }

        @Override
        public synchronized void run() {

            // nocommit add defensive try/catch-everything here?  bad if an errant EngineClosedExc kills off this thread!!

            // Fast check to sum up how much heap all shards' indexing buffers are using now:
            long totalBytesUsed = 0;
            for (IndexShard shard : availableShards()) {

                // Give shard a chance to transition to inactive so sync'd flush can happen:
                checkIdle(shard, inactiveTime.nanos());

                // nocommit explain why order is important here!
                Long bytes = refreshingBytes.get(shard);

                long shardBytesUsed = getIndexBufferRAMBytesUsed(shard);

                if (bytes != null) {
                    // Only count up bytes not already being refreshed:
                    shardBytesUsed -= bytes;

                    // If the refresh completed just after we pulled refreshingBytes and before we pulled index buffer bytes, then we could
                    // have a negative value here:
                    if (shardBytesUsed < 0) {
                        continue;
                    }
                }

                totalBytesUsed += shardBytesUsed;
                System.out.println("IMC:   " + shard.shardId() + " using " + (shardBytesUsed/1024./1024.) + " MB");
            }

            System.out.println(((System.currentTimeMillis() - startMS)/1000.0) + ": TOT=" + totalBytesUsed + " vs " + indexingBuffer.bytes());

            if (totalBytesUsed - bytesRefreshingNow.get() > indexingBuffer.bytes()) {
                // OK we are using too much; make a queue and ask largest shard(s) to refresh:
                logger.debug("now refreshing some shards: total indexing bytes used [{}] vs index_buffer_size [{}]", new ByteSizeValue(totalBytesUsed), indexingBuffer);
                PriorityQueue<ShardAndBytesUsed> queue = new PriorityQueue<>();
                for (IndexShard shard : availableShards()) {
                    // nocommit explain why order is important here!
                    Long bytes = refreshingBytes.get(shard);

                    long shardBytesUsed = getIndexBufferRAMBytesUsed(shard);

                    if (bytes != null) {
                        // Only count up bytes not already being refreshed:
                        shardBytesUsed -= bytes;

                        // If the refresh completed just after we pulled refreshingBytes and before we pulled index buffer bytes, then we could
                        // have a negative value here:
                        if (shardBytesUsed < 0) {
                            continue;
                        }
                    }

                    if (shardBytesUsed > 0) {
                        queue.add(new ShardAndBytesUsed(shardBytesUsed, shard));
                    }
                }

                while (totalBytesUsed > indexingBuffer.bytes() && queue.isEmpty() == false) {
                    ShardAndBytesUsed largest = queue.poll();
                    System.out.println("IMC: write " + largest.shard.shardId() + ": " + (largest.bytesUsed/1024./1024.) + " MB");
                    logger.debug("refresh shard [{}] to free up its [{}] indexing buffer", largest.shard.shardId(), new ByteSizeValue(largest.bytesUsed));
                    writeIndexingBufferAsync(largest.shard);
                    totalBytesUsed -= largest.bytesUsed;
                }
            }

            bytesWrittenSinceCheck = 0;
        }
    }

    /**
     * ask this shard to check now whether it is inactive, and reduces its indexing and translog buffers if so.
     * return false if the shard is not idle, otherwise true
     */
    protected void checkIdle(IndexShard shard, long inactiveTimeNS) {
        try {
            shard.checkIdle(inactiveTimeNS);
        } catch (EngineClosedException | FlushNotAllowedEngineException e) {
            logger.trace("ignore exception while checking if shard {} is inactive", e, shard.shardId());
        }
    }
}
