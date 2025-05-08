/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class IndexingMemoryController implements IndexingOperationListener, Closeable {

    private static final Logger logger = LogManager.getLogger(IndexingMemoryController.class);

    /** How much heap (% or bytes) we will share across all actively indexing shards on this node (default: 10%). */
    public static final Setting<ByteSizeValue> INDEX_BUFFER_SIZE_SETTING = Setting.memorySizeSetting(
        "indices.memory.index_buffer_size",
        "10%",
        Property.NodeScope
    );

    /** Only applies when <code>indices.memory.index_buffer_size</code> is a %,
     * to set a floor on the actual size in bytes (default: 48 MB). */
    public static final Setting<ByteSizeValue> MIN_INDEX_BUFFER_SIZE_SETTING = Setting.byteSizeSetting(
        "indices.memory.min_index_buffer_size",
        ByteSizeValue.of(48, ByteSizeUnit.MB),
        ByteSizeValue.ZERO,
        ByteSizeValue.ofBytes(Long.MAX_VALUE),
        Property.NodeScope
    );

    /** Only applies when <code>indices.memory.index_buffer_size</code> is a %,
     * to set a ceiling on the actual size in bytes (default: not set). */
    public static final Setting<ByteSizeValue> MAX_INDEX_BUFFER_SIZE_SETTING = Setting.byteSizeSetting(
        "indices.memory.max_index_buffer_size",
        ByteSizeValue.MINUS_ONE,
        ByteSizeValue.MINUS_ONE,
        ByteSizeValue.ofBytes(Long.MAX_VALUE),
        Property.NodeScope
    );

    /** If we see no indexing operations after this much time for a given shard,
     * we consider that shard inactive (default: 5 minutes). */
    public static final Setting<TimeValue> SHARD_INACTIVE_TIME_SETTING = Setting.positiveTimeSetting(
        "indices.memory.shard_inactive_time",
        TimeValue.timeValueMinutes(5),
        Property.NodeScope
    );

    /** How frequently we check indexing memory usage (default: 5 seconds). */
    public static final Setting<TimeValue> SHARD_MEMORY_INTERVAL_TIME_SETTING = Setting.positiveTimeSetting(
        "indices.memory.interval",
        TimeValue.timeValueSeconds(5),
        Property.NodeScope
    );

    /* Currently, indexing is throttled due to memory pressure in stateful/stateless or disk pressure in stateless.
     * This limits the number of indexing threads to 1 per shard. However, this might not be enough when the number of
     * shards that need indexing is larger than the number of threads. So we might opt to pause indexing completely.
     * The default value for this setting is false, but it will be set to true in stateless.
     */
    public static final Setting<Boolean> PAUSE_INDEXING_ON_THROTTLE = Setting.boolSetting(
        "indices.pause.on.throttle",
        false,
        Property.NodeScope
    );

    private final ThreadPool threadPool;

    private final Iterable<IndexShard> indexShards;

    private final long indexingBuffer;

    private final TimeValue inactiveTime;
    private final TimeValue interval;

    /** Contains shards currently being throttled because we can't write segments quickly enough */
    private final Set<IndexShard> throttled = new HashSet<>();

    private final Cancellable scheduler;

    private static final EnumSet<IndexShardState> CAN_WRITE_INDEX_BUFFER_STATES = EnumSet.of(
        IndexShardState.RECOVERING,
        IndexShardState.POST_RECOVERY,
        IndexShardState.STARTED
    );

    private final ShardsIndicesStatusChecker statusChecker;

    private final Set<IndexShard> pendingWriteIndexingBufferSet = ConcurrentCollections.newConcurrentSet();
    private final Deque<IndexShard> pendingWriteIndexingBufferQueue = new ConcurrentLinkedDeque<>();

    IndexingMemoryController(Settings settings, ThreadPool threadPool, Iterable<IndexShard> indexServices) {
        this.indexShards = indexServices;

        ByteSizeValue indexingBuffer = INDEX_BUFFER_SIZE_SETTING.get(settings);

        String indexingBufferSetting = settings.get(INDEX_BUFFER_SIZE_SETTING.getKey());
        // null means we used the default (10%)
        if (indexingBufferSetting == null || indexingBufferSetting.endsWith("%")) {
            // We only apply the min/max when % value was used for the index buffer:
            ByteSizeValue minIndexingBuffer = MIN_INDEX_BUFFER_SIZE_SETTING.get(settings);
            ByteSizeValue maxIndexingBuffer = MAX_INDEX_BUFFER_SIZE_SETTING.get(settings);
            if (indexingBuffer.getBytes() < minIndexingBuffer.getBytes()) {
                indexingBuffer = minIndexingBuffer;
            }
            if (maxIndexingBuffer.getBytes() != -1 && indexingBuffer.getBytes() > maxIndexingBuffer.getBytes()) {
                indexingBuffer = maxIndexingBuffer;
            }
        }
        this.indexingBuffer = indexingBuffer.getBytes();

        this.inactiveTime = SHARD_INACTIVE_TIME_SETTING.get(settings);
        // we need to have this relatively small to free up heap quickly enough
        this.interval = SHARD_MEMORY_INTERVAL_TIME_SETTING.get(settings);

        this.statusChecker = new ShardsIndicesStatusChecker();

        logger.debug(
            "using indexing buffer size [{}] with {} [{}], {} [{}]",
            this.indexingBuffer,
            SHARD_INACTIVE_TIME_SETTING.getKey(),
            this.inactiveTime,
            SHARD_MEMORY_INTERVAL_TIME_SETTING.getKey(),
            this.interval
        );
        this.scheduler = scheduleTask(threadPool);

        // Need to save this so we can later launch async "write indexing buffer to disk" on shards:
        this.threadPool = threadPool;
    }

    protected Cancellable scheduleTask(ThreadPool threadPool) {
        // it's fine to run it on the scheduler thread, no busy work
        return threadPool.scheduleWithFixedDelay(statusChecker, interval, EsExecutors.DIRECT_EXECUTOR_SERVICE);
    }

    @Override
    public void close() {
        scheduler.cancel();
    }

    /**
     * returns the current budget for the total amount of indexing buffers of
     * active shards on this node
     */
    long indexingBufferSize() {
        return indexingBuffer;
    }

    protected List<IndexShard> availableShards() {
        List<IndexShard> availableShards = new ArrayList<>();
        for (IndexShard shard : indexShards) {
            if (CAN_WRITE_INDEX_BUFFER_STATES.contains(shard.state())) {
                availableShards.add(shard);
            }
        }
        return availableShards;
    }

    /** returns how much heap this shard is using for its indexing buffer */
    protected long getIndexBufferRAMBytesUsed(IndexShard shard) {
        return shard.getIndexBufferRAMBytesUsed();
    }

    /** returns how many bytes this shard is currently writing to disk */
    protected long getShardWritingBytes(IndexShard shard) {
        return shard.getWritingBytes();
    }

    /** Record that the given shard needs to write its indexing buffer. */
    protected void enqueueWriteIndexingBuffer(IndexShard shard) {
        if (pendingWriteIndexingBufferSet.add(shard)) {
            pendingWriteIndexingBufferQueue.addLast(shard);
        }
        // Else there is already a queued task for the same shard and there is no evidence that adding another one is required since we'd
        // need the first one to start running to know about the number of bytes still not being written.
    }

    /**
     * Write pending indexing buffers. This should run on indexing threads in order to naturally apply back pressure on indexing. Lucene has
     * similar logic in DocumentsWriter#postUpdate.
     */
    private boolean writePendingIndexingBuffers() {
        boolean wrotePendingIndexingBuffer = false;
        for (IndexShard shard = pendingWriteIndexingBufferQueue.pollFirst(); shard != null; shard = pendingWriteIndexingBufferQueue
            .pollFirst()) {
            // Remove the shard from the set first, so that multiple threads can run writeIndexingBuffer concurrently on the same shard.
            pendingWriteIndexingBufferSet.remove(shard);
            // Calculate the time taken to write the indexing buffers so it can be accounted for in the index write load
            long startTime = System.nanoTime();
            shard.writeIndexingBuffer();
            long took = System.nanoTime() - startTime;
            shard.addWriteIndexBuffersToIndexThreadsTime(took);
            wrotePendingIndexingBuffer = true;
        }
        return wrotePendingIndexingBuffer;
    }

    private void writePendingIndexingBuffersAsync() {
        for (IndexShard shard = pendingWriteIndexingBufferQueue.pollFirst(); shard != null; shard = pendingWriteIndexingBufferQueue
            .pollFirst()) {
            final IndexShard finalShard = shard;
            threadPool.executor(ThreadPool.Names.REFRESH).execute(() -> {
                // Remove the shard from the set first, so that multiple threads can run writeIndexingBuffer concurrently on the same shard.
                pendingWriteIndexingBufferSet.remove(finalShard);
                finalShard.writeIndexingBuffer();
            });
        }
    }

    /** force checker to run now */
    void forceCheck() {
        statusChecker.run();
    }

    /** Asks this shard to throttle indexing to one thread. If the PAUSE_INDEXING_ON_THROTTLE seeting is set to true,
     * throttling will pause indexing completely for the throttled shard.
     */
    protected void activateThrottling(IndexShard shard) {
        shard.activateThrottling();
    }

    /** Asks this shard to stop throttling indexing to one thread */
    protected void deactivateThrottling(IndexShard shard) {
        shard.deactivateThrottling();
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        postOperation(index, result);
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        postOperation(delete, result);
    }

    private void postOperation(Engine.Operation operation, Engine.Result result) {
        recordOperationBytes(operation, result);
        // Piggy back on indexing threads to write segments. We're not submitting a task to the index threadpool because we want memory to
        // be reclaimed rapidly. This has the downside of increasing the latency of _bulk requests though. Lucene does the same thing in
        // DocumentsWriter#postUpdate, flushing a segment because the size limit on the RAM buffer was reached happens on the call to
        // IndexWriter#addDocument.

        while (writePendingIndexingBuffers()) {
            // If we just wrote segments, then run the checker again if not already running to check if we released enough memory.
            if (statusChecker.tryRun() == false) {
                break;
            }
        }
    }

    /** called by IndexShard to record estimated bytes written to translog for the operation */
    private void recordOperationBytes(Engine.Operation operation, Engine.Result result) {
        if (result.getResultType() == Engine.Result.Type.SUCCESS) {
            statusChecker.bytesWritten(operation.estimatedSizeInBytes());
        }
    }

    private static final class ShardAndBytesUsed {
        final long bytesUsed;
        final IndexShard shard;

        ShardAndBytesUsed(long bytesUsed, IndexShard shard) {
            this.bytesUsed = bytesUsed;
            this.shard = shard;
        }

    }

    /** not static because we need access to many fields/methods from our containing class (IMC): */
    final class ShardsIndicesStatusChecker implements Runnable {

        final AtomicLong bytesWrittenSinceCheck = new AtomicLong();
        final ReentrantLock runLock = new ReentrantLock();
        // Last shard ID whose indexing buffer was written. We keep track of it to be able to go over shards in a round-robin fashion.
        private ShardId lastShardId = null;

        /** Shard calls this on each indexing/delete op */
        public void bytesWritten(int bytes) {
            long totalBytes = bytesWrittenSinceCheck.addAndGet(bytes);
            assert totalBytes >= 0;
            while (totalBytes > indexingBuffer / 128) {

                if (runLock.tryLock()) {
                    try {
                        // Must pull this again because it may have changed since we first checked:
                        totalBytes = bytesWrittenSinceCheck.get();
                        if (totalBytes > indexingBuffer / 128) {
                            bytesWrittenSinceCheck.addAndGet(-totalBytes);
                            // NOTE: this is only an approximate check, because bytes written is to the translog,
                            // vs indexing memory buffer which is typically smaller but can be larger in extreme
                            // cases (many unique terms). This logic is here only as a safety against thread
                            // starvation or too infrequent checking, to ensure we are still checking periodically,
                            // in proportion to bytes processed by indexing:
                            runUnlocked();
                        }
                    } finally {
                        runLock.unlock();
                    }

                    // Must get it again since other threads could have increased it while we were in runUnlocked
                    totalBytes = bytesWrittenSinceCheck.get();
                } else {
                    // Another thread beat us to it: let them do all the work, yay!
                    break;
                }
            }
        }

        public boolean tryRun() {
            if (runLock.tryLock()) {
                try {
                    runUnlocked();
                } finally {
                    runLock.unlock();
                }
                return true;
            } else {
                return false;
            }
        }

        @Override
        public void run() {
            // If there are any remainders from the previous check, schedule them now. Most of the time, indexing threads would have taken
            // care of these indexing buffers before, and we wouldn't need to do it here.
            writePendingIndexingBuffersAsync();
            runLock.lock();
            try {
                runUnlocked();
            } finally {
                runLock.unlock();
            }
        }

        private void runUnlocked() {
            assert runLock.isHeldByCurrentThread() : "ShardsIndicesStatusChecker#runUnlocked must always run under the run lock";
            // NOTE: even if we hit an errant exc here, our ThreadPool.scheduledWithFixedDelay will log the exception and re-invoke us
            // again, on schedule

            // First pass to sum up how much heap all shards' indexing buffers are using now, and how many bytes they are currently moving
            // to disk:
            long totalBytesUsed = 0;
            long totalBytesWriting = 0;
            for (IndexShard shard : availableShards()) {

                // Give shard a chance to transition to inactive so we can flush
                checkIdle(shard, inactiveTime.nanos());

                // How many bytes this shard is currently (async'd) moving from heap to disk:
                long shardWritingBytes = getShardWritingBytes(shard);

                // How many heap bytes this shard is currently using
                long shardBytesUsed = getIndexBufferRAMBytesUsed(shard);

                shardBytesUsed -= shardWritingBytes;
                totalBytesWriting += shardWritingBytes;

                // If the refresh completed just after we pulled shardWritingBytes and before we pulled shardBytesUsed, then we could
                // have a negative value here. So we just skip this shard since that means it's now using very little heap:
                if (shardBytesUsed < 0) {
                    continue;
                }

                totalBytesUsed += shardBytesUsed;
            }

            if (logger.isTraceEnabled()) {
                logger.trace(
                    "total indexing heap bytes used [{}] vs {} [{}], currently writing bytes [{}]",
                    ByteSizeValue.ofBytes(totalBytesUsed),
                    INDEX_BUFFER_SIZE_SETTING.getKey(),
                    indexingBuffer,
                    ByteSizeValue.ofBytes(totalBytesWriting)
                );
            }

            // If we are using more than 50% of our budget across both indexing buffer and bytes we are still moving to disk, then we now
            // throttle the top shards to send back-pressure to ongoing indexing:
            boolean doThrottle = (totalBytesWriting + totalBytesUsed) > 1.5 * indexingBuffer;

            if (totalBytesUsed > indexingBuffer) {
                // OK we are now over-budget; fill the priority queue and ask largest shard(s) to refresh:
                List<ShardAndBytesUsed> queue = new ArrayList<>();

                for (IndexShard shard : availableShards()) {
                    // How many bytes this shard is currently (async'd) moving from heap to disk:
                    long shardWritingBytes = getShardWritingBytes(shard);

                    // How many heap bytes this shard is currently using
                    long shardBytesUsed = getIndexBufferRAMBytesUsed(shard);

                    // Only count up bytes not already being refreshed:
                    shardBytesUsed -= shardWritingBytes;

                    // If the refresh completed just after we pulled shardWritingBytes and before we pulled shardBytesUsed, then we could
                    // have a negative value here. So we just skip this shard since that means it's now using very little heap:
                    if (shardBytesUsed < 0) {
                        continue;
                    }

                    if (shardBytesUsed > 0) {
                        if (logger.isTraceEnabled()) {
                            if (shardWritingBytes != 0) {
                                logger.trace(
                                    "shard [{}] is using [{}] heap, writing [{}] heap",
                                    shard.shardId(),
                                    shardBytesUsed,
                                    shardWritingBytes
                                );
                            } else {
                                logger.trace("shard [{}] is using [{}] heap, not writing any bytes", shard.shardId(), shardBytesUsed);
                            }
                        }
                        queue.add(new ShardAndBytesUsed(shardBytesUsed, shard));
                    }
                }

                logger.debug(
                    "now write some indexing buffers: total indexing heap bytes used [{}] vs {} [{}], "
                        + "currently writing bytes [{}], [{}] shards with non-zero indexing buffer",
                    ByteSizeValue.ofBytes(totalBytesUsed),
                    INDEX_BUFFER_SIZE_SETTING.getKey(),
                    indexingBuffer,
                    ByteSizeValue.ofBytes(totalBytesWriting),
                    queue.size()
                );

                // What is the best order to go over shards and reclaim memory usage? Interestingly, picking random shards performs _much_
                // better than picking the largest shard when trying to optimize for the elastic/logs Rally track. One explanation for this
                // is that Lucene's IndexWriter creates new pending segments in memory in order to satisfy indexing concurrency. E.g. if N
                // indexing threads suddenly index into the same IndexWriter, then the IndexWriter will have N pending segments in memory.
                // However, it's likely that indexing concurrency is not constant on a per-shard basis, especially when indexing into many
                // shards concurrently. So there are chances that if we flush a single segment now, then it won't be re-created shortly
                // because the peak indexing concurrency is rarely observed, and we end up indexing into fewer pending segments globally on
                // average, which in-turn reduces the total number of segments that get produced, and also reduces merging.
                // The downside of picking the shard that has the biggest indexing buffer is that it is often also the shard that has the
                // highest ingestion rate, and thus it is also the shard that is the most likely to re-create a new pending segment in the
                // very near future after one segment has been flushed.

                // We want to go over shards in a round-robin fashion across calls to #runUnlocked. First sort shards by something stable
                // like the shard ID.
                queue.sort(Comparator.comparing(shardAndBytes -> shardAndBytes.shard.shardId()));
                if (lastShardId != null) {
                    // Then rotate the list so that the first shard that is greater than the ID of the last shard whose indexing buffer was
                    // written comes first.
                    int nextShardIdIndex = 0;
                    for (ShardAndBytesUsed shardAndBytes : queue) {
                        if (shardAndBytes.shard.shardId().compareTo(lastShardId) > 0) {
                            break;
                        }
                        nextShardIdIndex++;
                    }
                    Collections.rotate(queue, -nextShardIdIndex);
                }

                for (ShardAndBytesUsed shardAndBytesUsed : queue) {
                    logger.debug(
                        "write indexing buffer to disk for shard [{}] to free up its [{}] indexing buffer",
                        shardAndBytesUsed.shard.shardId(),
                        ByteSizeValue.ofBytes(shardAndBytesUsed.bytesUsed)
                    );
                    enqueueWriteIndexingBuffer(shardAndBytesUsed.shard);
                    totalBytesUsed -= shardAndBytesUsed.bytesUsed;
                    lastShardId = shardAndBytesUsed.shard.shardId();
                    if (doThrottle && throttled.contains(shardAndBytesUsed.shard) == false) {
                        logger.debug(
                            "now throttling indexing for shard [{}]: segment writing can't keep up",
                            shardAndBytesUsed.shard.shardId()
                        );
                        throttled.add(shardAndBytesUsed.shard);
                        activateThrottling(shardAndBytesUsed.shard);
                    }
                    if (totalBytesUsed <= indexingBuffer) {
                        break;
                    }
                }

            }

            if (doThrottle == false) {
                for (IndexShard shard : throttled) {
                    logger.info("stop throttling indexing for shard [{}]", shard.shardId());
                    deactivateThrottling(shard);
                }
                throttled.clear();
            }
        }
    }

    /**
     * ask this shard to check now whether it is inactive, and reduces its indexing buffer if so.
     */
    protected void checkIdle(IndexShard shard, long inactiveTimeNS) {
        try {
            shard.flushOnIdle(inactiveTimeNS);
        } catch (AlreadyClosedException e) {
            logger.trace(() -> "ignore exception while checking if shard " + shard.shardId() + " is inactive", e);
        }
    }
}
