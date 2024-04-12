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

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.lucene.IndexDirectory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.monitor.fs.FsProbe;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class IndexingDiskController extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(IndexingDiskController.class);

    /** How much of total disk space should be reserved before flushing shards (default: 20%) **/
    public static final Setting<RelativeByteSizeValue> INDEXING_DISK_RESERVED_BYTES_SETTING = new Setting<>(
        "indices.disk.reserved_bytes",
        "20%",
        (s) -> RelativeByteSizeValue.parseRelativeByteSizeValue(s, "indices.disk.reserved_bytes"),
        Setting.Property.NodeScope
    );

    /** How frequently we check disk usage (default: 5 seconds). */
    public static final Setting<TimeValue> INDEXING_DISK_INTERVAL_TIME_SETTING = Setting.timeSetting(
        "indices.disk.interval",
        TimeValue.timeValueSeconds(5),
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope
    );

    private final TimeValue interval;
    private final ThreadPool threadPool;
    private final NodeEnvironment.DataPath[] dataPaths;
    private final IndicesService indicesService;
    private final StatelessCommitService commitService;
    private final ShardsDiskUsageMonitor monitor;
    private final ByteSizeValue reservedBytes;

    /** Contains shards currently being flushed */
    private final Set<IndexShard> pendingFlush = ConcurrentCollections.newConcurrentSet();

    /** Contains shards currently being throttled because we can't flush segments to object store quickly enough */
    private final Set<IndexShard> throttled = new HashSet<>();

    // no need for volatile, accesses are synchronized in AbstractLifecycleComponent.start/stop methods
    private Scheduler.Cancellable scheduledMonitorFuture;

    public IndexingDiskController(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        IndicesService indicesService,
        StatelessCommitService commitService
    ) {
        this(settings, threadPool, environment.dataPaths(), indicesService, commitService);
    }

    private IndexingDiskController(
        Settings settings,
        ThreadPool threadPool,
        NodeEnvironment.DataPath[] dataPaths,
        IndicesService indicesService,
        StatelessCommitService commitService
    ) {
        this.interval = INDEXING_DISK_INTERVAL_TIME_SETTING.get(settings);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.dataPaths = Objects.requireNonNull(dataPaths);
        this.indicesService = Objects.requireNonNull(indicesService);
        this.commitService = Objects.requireNonNull(commitService);

        final ByteSizeValue reservedBytes;
        final ByteSizeValue totalBytes = totalBytes();

        var relativeDiskReservedBytes = INDEXING_DISK_RESERVED_BYTES_SETTING.get(settings);
        if (relativeDiskReservedBytes.isAbsolute()) {
            reservedBytes = relativeDiskReservedBytes.getAbsolute();
        } else {
            reservedBytes = relativeDiskReservedBytes.calculateValue(totalBytes, null);
        }
        if (reservedBytes.getBytes() >= totalBytes.getBytes()) {
            throw new IllegalStateException(
                "Reserved disk space ["
                    + reservedBytes
                    + " ("
                    + reservedBytes.getBytes()
                    + " bytes)] exceeds total disk space ["
                    + totalBytes
                    + " ("
                    + totalBytes.getBytes()
                    + " bytes)]"
            );
        }
        if (reservedBytes.getBytes() <= indicesService.getTotalIndexingBufferBytes().getBytes()) {
            throw new IllegalStateException(
                "Reserved disk space ["
                    + reservedBytes
                    + " ("
                    + reservedBytes.getBytes()
                    + " bytes)] must be larger than Lucene indexing buffer ["
                    + indicesService.getTotalIndexingBufferBytes()
                    + " ("
                    + indicesService.getTotalIndexingBufferBytes().getBytes()
                    + " bytes)]"
            );
        }
        this.reservedBytes = reservedBytes;
        if (interval.millis() > 0) {
            logger.info(
                "indexing disk controller will flush and throttle indexing shards "
                    + "if available disk space drops below [{}/{} bytes] on [{}/{}] total [indexing buffer size={}/{}]",
                reservedBytes,
                reservedBytes.getBytes(),
                totalBytes,
                totalBytes.getBytes(),
                indicesService.getTotalIndexingBufferBytes(),
                indicesService.getTotalIndexingBufferBytes().getBytes()
            );
            this.monitor = new ShardsDiskUsageMonitor();
        } else {
            logger.warn("indexing disk controller is disabled");
            this.monitor = null;
        }
    }

    @Override
    protected void doStart() {
        assert Thread.holdsLock(lifecycle);
        if (interval.millis() > 0) {
            scheduledMonitorFuture = threadPool.scheduleWithFixedDelay(monitor, interval, threadPool.generic());
        }
    }

    @Override
    protected void doStop() {
        assert Thread.holdsLock(lifecycle);
        if (scheduledMonitorFuture != null) {
            scheduledMonitorFuture.cancel();
        }
    }

    @Override
    protected void doClose() {
        assert Thread.holdsLock(lifecycle);
        throttled.clear();
    }

    private static final Predicate<IndexShard> FLUSHABLE_SHARD = indexShard -> {
        var state = indexShard.state();
        return state == IndexShardState.STARTED || state == IndexShardState.RECOVERING || state == IndexShardState.POST_RECOVERY;
    };

    private Stream<IndexShard> flushableShards() {
        return StreamSupport.stream(indicesService.spliterator(), false)
            .flatMap(indexService -> StreamSupport.stream(indexService.spliterator(), false))
            .filter(FLUSHABLE_SHARD);
    }

    private ByteSizeValue totalBytes() {
        long total = 0L;
        for (NodeEnvironment.DataPath dataPath : dataPaths) {
            try {
                total += FsProbe.getTotal(dataPath.fileStore);
            } catch (IOException e) {
                throw new IllegalStateException("Unable to get total size of filesystem [" + dataPath + ']');
            }
        }
        return ByteSizeValue.ofBytes(total);
    }

    // package private for testing
    long availableBytes() {
        long available = 0L;
        for (NodeEnvironment.DataPath dataPath : dataPaths) {
            try {
                available += FsProbe.getFSInfo(dataPath).getAvailable().getBytes(); // not cached
            } catch (IOException e) {
                logger.warn(() -> "Failed to get the number of bytes available on [" + dataPath + ']', e);
            }
        }
        return available;
    }

    ByteSizeValue getReservedBytes() {
        return reservedBytes;
    }

    record ShardDiskUsage(IndexShard shard, long directorySizeInBytes, long indexBufferRAMBytesUsed) {
        long totalSizeInBytes() {
            return directorySizeInBytes + indexBufferRAMBytesUsed;
        }
    }

    static final Comparator<ShardDiskUsage> LARGEST_SHARD_FIRST_COMPARATOR = (a, b) -> Long.compare(
        b.totalSizeInBytes(),
        a.totalSizeInBytes()
    );

    // package private for testing
    void runNow() {
        monitor.run();
    }

    /**
     * Note: this runnable is scheduled with ThreadPool.scheduleWithFixedDelay that wraps it into an AbstractRunnable, logs any thrown
     * exception thrown and always reschedules.
     */
    private class ShardsDiskUsageMonitor implements Runnable {

        @Override
        public void run() {
            if (lifecycleState() != Lifecycle.State.STARTED) {
                return;
            }
            final var shards = flushableShards().map(IndexingDiskController::shardDiskUsage)
                .filter(shardDiskUsage -> shardDiskUsage.totalSizeInBytes() > 0L)
                .toList();

            // compute the total number of available bytes across all data paths (note: indexing shards usually have 1 data path)
            final long availableBytes = availableBytes();

            // substract how many bytes will be moved from heap to disk in case of refresh/flush
            final long indexingBufferBytes = shards.stream().mapToLong(ShardDiskUsage::indexBufferRAMBytesUsed).sum();
            final long availableBytesAfterFlushes = Math.max(0L, availableBytes - indexingBufferBytes);

            // if there is less free disk space than we would like, we are going to flush the shards with the highest disk footprints
            // in the hope to upload enough local files to the object store and free up some bytes on disk. This is not immediate since
            // upload takes time and some files may still be required on disk (case of generational docs values files) so we'll also
            // throttle those shards to send back-pressure to indexing.
            final boolean doFlush = availableBytesAfterFlushes <= reservedBytes.getBytes();
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "estimated available disk space is {} bytes after potential flushes ({} bytes available on data path, {} bytes used by "
                        + "Lucene indexing buffer), {} limit for reserved disk space of {} bytes",
                    availableBytesAfterFlushes,
                    availableBytes,
                    indexingBufferBytes,
                    (doFlush ? "reaching" : "not reaching"),
                    reservedBytes
                );
            }
            if (doFlush) {
                final PriorityQueue<ShardDiskUsage> queue = new PriorityQueue<>(LARGEST_SHARD_FIRST_COMPARATOR);
                queue.addAll(shards);

                long estimatedFlushedBytes = 0L;
                while ((availableBytesAfterFlushes + estimatedFlushedBytes <= reservedBytes.getBytes()) && queue.isEmpty() == false) {
                    ShardDiskUsage largest = queue.poll();
                    var shardId = largest.shard().shardId();
                    if (commitService.hasPendingBccUploads(shardId) == false) {
                        maybeFlushShardAsync(largest);
                    }
                    estimatedFlushedBytes += largest.totalSizeInBytes();
                    if (throttled.contains(largest.shard) == false) {
                        logger.info("now throttling indexing for shard {}: available disk space reached the limit", shardId);
                        throttled.add(largest.shard);
                        largest.shard.activateThrottling();
                    }
                }
            } else {
                for (IndexShard shard : throttled) {
                    logger.info("stop throttling indexing for shard {}", shard.shardId());
                    shard.deactivateThrottling();
                }
                throttled.clear();
            }
        }
    }

    /**
     * Ask this shard to flush, in the background, in the hope to free a bit of disk space
     */
    protected void maybeFlushShardAsync(ShardDiskUsage shardDiskUsage) {
        final var shard = shardDiskUsage.shard;
        if (pendingFlush.add(shard)) {
            threadPool.executor(ThreadPool.Names.FLUSH).execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            " {} flushing shard with estimated size of {} bytes",
                            shard.shardId(),
                            ByteSizeValue.ofBytes(shardDiskUsage.totalSizeInBytes())
                        );
                    }
                    shard.flush(new FlushRequest().waitIfOngoing(false));
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof AlreadyClosedException == false) {
                        logger.warn(() -> "failed to flush shard [" + shard.shardId() + "]; ignoring", e);
                    }
                }

                @Override
                public void onAfter() {
                    pendingFlush.remove(shard);
                }
            });
        }
    }

    static ShardDiskUsage shardDiskUsage(IndexShard shard) {
        var engine = shard.getEngineOrNull();
        if (engine != null) {
            var directory = IndexDirectory.unwrapDirectory(engine.config().getStore().directory());
            assert directory != null : shard.shardId();

            long directorySizeInBytes = directory.estimateSizeInBytes();
            long indexBufferRAMBytesUsed = shard.getIndexBufferRAMBytesUsed();
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "shard {} is using [{}] bytes on disk and [{}] bytes in heap",
                    shard.shardId(),
                    directorySizeInBytes,
                    indexBufferRAMBytesUsed
                );
            }
            return new ShardDiskUsage(shard, directorySizeInBytes, indexBufferRAMBytesUsed);
        }
        return new ShardDiskUsage(shard, 0L, 0L);
    }
}
