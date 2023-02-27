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

package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.ObjectStoreService;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.BufferedChecksumStreamOutput;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;

public class TranslogReplicator extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(TranslogReplicator.class);

    private static final TimeValue FLUSH_CHECK_INTERVAL = TimeValue.timeValueMillis(50);

    public static final Setting<TimeValue> FLUSH_RETRY_INITIAL_DELAY_SETTING = Setting.timeSetting(
        "stateless.translog.flush.retry.initial_delay",
        timeValueMillis(50),
        timeValueMillis(10),
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> FLUSH_RETRY_TIMEOUT_SETTING = Setting.timeSetting(
        "stateless.translog.flush.retry.timeout",
        timeValueSeconds(60),
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> FLUSH_INTERVAL_SETTING = Setting.timeSetting(
        "stateless.translog.flush.interval",
        timeValueMillis(200),
        timeValueMillis(10),
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> FLUSH_SIZE_SETTING = Setting.byteSizeSetting(
        "stateless.translog.flush_size",
        ByteSizeValue.ofMb(16),
        ByteSizeValue.ofBytes(0),
        ByteSizeValue.ofBytes(Long.MAX_VALUE),
        Setting.Property.NodeScope
    );

    private volatile BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
    private final ObjectStoreService objectStoreService;
    private final ThreadPool threadPool;
    private final ConcurrentHashMap<ShardId, ShardSyncState> shardSyncStateByShardId = new ConcurrentHashMap<>();
    private final Object generateFlushLock = new Object();
    private final AtomicLong fileName = new AtomicLong(0);

    private final AtomicLong lastFlushTime;

    private final TimeValue flushRetryInitialDelay;
    private final TimeValue flushRetryTimeout;
    private final TimeValue flushInterval;
    private final ByteSizeValue flushSize;

    public TranslogReplicator(final ThreadPool threadPool, final Settings settings, final ObjectStoreService objectStoreService) {
        this.threadPool = threadPool;
        this.objectStoreService = objectStoreService;
        this.flushRetryInitialDelay = FLUSH_RETRY_INITIAL_DELAY_SETTING.get(settings);
        this.flushRetryTimeout = FLUSH_RETRY_TIMEOUT_SETTING.get(settings);
        this.flushInterval = FLUSH_INTERVAL_SETTING.get(settings);
        this.flushSize = FLUSH_SIZE_SETTING.get(settings);
        this.lastFlushTime = new AtomicLong(getCurrentTimeMillis());
    }

    public void setBigArrays(BigArrays bigArrays) {
        this.bigArrays = bigArrays;
    }

    public BigArrays bigArrays() {
        return bigArrays;
    }

    @Override
    protected void doStart() {
        threadPool.scheduleWithFixedDelay(new AbstractRunnable() {
            @Override
            protected void doRun() throws IOException {
                if (isFlushIntervalReached() || isFlushSizeReached()) {
                    flush();
                }
            }

            private boolean isFlushIntervalReached() {
                return lastFlushTime.get() + flushInterval.millis() <= getCurrentTimeMillis();
            }

            private boolean isFlushSizeReached() {
                return getCurrentBufferSize() >= flushSize.getBytes();
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Unexpected exception when running translog replication task", e);
            }
        }, FLUSH_CHECK_INTERVAL, ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {
        shardSyncStateByShardId.values().forEach(ShardSyncState::close);
        // TODO: Finish listeners
    }

    public void add(final ShardId shardId, final BytesReference data, final long seqNo, final Translog.Location location) {
        try {
            shardSyncStateByShardId.computeIfAbsent(shardId, (s) -> new ShardSyncState()).writeToBuffer(data, seqNo, location);
        } catch (IOException e) {
            // TODO: IOException is required by the interface of BytesReference#write. However, it should never throw. If it were to throw,
            // this exception would propogate to the TranslogWriter and I think fail the engine. However, we should discuss whether this is
            // enough protection.
            assert false;
            throw new UncheckedIOException(e);
        }
    }

    public void sync(final ShardId shardId, Translog.Location location, ActionListener<Void> listener) {
        shardSyncStateByShardId.computeIfAbsent(shardId, (k) -> new ShardSyncState())
            .ensureSynced(new Translog.Location(location.generation, location.translogLocation + location.size, 0), listener);
    }

    private void flush() throws IOException {
        var translog = createCompoundTranslog();
        if (translog != null) {
            uploadCompoundTranslog(translog);
        }
    }

    private long getCurrentTimeMillis() {
        return threadPool.rawRelativeTimeInMillis();
    }

    private long getCurrentBufferSize() {
        long size = 0;
        for (ShardSyncState state : shardSyncStateByShardId.values()) {
            size += state.currentBufferSize();
        }
        return size;
    }

    private CompoundTranslog createCompoundTranslog() throws IOException {
        synchronized (generateFlushLock) {
            lastFlushTime.set(getCurrentTimeMillis());
            var checkpoints = new HashMap<ShardId, TranslogMetadata>();
            var onComplete = new ArrayList<Releasable>();

            var compoundTranslog = new ReleasableBytesStreamOutput(bigArrays);
            var header = new ReleasableBytesStreamOutput(bigArrays);

            onComplete.add(compoundTranslog);
            onComplete.add(header);

            for (var entry : shardSyncStateByShardId.entrySet()) {
                ShardId shardId = entry.getKey();
                ShardSyncState state = entry.getValue();
                BufferState buffer = state.pollBufferForSync();
                if (buffer == null) {
                    continue;
                }

                Translog.Location lastOpLocation = buffer.location;
                Translog.Location syncedLocation = new Translog.Location(
                    lastOpLocation.generation,
                    lastOpLocation.translogLocation + lastOpLocation.size,
                    0
                );

                state.markSyncStarting(syncedLocation);
                onComplete.add(() -> state.markSyncFinished(syncedLocation));

                long position = compoundTranslog.position();
                buffer.data.bytes().writeTo(compoundTranslog);
                long size = compoundTranslog.position() - position;
                checkpoints.put(shardId, new TranslogMetadata(position, size, buffer.minSeqNo, buffer.maxSeqNo, buffer.totalOps));

                buffer.close();
            }

            if (checkpoints.isEmpty()) {
                Releasables.close(onComplete);
                return null;
            }

            var bufferedChecksumStreamOutput = new BufferedChecksumStreamOutput(header);
            bufferedChecksumStreamOutput.writeMap(checkpoints);
            header.writeLong(bufferedChecksumStreamOutput.getChecksum());

            return new CompoundTranslog(
                Strings.format("%019d", fileName.getAndIncrement()),
                CompositeBytesReference.of(header.bytes(), compoundTranslog.bytes()),
                onComplete
            );
        }
    }

    private void uploadCompoundTranslog(CompoundTranslog translog) {
        new RetryableAction<Void>(
            org.apache.logging.log4j.LogManager.getLogger(TranslogReplicator.class),
            threadPool,
            flushRetryInitialDelay,
            flushRetryTimeout,
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    translog.close();
                }

                @Override
                public void onFailure(Exception e) {
                    translog.close();
                    logger.error(() -> "Failed to upload translog file [" + translog.name() + "]", e);
                    // TODO all retry attempts exhausted. Fail this indexing shard
                }
            }
        ) {
            @Override
            public void tryAction(ActionListener<Void> listener) {
                objectStoreService.uploadTranslogFile(translog.name(), translog.data(), listener);
            }

            @Override
            public boolean shouldRetry(Exception e) {
                return e instanceof IOException;
            }
        }.run();
    }

    private record CompoundTranslog(String name, BytesReference data, List<Releasable> onComplete) implements Releasable {

        @Override
        public void close() {
            Releasables.close(onComplete);
        }
    }

    private static class BufferState implements Releasable {

        private final ReleasableBytesStreamOutput data;
        private long minSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        private long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        private long totalOps = 0;
        private Translog.Location location = null;

        private BufferState(ReleasableBytesStreamOutput data) {
            this.data = data;
        }

        public final void append(BytesReference data, long seqNo, Translog.Location location) throws IOException {
            data.writeTo(this.data);
            minSeqNo = SequenceNumbers.min(minSeqNo, seqNo);
            maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNo);
            totalOps++;
            this.location = location;
        }

        @Override
        public void close() {
            data.close();
        }
    }

    private class ShardSyncState implements Releasable {

        private final Object bufferLock = new Object();
        private final TreeMap<Translog.Location, Boolean> ongoingSyncs = new TreeMap<>();
        private final PriorityQueue<SyncListener> listeners = new PriorityQueue<>();
        private volatile Translog.Location syncedLocation = new Translog.Location(0, 0, 0);
        private BufferState bufferState = null;

        private synchronized void markSyncStarting(Translog.Location location) {
            synchronized (ongoingSyncs) {
                ongoingSyncs.put(location, false);
            }
        }

        private void ensureSynced(Translog.Location location, ActionListener<Void> listener) {
            boolean completeListener = true;
            if (location.compareTo(syncedLocation) > 0) {
                synchronized (listeners) {
                    if (location.compareTo(syncedLocation) > 0) {
                        listeners.add(new SyncListener(location, listener));
                        completeListener = false;
                    }
                }
            }

            if (completeListener) {
                // TODO: Error Handling
                listener.onResponse(null);
            }
        }

        public void markSyncFinished(Translog.Location newlySyncedLocation) {
            boolean completeListeners = false;
            synchronized (ongoingSyncs) {
                Translog.Location firstLocation = ongoingSyncs.firstKey();
                if (firstLocation.equals(newlySyncedLocation)) {
                    ongoingSyncs.pollFirstEntry();
                    syncedLocation = firstLocation;

                    Iterator<Map.Entry<Translog.Location, Boolean>> iterator = ongoingSyncs.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<Translog.Location, Boolean> entry = iterator.next();
                        if (entry.getValue()) {
                            syncedLocation = entry.getKey();
                            iterator.remove();
                        } else {
                            break;
                        }
                    }
                    completeListeners = true;
                } else {
                    Boolean previousValue = ongoingSyncs.put(newlySyncedLocation, true);
                    assert previousValue != null && previousValue == false;
                }
            }

            if (completeListeners) {
                ArrayList<SyncListener> toComplete = new ArrayList<>();
                synchronized (listeners) {
                    SyncListener listener;
                    while ((listener = listeners.peek()) != null && syncedLocation.compareTo(listener.location) >= 0) {
                        toComplete.add(listener);
                        listeners.poll();
                    }
                }
                for (SyncListener listener : toComplete) {
                    // TODO: Error handling
                    listener.listener().onResponse(null);
                }
            }

        }

        public void writeToBuffer(BytesReference data, long seqNo, Translog.Location location) throws IOException {
            synchronized (bufferLock) {
                if (bufferState == null) {
                    bufferState = new BufferState(new ReleasableBytesStreamOutput(bigArrays));
                } else {
                    assert location.compareTo(bufferState.location) >= 0;
                }
                bufferState.append(data, seqNo, location);
            }
        }

        public long currentBufferSize() {
            synchronized (bufferLock) {
                return bufferState != null ? bufferState.data.size() : 0L;
            }
        }

        public BufferState pollBufferForSync() {
            synchronized (bufferLock) {
                BufferState toReturn = bufferState;
                bufferState = null;
                return toReturn;
            }
        }

        @Override
        public void close() {
            Releasables.close(bufferState);
        }

        private record SyncListener(Translog.Location location, ActionListener<Void> listener) implements Comparable<SyncListener> {

            @Override
            public int compareTo(SyncListener o) {
                return location.compareTo(o.location);
            }
        }
    }
}
