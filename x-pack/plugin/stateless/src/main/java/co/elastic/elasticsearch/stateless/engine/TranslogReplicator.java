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

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.core.TimeValue.timeValueMillis;

public class TranslogReplicator extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(TranslogReplicator.class);

    private static final TimeValue FLUSH_CHECK_INTERVAL = TimeValue.timeValueMillis(50);

    public static final Setting<TimeValue> FLUSH_RETRY_INITIAL_DELAY_SETTING = Setting.timeSetting(
        "stateless.translog.flush.retry.initial_delay",
        timeValueMillis(50),
        timeValueMillis(10),
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
    private final ConcurrentHashMap<ShardId, ShardSyncState> shardSyncStates = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<CompoundTranslog> compoundTranslogs = new ConcurrentLinkedQueue<>();
    private final Object generateFlushLock = new Object();
    private final AtomicLong fileName = new AtomicLong(0);
    private final AtomicLong maxUploadedFileName = new AtomicLong(-1);
    private final AtomicLong lastFlushTime;
    private final TimeValue flushRetryInitialDelay;
    private final TimeValue flushInterval;
    private final ByteSizeValue flushSize;

    public TranslogReplicator(final ThreadPool threadPool, final Settings settings, final ObjectStoreService objectStoreService) {
        this.threadPool = threadPool;
        this.objectStoreService = objectStoreService;
        this.flushRetryInitialDelay = FLUSH_RETRY_INITIAL_DELAY_SETTING.get(settings);
        this.flushInterval = FLUSH_INTERVAL_SETTING.get(settings);
        this.flushSize = FLUSH_SIZE_SETTING.get(settings);
        this.lastFlushTime = new AtomicLong(getCurrentTimeMillis());
    }

    public void setBigArrays(BigArrays bigArrays) {
        this.bigArrays = bigArrays;
    }

    public long getMaxUploadedFile() {
        return maxUploadedFileName.get();
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
                    var translog = createCompoundTranslog();
                    if (translog != null) {
                        uploadCompoundTranslog(translog);
                    }
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
        compoundTranslogs.forEach(CompoundTranslog::close);
        shardSyncStates.values().forEach(ShardSyncState::close);
    }

    public void register(ShardId shardId) {
        var previous = shardSyncStates.put(shardId, new ShardSyncState(shardId));
        assert previous == null;
    }

    public void unregister(ShardId shardId) {
        var unregistered = shardSyncStates.remove(shardId);
        assert unregistered != null;
        unregistered.close();
    }

    public void add(final ShardId shardId, final BytesReference data, final long seqNo, final Translog.Location location) {
        try {
            ShardSyncState shardSyncState = getShardSyncStateSafe(shardId);
            shardSyncState.writeToBuffer(data, seqNo, location);
        } catch (IOException e) {
            // TODO: IOException is required by the interface of BytesReference#write. However, it should never throw. If it were to throw,
            // this exception would propogate to the TranslogWriter and I think fail the engine. However, we should discuss whether this is
            // enough protection.
            assert false;
            throw new UncheckedIOException(e);
        }
    }

    public boolean isSyncNeeded(final ShardId shardId) {
        ShardSyncState shardSyncState = getShardSyncStateSafe(shardId);
        return shardSyncState.currentBufferSize() > 0L || shardSyncState.ongoingSyncs.isEmpty() == false;
    }

    public void sync(final ShardId shardId, Translog.Location location, ActionListener<Void> listener) {
        ShardSyncState shardSyncState = getShardSyncStateSafe(shardId);
        shardSyncState.ensureSynced(new Translog.Location(location.generation, location.translogLocation + location.size, 0), listener);
    }

    public void syncAll(final ShardId shardId, ActionListener<Void> listener) {
        ShardSyncState shardSyncState = getShardSyncStateSafe(shardId);
        shardSyncState.waitForAllSynced(listener);
    }

    private ShardSyncState getShardSyncStateSafe(ShardId shardId) {
        ShardSyncState shardSyncState = shardSyncStates.get(shardId);
        if (shardSyncState == null) {
            throw alreadyClosedException(shardId);
        }
        return shardSyncState;
    }

    private long getCurrentTimeMillis() {
        return threadPool.rawRelativeTimeInMillis();
    }

    private long getCurrentBufferSize() {
        long size = 0;
        for (ShardSyncState state : shardSyncStates.values()) {
            size += state.currentBufferSize();
        }
        return size;
    }

    private CompoundTranslog createCompoundTranslog() throws IOException {
        synchronized (generateFlushLock) {
            long fileName = this.fileName.get();
            lastFlushTime.set(getCurrentTimeMillis());
            var checkpoints = new HashMap<ShardId, TranslogMetadata>();
            var onComplete = new ArrayList<Releasable>();
            var onSuccess = new ArrayList<Releasable>();

            var compoundTranslogStream = new ReleasableBytesStreamOutput(bigArrays);
            var headerStream = new ReleasableBytesStreamOutput(bigArrays);

            onComplete.add(compoundTranslogStream);
            onComplete.add(headerStream);

            for (var entry : shardSyncStates.entrySet()) {
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
                onSuccess.add(() -> {
                    maxUploadedFileName.getAndAccumulate(fileName, Math::max);
                    state.markSyncFinished(syncedLocation);
                });

                long position = compoundTranslogStream.position();
                buffer.data.bytes().writeTo(compoundTranslogStream);
                long size = compoundTranslogStream.position() - position;
                checkpoints.put(shardId, new TranslogMetadata(position, size, buffer.minSeqNo, buffer.maxSeqNo, buffer.totalOps));

                buffer.close();
            }

            if (checkpoints.isEmpty()) {
                Releasables.close(onComplete);
                return null;
            }

            // Write the header to the stream
            new CompoundTranslogHeader(checkpoints).writeToStore(headerStream);

            long beforeIncrement = this.fileName.getAndIncrement();
            assert beforeIncrement == fileName;
            CompoundTranslog compoundTranslog = new CompoundTranslog(
                Strings.format("%019d", fileName),
                CompositeBytesReference.of(headerStream.bytes(), compoundTranslogStream.bytes()),
                onSuccess,
                onComplete
            );
            compoundTranslogs.add(compoundTranslog);
            return compoundTranslog;
        }
    }

    private void uploadCompoundTranslog(CompoundTranslog translog) {
        new RetryableAction<Void>(
            org.apache.logging.log4j.LogManager.getLogger(TranslogReplicator.class),
            threadPool,
            flushRetryInitialDelay,
            TimeValue.timeValueMillis(Long.MAX_VALUE),
            TimeValue.timeValueSeconds(5),
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    translog.closeOnSuccess();
                    translog.close();
                    compoundTranslogs.remove(translog);
                }

                @Override
                public void onFailure(Exception e) {
                    translog.close();
                    compoundTranslogs.remove(translog);
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

    private record CompoundTranslog(String name, BytesReference data, List<Releasable> onSuccess, List<Releasable> onComplete)
        implements
            Releasable {

        @Override
        public void close() {
            Releasables.close(onComplete);
        }

        public void closeOnSuccess() {
            Releasables.close(onSuccess);
        }
    }

    private static class BufferState implements Releasable {

        private final ReleasableBytesStreamOutput data;
        private long minSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        private long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        private long totalOps = 0;
        private Translog.Location location;

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

        private final ShardId shardId;
        private final TreeMap<Translog.Location, Boolean> ongoingSyncs = new TreeMap<>();
        private final PriorityQueue<SyncListener> listeners = new PriorityQueue<>();
        private volatile Translog.Location syncedLocation = new Translog.Location(0, 0, 0);
        private final Object bufferLock = new Object();
        private BufferState bufferState = null;
        private volatile boolean isClosed = false;

        ShardSyncState(ShardId shardId) {
            this.shardId = shardId;
        }

        private synchronized void markSyncStarting(Translog.Location location) {
            synchronized (ongoingSyncs) {
                ongoingSyncs.put(location, false);
            }
        }

        private void waitForAllSynced(ActionListener<Void> listener) {
            Translog.Location location = null;

            // Check if there is a location from buffered data
            synchronized (bufferLock) {
                if (bufferState != null) {
                    Translog.Location lastOpLocation = bufferState.location;
                    location = new Translog.Location(lastOpLocation.generation, lastOpLocation.translogLocation + lastOpLocation.size, 0);
                }
            }
            // If a location has not been found from buffered data, check if there is one from ongoing syncs
            if (location == null) {
                synchronized (ongoingSyncs) {
                    Map.Entry<Translog.Location, Boolean> lastEntry = ongoingSyncs.lastEntry();
                    if (lastEntry != null) {
                        location = lastEntry.getKey();
                    }
                }
            }

            if (location != null) {
                ensureSynced(location, listener);
            } else {
                if (isClosed) {
                    listener.onFailure(alreadyClosedException(shardId));
                } else {
                    listener.onResponse(null);
                }
            }
        }

        private void ensureSynced(Translog.Location location, ActionListener<Void> listener) {
            boolean completeListener = true;
            boolean alreadyClosed = false;
            if (location.compareTo(syncedLocation) > 0) {
                synchronized (listeners) {
                    if (isClosed) {
                        alreadyClosed = true;
                    } else if (location.compareTo(syncedLocation) > 0) {
                        ContextPreservingActionListener<Void> contextPreservingActionListener = ContextPreservingActionListener
                            .wrapPreservingContext(listener, threadPool.getThreadContext());
                        listeners.add(new SyncListener(location, contextPreservingActionListener));
                        completeListener = false;
                    }
                }
            }

            if (completeListener) {
                if (alreadyClosed) {
                    listener.onFailure(alreadyClosedException(shardId));
                } else {
                    listener.onResponse(null);
                }
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
                var toComplete = new ArrayList<ActionListener<Void>>();
                synchronized (listeners) {
                    SyncListener listener;
                    while ((listener = listeners.peek()) != null && syncedLocation.compareTo(listener.location) >= 0) {
                        toComplete.add(listener);
                        listeners.poll();
                    }
                }
                ActionListener.onResponse(toComplete, null);
            }
        }

        public void writeToBuffer(BytesReference data, long seqNo, Translog.Location location) throws IOException {
            synchronized (bufferLock) {
                if (isClosed) {
                    throw alreadyClosedException(shardId);
                }
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
            final ArrayList<ActionListener<Void>> toComplete;
            isClosed = true;
            synchronized (listeners) {
                toComplete = new ArrayList<>(listeners);
                listeners.clear();
            }
            synchronized (bufferLock) {
                Releasables.close(bufferState);
                bufferState = null;
            }

            ActionListener.onFailure(toComplete, alreadyClosedException(shardId));
        }

        private record SyncListener(Translog.Location location, ActionListener<Void> listener)
            implements
                ActionListener<Void>,
                Comparable<SyncListener> {

            @Override
            public void onResponse(Void unused) {
                listener.onResponse(unused);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public int compareTo(SyncListener o) {
                return location.compareTo(o.location);
            }
        }
    }

    private static AlreadyClosedException alreadyClosedException(ShardId shardId) {
        return new AlreadyClosedException("The translog for shard [" + shardId + "] is already closed.");
    }
}
