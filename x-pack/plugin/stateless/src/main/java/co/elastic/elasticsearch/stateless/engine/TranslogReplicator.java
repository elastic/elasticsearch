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
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;

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
    private final StatelessClusterConsistencyService consistencyService;
    private final ToLongFunction<ShardId> currentPrimaryTerm;
    private final ThreadPool threadPool;
    private final NodeSyncState nodeState = new NodeSyncState();
    private final ConcurrentHashMap<ShardId, ShardSyncState> shardSyncStates = new ConcurrentHashMap<>();
    private final Object generateFlushLock = new Object();
    private final AtomicLong lastFlushTime;
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final TimeValue flushRetryInitialDelay;
    private final TimeValue flushInterval;
    private final ByteSizeValue flushSize;

    public TranslogReplicator(
        final ThreadPool threadPool,
        final Settings settings,
        final ObjectStoreService objectStoreService,
        final StatelessClusterConsistencyService consistencyService
    ) {

        this(threadPool, settings, objectStoreService, consistencyService, shardId -> {
            IndexMetadata index = consistencyService.state().metadata().index(shardId.getIndex());
            if (index == null) {
                return Long.MIN_VALUE;
            }
            return index.primaryTerm(shardId.getId());
        });
    }

    public TranslogReplicator(
        final ThreadPool threadPool,
        final Settings settings,
        final ObjectStoreService objectStoreService,
        final StatelessClusterConsistencyService consistencyService,
        final ToLongFunction<ShardId> currentPrimaryTerm
    ) {
        this.threadPool = threadPool;
        this.objectStoreService = objectStoreService;
        this.flushRetryInitialDelay = FLUSH_RETRY_INITIAL_DELAY_SETTING.get(settings);
        this.flushInterval = FLUSH_INTERVAL_SETTING.get(settings);
        this.flushSize = FLUSH_SIZE_SETTING.get(settings);
        this.consistencyService = consistencyService;
        this.currentPrimaryTerm = currentPrimaryTerm;
        this.lastFlushTime = new AtomicLong(getCurrentTimeMillis());
    }

    public void setBigArrays(BigArrays bigArrays) {
        this.bigArrays = bigArrays;
    }

    public long getMaxUploadedFile() {
        return nodeState.maxUploadedFileName.get();
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
        isOpen.set(false);
        nodeState.close();
        shardSyncStates.values().forEach(ShardSyncState::close);
    }

    public void register(ShardId shardId, long primaryTerm) {
        var previous = shardSyncStates.put(shardId, new ShardSyncState(shardId, primaryTerm));
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
        return shardSyncState.processedLocation.compareTo(shardSyncState.syncedLocation) > 0;
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
            long fileName = nodeState.compoundTranslogGeneration.get();
            lastFlushTime.set(getCurrentTimeMillis());
            var checkpoints = new HashMap<ShardId, TranslogMetadata>();
            var onComplete = new ArrayList<Releasable>();
            var syncedLocations = new HashMap<ShardId, ShardSyncState.SyncMarker>();

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

                syncedLocations.put(shardId, new ShardSyncState.SyncMarker(state.startingPrimaryTerm, syncedLocation));

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

            long beforeIncrement = nodeState.compoundTranslogGeneration.getAndIncrement();
            assert beforeIncrement == fileName;
            CompoundTranslogBytes compoundTranslogBytes = new CompoundTranslogBytes(
                CompositeBytesReference.of(headerStream.bytes(), compoundTranslogStream.bytes()),
                onComplete
            );
            return new CompoundTranslog(Strings.format("%019d", fileName), fileName, syncedLocations, compoundTranslogBytes);
        }
    }

    private class UploadTranslogTask extends RetryableAction<Void> implements Comparable<UploadTranslogTask> {

        private final CompoundTranslog translog;
        private final RefCounted bytesToClose;
        private volatile boolean isUploaded = false;

        private UploadTranslogTask(CompoundTranslog translog, ActionListener<Void> listener) {
            super(
                org.apache.logging.log4j.LogManager.getLogger(TranslogReplicator.class),
                threadPool,
                flushRetryInitialDelay,
                TimeValue.timeValueSeconds(5),
                TimeValue.MAX_VALUE,
                listener
            );
            this.translog = translog;
            this.bytesToClose = new AbstractRefCounted() {
                @Override
                protected void closeInternal() {
                    translog.bytes().close();
                }
            };
        }

        @Override
        public void tryAction(ActionListener<Void> listener) {
            // Retain the bytes to ensure that a cancel call does not corrupt them before upload
            if (bytesToClose.tryIncRef()) {
                objectStoreService.uploadTranslogFile(
                    translog.name(),
                    translog.bytes().data(),
                    ActionListener.releaseAfter(listener.delegateFailureAndWrap((l, v) -> {
                        isUploaded = true;
                        l.onResponse(null);

                    }), bytesToClose::decRef)
                );
            } else {
                listener.onFailure(new ElasticsearchException("Cannot acquire upload lock."));
            }
        }

        @Override
        public boolean shouldRetry(Exception e) {
            return isOpen.get();
        }

        @Override
        public void onFinished() {
            bytesToClose.decRef();
            super.onFinished();
        }

        @Override
        public int compareTo(UploadTranslogTask o) {
            return Long.compare(translog.generation(), o.translog.generation());
        }
    }

    private void uploadCompoundTranslog(CompoundTranslog translog) {
        UploadTranslogTask uploadTask = new UploadTranslogTask(translog, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                nodeState.markSyncFinished(translog.generation());
            }

            @Override
            public void onFailure(Exception e) {
                // We only fully fail when the translog replicator is shutting down
                logger.error(() -> "Failed to upload translog file [" + translog.name() + "] due to translog replicator shutdown", e);
                assert isOpen.get() == false;
            }
        });
        nodeState.markSyncStarting(uploadTask);
        uploadTask.run();
    }

    private record CompoundTranslog(
        String name,
        long generation,
        Map<ShardId, ShardSyncState.SyncMarker> syncedLocations,
        CompoundTranslogBytes bytes
    ) {}

    private record CompoundTranslogBytes(BytesReference data, List<Releasable> onComplete) implements Releasable {

        @Override
        public void close() {
            Releasables.close(onComplete);
        }
    }

    private class ValidateClusterStateTask extends RetryableAction<Void> implements Comparable<ValidateClusterStateTask> {

        private final long validateGeneration;
        private final ArrayList<CompoundTranslog> completedSyncs;

        private ValidateClusterStateTask(long validateGeneration, ArrayList<CompoundTranslog> completedSyncs) {
            super(
                org.apache.logging.log4j.LogManager.getLogger(TranslogReplicator.class),
                threadPool,
                flushRetryInitialDelay,
                TimeValue.timeValueSeconds(5),
                TimeValue.MAX_VALUE,
                new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        nodeState.markClusterStateValidateFinished(validateGeneration);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // We only fully fail when the translog replicator is shutting down
                        logger.info(() -> "Failed to validate cluster state due to translog replicator shutdown", e);
                    }
                }
            );
            this.validateGeneration = validateGeneration;
            this.completedSyncs = completedSyncs;
        }

        @Override
        public int compareTo(ValidateClusterStateTask o) {
            return Long.compare(validateGeneration, o.validateGeneration);
        }

        @Override
        public void tryAction(ActionListener<Void> listener) {
            consistencyService.ensureClusterStateConsistentWithRootBlob(listener, TimeValue.timeValueSeconds(30));
        }

        @Override
        public boolean shouldRetry(Exception e) {
            return isOpen.get();
        }
    }

    private class NodeSyncState {

        private final AtomicLong maxUploadedFileName = new AtomicLong(-1);

        private final AtomicLong compoundTranslogGeneration = new AtomicLong(0);
        private final PriorityQueue<UploadTranslogTask> ongoingSyncs = new PriorityQueue<>();

        private final AtomicLong validateClusterStateGeneration = new AtomicLong(0);
        private final PriorityQueue<ValidateClusterStateTask> ongoingValidateClusterState = new PriorityQueue<>();

        private void markSyncStarting(final UploadTranslogTask uploadTranslogTask) {
            synchronized (ongoingSyncs) {
                ongoingSyncs.add(uploadTranslogTask);
            }
        }

        private void markSyncFinished(long nodeTranslogGeneration) {
            maxUploadedFileName.getAndAccumulate(nodeTranslogGeneration, Math::max);
            // We lock on ongoingSyncs to ensure that we transition all synced translog files to the validate step.
            synchronized (ongoingSyncs) {
                if (ongoingSyncs.peek().translog.generation() == nodeTranslogGeneration) {
                    ArrayList<CompoundTranslog> completedSyncs = new ArrayList<>(2);
                    Iterator<UploadTranslogTask> iterator = ongoingSyncs.iterator();
                    while (iterator.hasNext()) {
                        UploadTranslogTask entry = iterator.next();
                        if (entry.isUploaded) {
                            completedSyncs.add(entry.translog);
                            iterator.remove();
                        } else {
                            break;
                        }
                    }

                    // Submit the cluster state validate under lock so that the validate generations increase in order.
                    triggerClusterStateValidate(completedSyncs);
                } else {
                    assert ongoingSyncs.stream().filter(t -> t.translog.generation == nodeTranslogGeneration).findAny().get().isUploaded;
                }
            }
        }

        private void triggerClusterStateValidate(ArrayList<CompoundTranslog> completedSyncs) {
            final ValidateClusterStateTask validateClusterStateTask;
            synchronized (ongoingValidateClusterState) {
                validateClusterStateTask = new ValidateClusterStateTask(validateClusterStateGeneration.getAndIncrement(), completedSyncs);
                ongoingValidateClusterState.add(validateClusterStateTask);
            }

            validateClusterStateTask.run();
        }

        private void markClusterStateValidateFinished(long validateGeneration) {
            HashSet<ShardSyncState> modifiedShardSyncedLocations = new HashSet<>();
            synchronized (ongoingValidateClusterState) {
                Iterator<ValidateClusterStateTask> iterator = ongoingValidateClusterState.iterator();
                while (iterator.hasNext()) {
                    ValidateClusterStateTask task = iterator.next();
                    // Complete any validate listeners with a generation less or equal to the completed generation
                    if (validateGeneration >= task.validateGeneration) {
                        task.completedSyncs.forEach(sync -> {
                            for (Map.Entry<ShardId, ShardSyncState.SyncMarker> entry : sync.syncedLocations().entrySet()) {
                                ShardSyncState shardSyncState = shardSyncStates.get(entry.getKey());
                                // If the shard sync state has been deregistered we can just ignore
                                if (shardSyncState != null) {
                                    ShardSyncState.SyncMarker syncMarker = entry.getValue();
                                    shardSyncState.markSyncFinished(syncMarker);
                                    modifiedShardSyncedLocations.add(shardSyncState);
                                }
                            }
                        });
                        iterator.remove();
                    } else {
                        break;
                    }
                }
            }
            for (ShardSyncState modifiedShardSyncedLocation : modifiedShardSyncedLocations) {
                modifiedShardSyncedLocation.notifyListeners();
            }
        }

        public void close() {
            synchronized (ongoingSyncs) {
                // Don't remove. Just cancel since this only happens on shutdown.
                ongoingSyncs.forEach(r -> r.cancel(new ElasticsearchException("Node shutting down")));
            }
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
        private final long startingPrimaryTerm;
        private final PriorityQueue<SyncListener> listeners = new PriorityQueue<>();
        private volatile Translog.Location processedLocation = new Translog.Location(0, 0, 0);
        private volatile Translog.Location syncedLocation = new Translog.Location(0, 0, 0);
        private final Object bufferLock = new Object();
        private BufferState bufferState = null;
        private volatile boolean isClosed = false;

        ShardSyncState(ShardId shardId, long primaryTerm) {
            this.shardId = shardId;
            this.startingPrimaryTerm = primaryTerm;
        }

        private void waitForAllSynced(ActionListener<Void> listener) {
            // Single volatile read
            Translog.Location processedLocationCopy = processedLocation;
            if (processedLocationCopy.compareTo(syncedLocation) > 0) {
                ensureSynced(processedLocationCopy, listener);
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

        public void markSyncFinished(SyncMarker syncMarker) {
            // If the primary term changed this shard will eventually be closed and the listeners will be failed at that point, so we can
            // ignore them here.
            if (syncMarker.primaryTerm() == currentPrimaryTerm.applyAsLong(shardId)) {
                assert syncMarker.location().compareTo(syncedLocation) > 0;
                syncedLocation = syncMarker.location();
            }
        }

        private void notifyListeners() {
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

        public void writeToBuffer(BytesReference data, long seqNo, Translog.Location location) throws IOException {
            synchronized (bufferLock) {
                if (isClosed) {
                    throw alreadyClosedException(shardId);
                }
                Translog.Location newProcessedLocation = new Translog.Location(
                    location.generation,
                    location.translogLocation + location.size,
                    0
                );
                assert newProcessedLocation.compareTo(processedLocation) > 0;
                processedLocation = newProcessedLocation;
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

        private record SyncMarker(long primaryTerm, Translog.Location location) {}

    }

    private static AlreadyClosedException alreadyClosedException(ShardId shardId) {
        return new AlreadyClosedException("The translog for shard [" + shardId + "] is already closed.");
    }
}
