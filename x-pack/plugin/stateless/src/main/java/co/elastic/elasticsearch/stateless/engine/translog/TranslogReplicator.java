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

package co.elastic.elasticsearch.stateless.engine.translog;

import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterShardHealth;
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
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.function.ToLongFunction;

import static org.elasticsearch.core.Strings.format;
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
    private final Executor executor;
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
        this.executor = threadPool.generic();
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
        return nodeState.maxUploadedGeneration.get();
    }

    public void markShardCommitUploaded(ShardId shardId, long translogStartFile) {
        ShardSyncState shardSyncState = shardSyncStates.get(shardId);
        if (shardSyncState != null) {
            shardSyncState.markCommitUploaded(translogStartFile);
        }
    }

    public Set<BlobTranslogFile> getUploadingTranslogFiles() {
        return Set.copyOf(nodeState.uploadingTranslogFiles.values());
    }

    public Set<BlobTranslogFile> getActiveTranslogFiles() {
        return Set.copyOf(nodeState.activeTranslogFiles);
    }

    public Set<BlobTranslogFile> getTranslogFilesToDelete() {
        return Set.copyOf(nodeState.translogFilesToDelete);
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
                    UploadTranslogTask uploadTask = createCompoundTranslogSync();
                    if (uploadTask != null) {
                        uploadTask.run();
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
                logger.error("unexpected exception when running translog replication task", e);
                assert false : e;
            }

            @Override
            public void onRejection(Exception e) {
                if (e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown()) {
                    logger.debug("translog replication task rejected due to shutdown");
                } else {
                    onFailure(e);
                }
            }
        }, FLUSH_CHECK_INTERVAL, executor);
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {
        isOpen.set(false);
        nodeState.close();
        shardSyncStates.values().forEach(ShardSyncState::close);
    }

    public void register(ShardId shardId, long primaryTerm, LongConsumer persistedSeqNoConsumer) {
        logger.debug(() -> format("shard %s registered with translog replicator", shardId));
        var previous = shardSyncStates.put(
            shardId,
            new ShardSyncState(
                shardId,
                primaryTerm,
                () -> currentPrimaryTerm.applyAsLong(shardId),
                persistedSeqNoConsumer,
                threadPool.getThreadContext(),
                bigArrays
            )
        );
        assert previous == null;
    }

    public void unregister(ShardId shardId) {
        var unregistered = shardSyncStates.remove(shardId);
        logger.debug(() -> format("shard %s unregistered with translog replicator", shardId));
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
        return shardSyncState.syncNeeded();
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
            throw ShardSyncState.alreadyClosedException(shardId);
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

    private UploadTranslogTask createCompoundTranslogSync() throws IOException {
        synchronized (generateFlushLock) {
            long fileName = nodeState.compoundTranslogGeneration.get();
            lastFlushTime.set(getCurrentTimeMillis());
            var metadata = new HashMap<ShardId, TranslogMetadata>();
            var syncedLocations = new HashMap<ShardId, ShardSyncState.SyncMarker>();

            var compoundTranslogStream = new ReleasableBytesStreamOutput(bigArrays);
            var headerStream = new ReleasableBytesStreamOutput(bigArrays);

            boolean dataToSync = false;
            for (var entry : shardSyncStates.entrySet()) {
                ShardId shardId = entry.getKey();
                ShardSyncState state = entry.getValue();
                ShardSyncState.SyncState syncState = state.pollSync(fileName);

                long position = compoundTranslogStream.position();
                if (syncState.buffer() != null) {
                    dataToSync = true;
                    ShardSyncState.BufferState buffer = syncState.buffer();
                    buffer.data().bytes().writeTo(compoundTranslogStream);
                    metadata.put(shardId, syncState.metadata(position, compoundTranslogStream.position() - position));
                    syncedLocations.put(shardId, buffer.syncMarker());
                    buffer.close();
                } else {
                    metadata.put(shardId, syncState.metadata(position, compoundTranslogStream.position() - position));
                }
            }

            if (dataToSync == false) {
                Releasables.close(headerStream, compoundTranslogStream);
                return null;
            }

            // Write the header to the stream
            new CompoundTranslogHeader(metadata).writeToStore(headerStream);

            long beforeIncrement = nodeState.compoundTranslogGeneration.getAndIncrement();
            assert beforeIncrement == fileName;
            CompoundTranslogBytes compoundTranslogBytes = new CompoundTranslogBytes(
                CompositeBytesReference.of(headerStream.bytes(), compoundTranslogStream.bytes()),
                () -> Releasables.close(headerStream, compoundTranslogStream)
            );
            CompoundTranslogMetadata compoundMetadata = new CompoundTranslogMetadata(
                Strings.format("%019d", fileName),
                fileName,
                metadata,
                syncedLocations
            );
            return createUploadTask(new CompoundTranslog(compoundMetadata, compoundTranslogBytes));
        }
    }

    private class UploadTranslogTask extends RetryableAction<UploadTranslogTask> implements Comparable<UploadTranslogTask> {

        private final CompoundTranslog translog;
        private final RefCounted bytesToClose;
        private int uploadTryNumber = 0;
        private volatile boolean isUploaded = false;

        private UploadTranslogTask(CompoundTranslog translog, ActionListener<UploadTranslogTask> listener) {
            super(
                org.apache.logging.log4j.LogManager.getLogger(TranslogReplicator.class),
                threadPool,
                flushRetryInitialDelay,
                TimeValue.timeValueSeconds(5),
                TimeValue.MAX_VALUE,
                listener,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
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
        public void tryAction(ActionListener<UploadTranslogTask> listener) {
            ++uploadTryNumber;
            // Retain the bytes to ensure that a cancel call does not corrupt them before upload
            if (bytesToClose.tryIncRef()) {
                logger.trace(() -> format("attempt [%s] to upload translog file [%s]", uploadTryNumber, translog.metadata().name()));
                objectStoreService.uploadTranslogFile(
                    translog.metadata().name(),
                    translog.bytes().data(),
                    ActionListener.releaseAfter(ActionListener.wrap(unused -> {
                        logger.debug(() -> format("uploaded translog file [%s]", translog.metadata().name()));
                        listener.onResponse(this);

                    }, e -> {
                        org.apache.logging.log4j.util.Supplier<Object> messageSupplier = () -> format(
                            "failed attempt [%s] to upload translog file [%s] to object store, will retry",
                            uploadTryNumber,
                            translog.metadata().name()
                        );
                        if (uploadTryNumber == 5) {
                            logger.warn(messageSupplier, e);
                        } else {
                            logger.info(messageSupplier, e);
                        }
                        listener.onFailure(e);

                    }), bytesToClose::decRef)
                );
            } else {
                listener.onFailure(new ElasticsearchException("Cannot acquire upload lock."));
            }
        }

        private void markUploaded() {
            assert Thread.holdsLock(nodeState.ongoingUploads);
            assert isUploaded == false;
            isUploaded = true;
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
            return Long.compare(translog.metadata().generation(), o.translog.metadata().generation());
        }

        @Override
        public String toString() {
            return "UploadTranslogTask{" + "isUploaded=" + isUploaded + ", generation=" + translog.metadata().generation() + '}';
        }
    }

    private UploadTranslogTask createUploadTask(CompoundTranslog translog) {
        UploadTranslogTask uploadTranslogTask = new UploadTranslogTask(translog, new ActionListener<>() {
            @Override
            public void onResponse(UploadTranslogTask task) {
                nodeState.markUploadFinished(task);
            }

            @Override
            public void onFailure(Exception e) {
                // We only fully fail when the translog replicator is shutting down
                logger.info(
                    () -> "failed to upload translog file [" + translog.metadata().name() + "] due to translog replicator shutdown",
                    e
                );
                assert isOpen.get() == false;
            }
        });
        nodeState.markUploadStarting(uploadTranslogTask);
        return uploadTranslogTask;

    }

    private record CompoundTranslog(CompoundTranslogMetadata metadata, CompoundTranslogBytes bytes) {}

    private record CompoundTranslogMetadata(
        String name,
        long generation,
        HashMap<ShardId, TranslogMetadata> checkpoints,
        Map<ShardId, ShardSyncState.SyncMarker> syncedLocations
    ) {}

    private record CompoundTranslogBytes(BytesReference data, Releasable onComplete) implements Releasable {

        @Override
        public void close() {
            onComplete.close();
        }
    }

    // TODO: Rename to reflect that this is for sync or combine with other task
    private class ValidateClusterStateTask extends RetryableAction<Void> implements Comparable<ValidateClusterStateTask> {

        private final long validateGeneration;
        private final ArrayList<CompoundTranslogMetadata> completedSyncs;

        private ValidateClusterStateTask(long validateGeneration, ArrayList<CompoundTranslogMetadata> completedSyncs) {
            super(
                org.apache.logging.log4j.LogManager.getLogger(TranslogReplicator.class),
                threadPool,
                flushRetryInitialDelay,
                TimeValue.timeValueSeconds(5),
                TimeValue.MAX_VALUE,
                new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        logger.debug(
                            () -> format(
                                "validated cluster state for translog file upload [validateGeneration=%s, files=%s]",
                                validateGeneration,
                                completedSyncs.stream().map(f -> f.name).toList()
                            )
                        );
                        nodeState.markClusterStateValidateFinished(validateGeneration);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // We only fully fail when the translog replicator is shutting down
                        logger.info(() -> "failed to validate cluster state due to translog replicator shutdown", e);
                    }
                },
                EsExecutors.DIRECT_EXECUTOR_SERVICE
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
            logger.trace(
                () -> format(
                    "attempting to validate cluster state for translog file upload [validateGeneration=%s, files=%s]",
                    validateGeneration,
                    completedSyncs.stream().map(f -> f.name).toList()
                )
            );
            consistencyService.ensureClusterStateConsistentWithRootBlob(listener, TimeValue.MAX_VALUE);
        }

        @Override
        public boolean shouldRetry(Exception e) {
            return isOpen.get();
        }
    }

    private class ValidateClusterStateForDeleteTask extends RetryableAction<Void> {

        private final BlobTranslogFile fileToDelete;

        private ValidateClusterStateForDeleteTask(BlobTranslogFile fileToDelete) {
            super(
                org.apache.logging.log4j.LogManager.getLogger(TranslogReplicator.class),
                threadPool,
                flushRetryInitialDelay,
                TimeValue.timeValueSeconds(5),
                TimeValue.MAX_VALUE,
                new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        logger.debug(() -> format("validated cluster state for translog file delete [file=%s]", fileToDelete.blobName()));
                        nodeState.clusterStateValidateForDeleteFinished(fileToDelete);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // We only fully fail when the translog replicator is shutting down
                        logger.info(() -> "failed to validate cluster state due to translog replicator shutdown", e);
                    }
                },
                threadPool.executor(ThreadPool.Names.GENERIC)
            );
            this.fileToDelete = fileToDelete;
        }

        @Override
        public void tryAction(ActionListener<Void> listener) {
            logger.trace(() -> format("attempting to validate cluster state for translog file delete [file=%s]", fileToDelete.blobName()));
            consistencyService.delayedEnsureClusterStateConsistentWithRootBlob(listener);
        }

        @Override
        public boolean shouldRetry(Exception e) {
            return isOpen.get();
        }
    }

    public abstract static class BlobTranslogFile extends AbstractRefCounted implements Comparable<BlobTranslogFile> {

        private final long generation;
        private final String blobName;
        private final Map<ShardId, TranslogMetadata> checkpoints;
        private final Set<ShardId> includedShards;

        BlobTranslogFile(long generation, String blobName, Map<ShardId, TranslogMetadata> checkpoints, Set<ShardId> includedShards) {
            this.generation = generation;
            this.blobName = blobName;
            this.checkpoints = checkpoints;
            this.includedShards = includedShards;
        }

        long generation() {
            return generation;
        }

        public String blobName() {
            return blobName;
        }

        public Map<ShardId, TranslogMetadata> checkpoints() {
            return checkpoints;
        }

        @Override
        public int compareTo(BlobTranslogFile o) {
            return Long.compare(generation(), o.generation());
        }

        @Override
        public String toString() {
            return "BlobTranslogFile{"
                + "generation="
                + generation
                + ", blobName='"
                + blobName
                + "', checkpoints="
                + checkpoints
                + ", includedShards="
                + includedShards
                + '}';
        }
    }

    private class BlobTranslogFileImpl extends BlobTranslogFile {

        private BlobTranslogFileImpl(CompoundTranslogMetadata compoundTranslog) {
            super(
                compoundTranslog.generation(),
                compoundTranslog.name(),
                compoundTranslog.checkpoints(),
                compoundTranslog.syncedLocations().keySet()
            );
        }

        @Override
        protected void closeInternal() {
            nodeState.clusterStateValidateForFileDelete(this);
        }
    }

    private class NodeSyncState {

        private final AtomicLong maxUploadedGeneration = new AtomicLong(-1);

        private final AtomicLong compoundTranslogGeneration = new AtomicLong(0);
        private final PriorityQueue<UploadTranslogTask> ongoingUploads = new PriorityQueue<>();

        private final AtomicLong validateClusterStateGeneration = new AtomicLong(0);
        private final PriorityQueue<ValidateClusterStateTask> ongoingValidateClusterState = new PriorityQueue<>();

        private final Map<Long, BlobTranslogFile> uploadingTranslogFiles = ConcurrentCollections.newConcurrentMap();
        private final Set<BlobTranslogFile> activeTranslogFiles = ConcurrentCollections.newConcurrentSet();
        private final Set<BlobTranslogFile> translogFilesToDelete = ConcurrentCollections.newConcurrentSet();

        private void markUploadStarting(final UploadTranslogTask uploadTranslogTask) {
            synchronized (ongoingUploads) {
                ongoingUploads.add(uploadTranslogTask);

                CompoundTranslogMetadata metadata = uploadTranslogTask.translog.metadata();
                BlobTranslogFileImpl translogFile = new BlobTranslogFileImpl(metadata);
                uploadingTranslogFiles.put(metadata.generation(), translogFile);
                for (Map.Entry<ShardId, ShardSyncState.SyncMarker> entry : metadata.syncedLocations().entrySet()) {
                    assert translogFile.checkpoints().get(entry.getKey()).totalOps() > 0;
                    ShardSyncState shardSyncState = shardSyncStates.get(entry.getKey());
                    // If the shard sync state has been deregistered we can just ignore
                    if (shardSyncState != null) {
                        translogFile.incRef();
                        shardSyncState.markSyncStarting(entry.getValue().primaryTerm(), translogFile);
                    }
                }
            }
        }

        private void markUploadFinished(UploadTranslogTask uploadTask) {
            long uploadedNodeTranslogGeneration = uploadTask.translog.metadata().generation();
            maxUploadedGeneration.getAndAccumulate(uploadedNodeTranslogGeneration, Math::max);
            // We lock on ongoingSyncs to ensure that we transition all synced translog files to the validate step.
            synchronized (ongoingUploads) {
                assert ongoingUploads.isEmpty() == false;
                uploadTask.markUploaded();
                if (ongoingUploads.peek() == uploadTask) {
                    ArrayList<CompoundTranslogMetadata> completedSyncs = new ArrayList<>(2);
                    Iterator<UploadTranslogTask> iterator = ongoingUploads.iterator();
                    while (iterator.hasNext()) {
                        UploadTranslogTask entry = iterator.next();
                        if (entry.isUploaded) {
                            completedSyncs.add(entry.translog.metadata());
                            iterator.remove();
                        } else {
                            break;
                        }
                    }

                    // Submit the cluster state validate under lock so that the validate generations increase in order.
                    triggerClusterStateValidate(completedSyncs);
                } else {
                    assert checkUploadTranslogTask(uploadedNodeTranslogGeneration)
                        : "Unable to find upload translog task with generation: "
                            + uploadedNodeTranslogGeneration
                            + " in ongoing upload generations: "
                            + ongoingUploads;
                }
            }
        }

        private boolean checkUploadTranslogTask(long nodeTranslogGeneration) {
            return ongoingUploads.stream()
                .filter(t -> t.translog.metadata().generation == nodeTranslogGeneration)
                .findAny()
                .map(t -> t.isUploaded)
                .orElse(false);
        }

        private void triggerClusterStateValidate(ArrayList<CompoundTranslogMetadata> completedSyncs) {
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
                            BlobTranslogFile translogFile = uploadingTranslogFiles.remove(sync.generation());
                            assert translogFile != null;
                            try {
                                activeTranslogFiles.add(translogFile);
                                for (Map.Entry<ShardId, ShardSyncState.SyncMarker> entry : sync.syncedLocations().entrySet()) {
                                    ShardSyncState shardSyncState = shardSyncStates.get(entry.getKey());
                                    // If the shard sync state has been deregistered we can just ignore
                                    if (shardSyncState != null) {
                                        ShardSyncState.SyncMarker syncMarker = entry.getValue();
                                        boolean syncFinishedAccepted = shardSyncState.markSyncFinished(syncMarker);
                                        if (syncFinishedAccepted) {
                                            modifiedShardSyncedLocations.add(shardSyncState);
                                        } else {
                                            assert syncMarker.primaryTerm() != shardSyncState.currentPrimaryTerm();
                                            logger.debug(
                                                () -> format(
                                                    "skipped shard %s sync notification after translog file [%s] upload "
                                                        + "because primary term advanced [syncPrimaryTerm=%s, currentPrimaryTerm=%s]",
                                                    entry.getKey(),
                                                    translogFile.blobName(),
                                                    syncMarker.primaryTerm(),
                                                    shardSyncState.currentPrimaryTerm()
                                                )
                                            );
                                        }
                                    } else {
                                        logger.debug(
                                            () -> format(
                                                "skipped shard %s sync notification after translog file [%s] upload because "
                                                    + "shard unregistered",
                                                entry.getKey(),
                                                translogFile.blobName()
                                            )
                                        );
                                    }
                                }
                            } finally {
                                translogFile.decRef();
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

        private void clusterStateValidateForFileDelete(BlobTranslogFile fileToDelete) {
            translogFilesToDelete.add(fileToDelete);
            boolean removed = activeTranslogFiles.remove(fileToDelete);
            final ValidateClusterStateForDeleteTask validateClusterStateTask = new ValidateClusterStateForDeleteTask(fileToDelete);
            validateClusterStateTask.run();
            assert removed;
        }

        private void clusterStateValidateForDeleteFinished(BlobTranslogFile fileToDelete) {
            ClusterState state = consistencyService.state();
            if (allShardsAtLeastYellow(fileToDelete, state)) {
                deleteFile(fileToDelete);
            } else {
                ClusterStateObserver observer = new ClusterStateObserver(
                    state.version(),
                    consistencyService.clusterService().getClusterApplierService(),
                    null,
                    logger,
                    threadPool.getThreadContext()
                );

                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        assert allShardsAtLeastYellow(fileToDelete, state);
                        deleteFile(fileToDelete);
                    }

                    @Override
                    public void onClusterServiceClose() {
                        logger.info("wait for yellow shards to delete translog blob file cancelled due to shutdown");
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        assert false : "No timeout.";
                    }
                }, newState -> allShardsAtLeastYellow(fileToDelete, newState));
            }
        }

        private void deleteFile(BlobTranslogFile fileToDelete) {
            translogFilesToDelete.remove(fileToDelete);
            for (ShardId shardId : fileToDelete.includedShards) {
                ShardSyncState shardSyncState = shardSyncStates.get(shardId);
                if (shardSyncState != null) {
                    shardSyncState.markTranslogDeleted(fileToDelete.generation());
                }
            }
            logger.debug(() -> format("scheduling translog file [%s] for async delete", fileToDelete.blobName()));
            objectStoreService.asyncDeleteTranslogFile(fileToDelete.blobName);
        }

        private static boolean allShardsAtLeastYellow(BlobTranslogFile fileToDelete, ClusterState state) {
            for (ShardId shardId : fileToDelete.includedShards) {
                if (isShardRed(state, shardId)) {
                    return false;
                }
            }
            return true;
        }

        private static boolean isShardRed(ClusterState state, ShardId shardId) {
            var indexRoutingTable = state.routingTable().index(shardId.getIndex());
            if (indexRoutingTable == null) {
                logger.debug("index not found while checking if shard {} is red", shardId);
                return false;
            }
            var shardRoutingTable = indexRoutingTable.shard(shardId.id());
            assert shardRoutingTable != null;
            return new ClusterShardHealth(shardId.getId(), shardRoutingTable).getStatus() == ClusterHealthStatus.RED;
        }

        public void close() {
            synchronized (ongoingUploads) {
                // Don't remove. Just cancel since this only happens on shutdown.
                ongoingUploads.forEach(r -> r.cancel(new ElasticsearchException("Node shutting down")));
            }
        }
    }
}
