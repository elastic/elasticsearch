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

import co.elastic.elasticsearch.stateless.ObjectStoreService;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.newSetFromMap;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.core.Strings.format;

public class StatelessCommitService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(StatelessCommitService.class);

    /** How long an indexing shard should not have sent new commit notifications in order to be deemed as inactive. */
    public static final Setting<TimeValue> SHARD_INACTIVITY_DURATION_TIME_SETTING = Setting.positiveTimeSetting(
        "shard.inactivity.duration",
        TimeValue.timeValueMinutes(10),
        Setting.Property.NodeScope
    );

    /** How frequently we check for inactive indexing shards to send new commit notifications. */
    public static final Setting<TimeValue> SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING = Setting.positiveTimeSetting(
        "shard.inactivity.monitor.interval",
        TimeValue.timeValueMinutes(30),
        Setting.Property.NodeScope
    );

    private final ObjectStoreService objectStoreService;
    private final Supplier<String> ephemeralNodeIdSupplier;
    private final Function<ShardId, IndexShardRoutingTable> shardRouting;
    private final ThreadPool threadPool;
    // We don't do null checks when reading from this sub-map because we hold a commit reference while files are being uploaded. This will
    // prevent commit deletion in the interim.
    private final ConcurrentHashMap<ShardId, ShardCommitState> shardsCommitsStates = new ConcurrentHashMap<>();
    private final ConcurrentMap<ShardId, Consumer<Long>> commitNotificationSuccessListeners = new ConcurrentHashMap<>();
    private final StatelessCommitCleaner commitCleaner;
    private final NewCommitNotificationGrouperClient client;
    private final TimeValue shardInactivityDuration;
    private final TimeValue shardInactivityMonitorInterval;
    private final ShardInactivityMonitor shardInactivityMonitor;
    private Scheduler.Cancellable scheduledShardInactivityMonitorFuture;

    public StatelessCommitService(
        Settings settings,
        ObjectStoreService objectStoreService,
        ClusterService clusterService,
        Client client,
        StatelessCommitCleaner commitCleaner
    ) {
        this(
            settings,
            objectStoreService,
            () -> clusterService.localNode().getEphemeralId(),
            (shardId) -> clusterService.state().routingTable().shardRoutingTable(shardId),
            clusterService.threadPool(),
            client,
            commitCleaner
        );
    }

    public StatelessCommitService(
        Settings settings,
        ObjectStoreService objectStoreService,
        Supplier<String> ephemeralNodeIdSupplier,
        Function<ShardId, IndexShardRoutingTable> shardRouting,
        ThreadPool threadPool,
        Client client,
        StatelessCommitCleaner commitCleaner
    ) {
        this.objectStoreService = objectStoreService;
        this.ephemeralNodeIdSupplier = ephemeralNodeIdSupplier;
        this.shardRouting = shardRouting;
        this.threadPool = threadPool;
        this.client = new NewCommitNotificationGrouperClient(client);
        this.commitCleaner = commitCleaner;
        this.shardInactivityDuration = SHARD_INACTIVITY_DURATION_TIME_SETTING.get(settings);
        this.shardInactivityMonitorInterval = SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING.get(settings);
        this.shardInactivityMonitor = new ShardInactivityMonitor();
    }

    public void markRecoveredCommit(ShardId shardId, StatelessCompoundCommit recoveredCommit, Set<BlobFile> unreferencedFiles) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        assert recoveredCommit != null;
        commitState.markCommitRecovered(recoveredCommit, unreferencedFiles);
    }

    public long getRecoveredGeneration(ShardId shardId) {
        return getSafe(shardsCommitsStates, shardId).recoveredGeneration;
    }

    public void markCommitDeleted(ShardId shardId, long generation) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        commitState.markCommitDeleted(generation);
    }

    @Override
    protected void doStart() {
        scheduledShardInactivityMonitorFuture = threadPool.scheduleWithFixedDelay(
            shardInactivityMonitor,
            shardInactivityMonitorInterval,
            threadPool.executor(ThreadPool.Names.GENERIC)
        );
    }

    @Override
    protected void doStop() {
        scheduledShardInactivityMonitorFuture.cancel();
    }

    @Override
    protected void doClose() throws IOException {}

    /**
     * An always rescheduled runnable that monitors shards which have been inactive, i.e., have not received indexing, for a long time, and
     * sends new commit notification to search shards.
     */
    private class ShardInactivityMonitor implements Runnable {

        @Override
        public void run() {
            if (lifecycleState() != Lifecycle.State.STARTED) {
                return;
            }

            shardsCommitsStates.forEach((shardId, commitState) -> {
                if (commitState.isClosed == false) {
                    long elapsed = threadPool.relativeTimeInMillis() - commitState.lastNewCommitNotificationSentTimestamp;
                    // TODO (ES-6726) improvement: there should be unpromotable searches registered in order to send out the notification.
                    if (elapsed > shardInactivityDuration.getMillis()) {
                        logger.debug("sending new commit notifications for inactive shard [{}]", shardId);
                        commitState.resendLatestNewCommitNotification();
                    }
                }
            });
        }
    }

    public void onCommitCreation(StatelessCommitRef reference) {
        var shardId = reference.getShardId();
        var generation = reference.getGeneration();

        ShardCommitState commitState = getSafe(shardsCommitsStates, reference.getShardId());
        if (commitState.recoveredGeneration == reference.getGeneration()) {
            logger.debug("{} skipping upload of recovered commit [{}]", shardId, generation);
            IOUtils.closeWhileHandlingException(reference);
            return;
        }

        logger.debug("{} uploading commit [{}][{}]", shardId, reference.getSegmentsFileName(), generation);
        var compoundCommitBlob = commitState.markCommitCreated(
            reference.getPrimaryTerm(),
            generation,
            reference.getCommitFiles(),
            reference.getAdditionalFiles()
        );

        // The CommitUpload listener is called after releasing the reference to the Lucene commit,
        // it's possible that due to a slow upload the commit is deleted in the meanwhile, therefore
        // we should acquire a reference to avoid deleting the commit before notifying the unpromotable shards.
        compoundCommitBlob.incRef();
        CommitUpload commitUpload = new CommitUpload(commitState, ActionListener.runAfter(ActionListener.wrap(new ActionListener<>() {
            @Override
            public void onResponse(StatelessCompoundCommit commit) {
                commitState.sendNewCommitNotification(compoundCommitBlob, commit);
            }

            @Override
            public void onFailure(Exception e) {
                assert assertClosedOrRejectionFailure(e);
                logger.warn(
                    () -> format(
                        "%s failed to upload commit [%s] to object store because shard was closed",
                        reference.getShardId(),
                        reference.getGeneration()
                    ),
                    e
                );
            }

            private boolean assertClosedOrRejectionFailure(final Exception e) {
                final var closed = commitState.isClosed;
                assert closed
                    || e instanceof EsRejectedExecutionException
                    || e instanceof IndexNotFoundException
                    || e instanceof ShardNotFoundException : closed + " vs " + e;
                return true;
            }

        }), compoundCommitBlob::decRef), reference, TimeValue.timeValueMillis(50));
        commitUpload.run();
    }

    public boolean hasPendingCommitUploads(ShardId shardId) {
        try {
            ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
            return commitState.pendingUploadGenerations.isEmpty() == false;
        } catch (AlreadyClosedException ace) {
            return false;
        }
    }

    public class CommitUpload extends RetryableAction<StatelessCompoundCommit> {

        private final StatelessCommitRef reference;
        private final ShardCommitState shardCommitState;
        private final ShardId shardId;
        private final long generation;
        private final long startNanos;
        private final AtomicLong uploadedFileCount = new AtomicLong();
        private final AtomicLong uploadedFileBytes = new AtomicLong();
        private final AtomicReference<Map<String, Long>> commitFilesToLength = new AtomicReference<>();

        public CommitUpload(
            ShardCommitState shardCommitState,
            ActionListener<StatelessCompoundCommit> listener,
            StatelessCommitRef reference,
            TimeValue initialDelay
        ) {
            super(
                logger,
                threadPool,
                initialDelay,
                TimeValue.timeValueSeconds(5),
                TimeValue.timeValueMillis(Long.MAX_VALUE),
                listener,
                ThreadPool.Names.GENERIC
            );
            this.shardCommitState = shardCommitState;
            this.reference = reference;
            this.shardId = reference.getShardId();
            this.generation = reference.getGeneration();
            this.startNanos = threadPool.relativeTimeInNanos();
        }

        @Override
        public void tryAction(ActionListener<StatelessCompoundCommit> listener) {
            try {
                // Only do this once across multiple retries since file lengths should not change
                if (this.commitFilesToLength.get() == null) {
                    final Collection<String> commitFileNames = reference.getCommitFiles();
                    Map<String, Long> mutableCommitFiles = Maps.newHashMapWithExpectedSize(commitFileNames.size());
                    for (String fileName : commitFileNames) {
                        mutableCommitFiles.put(fileName, reference.getDirectory().fileLength(fileName));
                    }
                    this.commitFilesToLength.set(Collections.unmodifiableMap(mutableCommitFiles));
                }
            } catch (AlreadyClosedException e) {
                logger.trace(() -> format("%s exception while reading file sizes to upload [%s] to object store", shardId, generation), e);
                listener.onFailure(e);
                return;
            } catch (Exception e) {
                logger.info(() -> format("%s exception while reading file sizes to upload [%s] to object store", shardId, generation), e);
                assert e instanceof IOException;
                listener.onFailure(e);
                return;
            }

            executeUpload(listener.delegateResponse((l, e) -> {
                logUploadAttemptFailure(e);
                l.onFailure(e);
            }));
        }

        private void logUploadAttemptFailure(Exception e) {
            if (e instanceof AlreadyClosedException) {
                logger.trace(
                    () -> format("%s failed attempt to upload commit [%s] to object store because shard closed", shardId, generation),
                    e
                );
            } else {
                logger.info(() -> format("%s failed attempt to upload commit [%s] to object store, will retry", shardId, generation), e);
            }
        }

        private void executeUpload(ActionListener<StatelessCompoundCommit> listener) {
            try {
                ActionListener<Void> uploadReadyListener = listener.delegateFailure((l, v) -> uploadStatelessCommitFile(l));
                checkReadyToUpload(uploadReadyListener, listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        private void checkReadyToUpload(ActionListener<Void> readyListener, ActionListener<StatelessCompoundCommit> notReadyListener) {
            OptionalLong missing = shardCommitState.pendingUploadGenerations.stream().mapToLong(l -> l).filter(g -> g < generation).max();
            if (missing.isPresent()) {
                long missingGeneration = missing.getAsLong();
                logger.trace("{} waiting for commit [{}] to finish before uploading commit [{}]", shardId, missingGeneration, generation);
                shardCommitState.addListenerForUploadedGeneration(
                    missingGeneration,
                    notReadyListener.delegateFailure((l, unused) -> executeUpload(notReadyListener))
                );
            } else {
                readyListener.onResponse(null);
            }
        }

        private void uploadStatelessCommitFile(ActionListener<StatelessCompoundCommit> listener) {
            String commitFileName = StatelessCompoundCommit.blobNameFromGeneration(generation);
            Set<String> internalFiles = reference.getAdditionalFiles();
            Set<String> referencedGenerationalFiles = reference.getCommitFiles()
                .stream()
                .filter(StatelessCommitService::isGenerationalFile)
                .filter(Predicate.not(internalFiles::contains))
                .collect(Collectors.toSet());
            if (referencedGenerationalFiles.isEmpty() == false) {
                internalFiles = Sets.union(internalFiles, referencedGenerationalFiles);
            }
            StatelessCompoundCommit.Writer pendingCommit = shardCommitState.returnPendingCompoundCommit(
                shardId,
                generation,
                reference.getPrimaryTerm(),
                internalFiles,
                commitFilesToLength.get(),
                reference.getTranslogRecoveryStartFile()
            );

            objectStoreService.uploadStatelessCommitFile(
                shardId,
                reference.getPrimaryTerm(),
                generation,
                reference.getDirectory(),
                commitFileName,
                startNanos,
                pendingCommit,
                listener.delegateFailure((l, commit) -> {
                    for (String internalFile : pendingCommit.getInternalFiles()) {
                        uploadedFileCount.getAndIncrement();
                        uploadedFileBytes.getAndAdd(commitFilesToLength.get().get(internalFile));
                        shardCommitState.markFileUploaded(internalFile, commit.commitFiles().get(internalFile));
                    }
                    shardCommitState.markCommitUploaded(commit);
                    final long end = threadPool.relativeTimeInNanos();
                    logger.debug(
                        () -> format(
                            "%s commit [%s] uploaded in [%s] ms (%s files, %s total bytes)",
                            shardId,
                            generation,
                            TimeValue.nsecToMSec(end - startNanos),
                            uploadedFileCount.get(),
                            uploadedFileBytes.get()
                        )
                    );
                    l.onResponse(commit);
                })
            );
        }

        @Override
        public void onFinished() {
            IOUtils.closeWhileHandlingException(reference);
        }

        @Override
        public boolean shouldRetry(Exception e) {
            return shardCommitState.isClosed == false;
        }
    }

    public void register(ShardId shardId, long primaryTerm) {
        ShardCommitState existing = shardsCommitsStates.put(shardId, new ShardCommitState(shardId, primaryTerm));
        assert existing == null : shardId + " already registered";
    }

    public void unregister(ShardId shardId) {
        ShardCommitState removed = shardsCommitsStates.remove(shardId);
        assert removed != null : shardId + " not registered";
        removed.close();
    }

    public void addListenerForUploadedGeneration(ShardId shardId, long generation, ActionListener<Void> listener) {
        requireNonNull(listener, "listener cannot be null");
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        commitState.addListenerForUploadedGeneration(generation, listener);
    }

    /**
     * @param commit the commit that was uploaded
     * @param filesToRetain the individual files (not blobs) that are still necessary to be able to access, including
     *                      being held by open readers or being part of a commit that is not yet deleted by lucene.
     *                      Always includes all files from the new commit.
     */
    public record UploadedCommitInfo(StatelessCompoundCommit commit, Set<String> filesToRetain) {}

    public void addConsumerForNewUploadedCommit(ShardId shardId, Consumer<UploadedCommitInfo> listener) {
        requireNonNull(listener, "listener cannot be null");
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        commitState.addConsumerForNewUploadedCommit(listener);
    }

    // Visible for testing
    Set<String> getFilesWithBlobLocations(ShardId shardId) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        return commitState.blobLocations.keySet();
    }

    private static ShardCommitState getSafe(ConcurrentHashMap<ShardId, ShardCommitState> map, ShardId shardId) {
        final ShardCommitState commitState = map.get(shardId);
        if (commitState == null) {
            throw new AlreadyClosedException("shard [" + shardId + "] has already been closed");
        }
        return commitState;
    }

    private class ShardCommitState {

        private final ShardId shardId;
        private final long allocationPrimaryTerm;
        private final Set<Long> pendingUploadGenerations = ConcurrentCollections.newConcurrentSet();
        private List<Tuple<Long, ActionListener<Void>>> generationListeners = null;
        private List<Consumer<UploadedCommitInfo>> uploadedCommitConsumers = null;
        private volatile long recoveredGeneration = -1;
        private volatile long recoveredPrimaryTerm = -1;
        private volatile StatelessCompoundCommit latestUploadedCommit = null; // having the highest generation ever uploaded
        private volatile boolean isClosed;
        // map generations to compound commit blob instances
        private final Map<PrimaryTermAndGeneration, CompoundCommitBlob> compoundCommitBlobs = new ConcurrentHashMap<>();
        private final Map<String, UnpromotableShardCommitReferences> unpromotableShardCommitReferencesByNode = new ConcurrentHashMap<>();

        // maps file names to their (maybe future) compound commit blob & blob location
        private final Map<String, CommitAndBlobLocation> blobLocations = new ConcurrentHashMap<>();
        private volatile long lastNewCommitNotificationSentTimestamp = -1;

        private ShardCommitState(ShardId shardId, long allocationPrimaryTerm) {
            this.shardId = shardId;
            this.allocationPrimaryTerm = allocationPrimaryTerm;
        }

        public void markFileUploaded(String fileName, BlobLocation blobLocation) {
            blobLocations.compute(fileName, (ignored, commitAndBlobLocation) -> {
                assert commitAndBlobLocation != null : fileName;
                assert assertBlobLocations(fileName, commitAndBlobLocation, blobLocation);
                return new CommitAndBlobLocation(commitAndBlobLocation.compoundCommitBlob, blobLocation);
            });
        }

        private boolean assertBlobLocations(String fileName, CommitAndBlobLocation current, BlobLocation uploaded) {
            if (current.blobLocation != null) {
                assert isGenerationalFile(fileName) : fileName + ':' + current;
                assert current.blobLocation.compoundFileGeneration() < uploaded.compoundFileGeneration()
                    : fileName + ':' + current + " vs " + uploaded;
                return true;
            }
            assert current.compoundCommitBlob().getPrimaryTermAndGeneration().generation() == uploaded.compoundFileGeneration()
                : fileName + ':' + current + " vs " + uploaded;
            return true;
        }

        public StatelessCompoundCommit.Writer returnPendingCompoundCommit(
            ShardId shardId,
            long generation,
            long primaryTerm,
            Set<String> internalFiles,
            Map<String, Long> commitFiles,
            long translogRecoveryStartFile
        ) {
            StatelessCompoundCommit.Writer writer = new StatelessCompoundCommit.Writer(
                shardId,
                generation,
                primaryTerm,
                translogRecoveryStartFile,
                ephemeralNodeIdSupplier.get()
            );
            for (Map.Entry<String, Long> commitFile : commitFiles.entrySet()) {
                String fileName = commitFile.getKey();
                if (internalFiles.contains(fileName) == false) {
                    var location = blobLocations.get(fileName);
                    assert location != null : fileName;
                    assert location.blobLocation() != null : fileName + ':' + location;
                    writer.addReferencedBlobFile(fileName, location.blobLocation());
                } else {
                    writer.addInternalFile(fileName, commitFile.getValue());
                }
            }
            return writer;
        }

        private void markCommitRecovered(StatelessCompoundCommit recoveredCommit, Set<BlobFile> nonRecoveredBlobs) {
            assert recoveredCommit != null;
            assert compoundCommitBlobs.isEmpty() : compoundCommitBlobs;
            assert blobLocations.isEmpty() : blobLocations;

            Map<PrimaryTermAndGeneration, Map<String, BlobLocation>> referencedBlobs = new HashMap<>();
            for (Map.Entry<String, BlobLocation> referencedBlob : recoveredCommit.commitFiles().entrySet()) {
                if (recoveredCommit.getInternalFiles().contains(referencedBlob.getKey()) == false) {
                    referencedBlobs.computeIfAbsent(
                        new PrimaryTermAndGeneration(
                            referencedBlob.getValue().primaryTerm(),
                            referencedBlob.getValue().compoundFileGeneration()
                        ),
                        primaryTermAndGeneration -> new HashMap<>()
                    ).put(referencedBlob.getKey(), referencedBlob.getValue());
                }
            }

            // create a compound commit blob instance for the recovery commit
            var recoveryCommitBlob = new CompoundCommitBlob(
                recoveredCommit.primaryTerm(),
                recoveredCommit.generation(),
                recoveredCommit.getInternalFiles()
            );

            PriorityQueue<CompoundCommitBlob> nonRecoveredCommits = new PriorityQueue<>(comparing(c -> c.primaryTermAndGeneration));
            for (BlobFile nonRecoveredBlobFile : nonRecoveredBlobs) {
                if (StatelessCompoundCommit.startsWithBlobPrefix(nonRecoveredBlobFile.blobName())) {
                    PrimaryTermAndGeneration nonRecoveredTermGen = new PrimaryTermAndGeneration(
                        nonRecoveredBlobFile.primaryTerm(),
                        StatelessCompoundCommit.parseGenerationFromBlobName(nonRecoveredBlobFile.blobName())
                    );

                    Map<String, BlobLocation> internalFiles = referencedBlobs.getOrDefault(nonRecoveredTermGen, Collections.emptyMap());

                    // create a compound commit blob instance for the new commit
                    var nonRecoveredCompoundCommitBlob = new CompoundCommitBlob(
                        nonRecoveredTermGen.primaryTerm(),
                        nonRecoveredTermGen.generation(),
                        internalFiles.keySet()
                    );
                    nonRecoveredCommits.add(nonRecoveredCompoundCommitBlob);

                    // If the recovery commit references files in this commit, ensure we increment a reference
                    if (referencedBlobs.containsKey(nonRecoveredTermGen)) {
                        assert internalFiles.isEmpty() == false;
                        recoveryCommitBlob.incRef(nonRecoveredCompoundCommitBlob);
                    }

                    internalFiles.forEach((key, value) -> {
                        var previous = blobLocations.put(key, new CommitAndBlobLocation(nonRecoveredCompoundCommitBlob, value));
                        assert previous == null : key + ':' + previous;
                    });

                } else {
                    logger.warn(
                        () -> format(
                            "%s found object store file which does not match compound commit file naming pattern [%s]",
                            shardId,
                            nonRecoveredBlobFile
                        )
                    );
                }
            }

            ArrayList<CompoundCommitBlob> previousCommits = new ArrayList<>();
            CompoundCommitBlob current;
            while ((current = nonRecoveredCommits.poll()) != null) {
                compoundCommitBlobs.put(current.getPrimaryTermAndGeneration(), current);
                for (CompoundCommitBlob previous : previousCommits) {
                    current.incRef(previous);
                }

                previousCommits.add(current);
            }

            compoundCommitBlobs.put(recoveryCommitBlob.getPrimaryTermAndGeneration(), recoveryCommitBlob);

            recoveredCommit.getInternalFiles().forEach(fileName -> {
                var existing = blobLocations.put(
                    fileName,
                    new CommitAndBlobLocation(recoveryCommitBlob, recoveredCommit.commitFiles().get(fileName))
                );
                assert existing == null : fileName + ':' + existing;
            });

            var currentUnpromotableShardAssignedNodes = shardRouting.apply(shardId)
                .unpromotableShards()
                .stream()
                .map(ShardRouting::currentNodeId)
                .collect(Collectors.toSet());
            updateUnpromotableShardAssignedNodes(currentUnpromotableShardAssignedNodes);

            for (UnpromotableShardCommitReferences unpromotableShardCommitReferences : unpromotableShardCommitReferencesByNode.values()) {
                unpromotableShardCommitReferences.trackReferencedCommits(new HashSet<>(compoundCommitBlobs.values()));
            }

            // Decrement all of the non-recovered commits since we do not reference them locally
            compoundCommitBlobs.values()
                .stream()
                .filter(b -> b.getPrimaryTermAndGeneration().equals(recoveryCommitBlob.getPrimaryTermAndGeneration()) == false)
                .forEach(b -> {
                    // Deleted and unused locally such that only the recovery commit retains this compound commit
                    b.deleted();
                    b.closedLocalReaders();
                });

            recoveredPrimaryTerm = recoveredCommit.primaryTerm();
            recoveredGeneration = recoveredCommit.generation();

            handleUploadedCommit(recoveredCommit);
        }

        public CompoundCommitBlob markCommitCreated(
            long primaryTerm,
            long generation,
            Collection<String> commitFiles,
            Set<String> additionalFiles
        ) {
            assert primaryTerm == allocationPrimaryTerm;

            pendingUploadGenerations.add(generation);

            return addCommitData(primaryTerm, generation, commitFiles, additionalFiles);
        }

        private CompoundCommitBlob addCommitData(
            long primaryTerm,
            long generation,
            Collection<String> commitFiles,
            Set<String> additionalFiles
        ) {
            // create a compound commit blob instance for the new commit
            var compoundCommitBlob = new CompoundCommitBlob(primaryTerm, generation, additionalFiles);
            if (compoundCommitBlobs.putIfAbsent(compoundCommitBlob.getPrimaryTermAndGeneration(), compoundCommitBlob) != null) {
                throw new IllegalArgumentException(compoundCommitBlob + " already exists");
            }

            // add pending blob locations for new files
            additionalFiles.forEach(fileName -> {
                var previous = blobLocations.put(fileName, new CommitAndBlobLocation(compoundCommitBlob, null));
                assert previous == null || isGenerationalFile(fileName) : fileName + ':' + previous;
            });

            // if there are external files the new instance must reference the corresponding commit blob instances
            commitFiles.forEach(fileName -> {
                if (additionalFiles.contains(fileName) == false) {
                    var commit = blobLocations.get(fileName).compoundCommitBlob();
                    compoundCommitBlob.incRef(commit);
                }
            });
            return compoundCommitBlob;
        }

        public void markCommitDeleted(long generation) {
            long primaryTerm = generation == recoveredGeneration ? recoveredPrimaryTerm : allocationPrimaryTerm;
            final var compoundCommitBlob = compoundCommitBlobs.get(new PrimaryTermAndGeneration(primaryTerm, generation));
            assert compoundCommitBlob != null : generation;
            compoundCommitBlob.deleted();
        }

        public void markCommitUploaded(StatelessCompoundCommit commit) {
            boolean removed = pendingUploadGenerations.remove(commit.generation());
            assert removed;
            handleUploadedCommit(commit);
        }

        private void handleUploadedCommit(StatelessCompoundCommit commit) {
            final long newGeneration = commit.generation();

            List<ActionListener<UploadedCommitInfo>> listenersToFire = null;
            List<Tuple<Long, ActionListener<Void>>> listenersToReregister = null;
            synchronized (this) {
                if (newGeneration > getMaxUploadedGeneration()) {
                    latestUploadedCommit = commit;
                } else {
                    // Generation did not increase so just bail early
                    return;
                }

                if (generationListeners != null) {
                    for (Tuple<Long, ActionListener<Void>> tuple : generationListeners) {
                        Long generation = tuple.v1();
                        if (getMaxUploadedGeneration() >= generation) {
                            if (listenersToFire == null) {
                                listenersToFire = new ArrayList<>();
                            }
                            listenersToFire.add(tuple.v2().map(c -> null));
                        } else {
                            if (listenersToReregister == null) {
                                listenersToReregister = new ArrayList<>();
                            }
                            listenersToReregister.add(tuple);
                        }
                    }
                    generationListeners = listenersToReregister;
                }
                if (uploadedCommitConsumers != null) {
                    if (listenersToFire == null) {
                        listenersToFire = new ArrayList<>();
                    }
                    for (var consumer : uploadedCommitConsumers) {
                        listenersToFire.add(ActionListener.wrap(consumer::accept, e -> {}));
                    }
                }
            }

            if (listenersToFire != null) {
                ActionListener.onResponse(listenersToFire, new UploadedCommitInfo(commit, Set.copyOf(blobLocations.keySet())));
            }
        }

        /**
         * Gets the max generation uploaded, by accessing the latest uploaded {@link StatelessCompoundCommit} without synchronization,
         * or -1 otherwise.
         */
        public long getMaxUploadedGeneration() {
            return latestUploadedCommit == null ? -1L : latestUploadedCommit.generation();
        }

        private void sendNewCommitNotification(CompoundCommitBlob compoundCommitBlob, StatelessCompoundCommit commit) {
            assert commit != null;
            var shardRoutingTable = shardRouting.apply(commit.shardId());
            var releasables = shardRoutingTable.unpromotableShards()
                .stream()
                .map(shardRouting -> trackOutstandingUnpromotableShardCommitRef(shardRouting.currentNodeId(), compoundCommitBlob))
                .toList();

            lastNewCommitNotificationSentTimestamp = threadPool.relativeTimeInMillis();
            NewCommitNotificationRequest request = new NewCommitNotificationRequest(shardRoutingTable, commit);
            client.sendRequest(request, ActionListener.releaseAfter(ActionListener.wrap(response -> {
                trackReferencedCommitsByUnpromotableShards(response.getUsedPrimaryTermAndGenerations());
                var consumer = commitNotificationSuccessListeners.get(shardId);
                if (consumer != null) {
                    consumer.accept(commit.generation());
                }
            },
                e -> logger.warn(
                    () -> format("%s failed to notify unpromotables after upload of commit [%s]", shardId, commit.generation()),
                    e
                )
            ), Releasables.wrap(releasables)));
        }

        private void resendLatestNewCommitNotification() {
            StatelessCompoundCommit latestStatelessCompoundCommitUploaded = null;
            ShardCommitState.CompoundCommitBlob latestCompoundCommitBlob = null;

            // Get latest uploaded stateless compound commit and the respective compound commit blob
            synchronized (this) {
                if (latestUploadedCommit == null) {
                    return;
                }
                latestStatelessCompoundCommitUploaded = latestUploadedCommit;
                PrimaryTermAndGeneration termGen = new PrimaryTermAndGeneration(
                    latestStatelessCompoundCommitUploaded.primaryTerm(),
                    latestStatelessCompoundCommitUploaded.generation()
                );
                latestCompoundCommitBlob = compoundCommitBlobs.get(termGen);
                assert latestCompoundCommitBlob != null : "could not find latest " + termGen + " in compound commit blobs";
            }

            sendNewCommitNotification(latestCompoundCommitBlob, latestStatelessCompoundCommitUploaded);
        }

        /**
         * Register a listener that is invoked once a commit with the given generation has been uploaded to the object store. The listener
         * is invoked only once.
         *
         * @param generation the commit generation
         * @param listener the listener
         */
        private void addListenerForUploadedGeneration(long generation, ActionListener<Void> listener) {
            boolean completeListenerSuccess = false;
            boolean completeListenerClosed = false;
            synchronized (this) {
                if (isClosed) {
                    completeListenerClosed = true;
                } else if (getMaxUploadedGeneration() >= generation) {
                    // Location already visible, just call the listener
                    completeListenerSuccess = true;
                } else {
                    List<Tuple<Long, ActionListener<Void>>> listeners = generationListeners;
                    ActionListener<Void> contextPreservingListener = ContextPreservingActionListener.wrapPreservingContext(
                        listener,
                        threadPool.getThreadContext()
                    );
                    if (listeners == null) {
                        listeners = new ArrayList<>();
                    }
                    listeners.add(new Tuple<>(generation, contextPreservingListener));
                    generationListeners = listeners;
                }
            }

            if (completeListenerClosed) {
                listener.onFailure(new AlreadyClosedException("shard [" + shardId + "] has already been closed"));
            } else if (completeListenerSuccess) {
                listener.onResponse(null);
            }
        }

        /**
         * Register a consumer that is invoked everytime a new commit has been uploaded to the object store
         * @param consumer the consumer
         */
        public void addConsumerForNewUploadedCommit(Consumer<UploadedCommitInfo> consumer) {
            synchronized (this) {
                if (isClosed == false) {
                    if (uploadedCommitConsumers == null) {
                        uploadedCommitConsumers = new ArrayList<>();
                    }
                    uploadedCommitConsumers.add(consumer);
                }
            }
        }

        private void close() {
            List<Tuple<Long, ActionListener<Void>>> listenersToFail;
            synchronized (this) {
                isClosed = true;
                listenersToFail = generationListeners;
                generationListeners = null;
                uploadedCommitConsumers = null;
            }

            if (listenersToFail != null) {
                ActionListener.onFailure(
                    listenersToFail.stream().map(Tuple::v2).collect(Collectors.toList()),
                    new AlreadyClosedException("shard closed")
                );
            }
        }

        void trackReferencedCommitsByUnpromotableShards(Set<PrimaryTermAndGeneration> usedPrimaryTermAndGenerations) {
            if (isClosed) {
                return;
            }

            // TODO: provide Map<nodeId, Set<PrimaryTermAndGeneration>>
            for (UnpromotableShardCommitReferences unpromotableShardCommitReferences : unpromotableShardCommitReferencesByNode.values()) {
                Set<CompoundCommitBlob> currentlyReferencedCommits = new HashSet<>();
                for (PrimaryTermAndGeneration usedPrimaryTermAndGeneration : usedPrimaryTermAndGenerations) {
                    var compoundCommit = compoundCommitBlobs.get(usedPrimaryTermAndGeneration);
                    assert compoundCommit != null : usedPrimaryTermAndGeneration + " " + shardId + " " + compoundCommitBlobs;
                    // TODO: Implement two ref-counting approaches, one to keep track of commits used by the index node and
                    // one to keep track of outstanding references
                    currentlyReferencedCommits.add(compoundCommit);
                    currentlyReferencedCommits.addAll(compoundCommit.references);
                }

                unpromotableShardCommitReferences.trackReferencedCommits(Collections.unmodifiableSet(currentlyReferencedCommits));
            }
        }

        void updateUnpromotableShardAssignedNodes(Set<String> currentUnpromotableNodes) {
            for (String currentNode : currentUnpromotableNodes) {
                unpromotableShardCommitReferencesByNode.computeIfAbsent(currentNode, UnpromotableShardCommitReferences::new);
            }

            var nodesIter = unpromotableShardCommitReferencesByNode.entrySet().iterator();
            while (nodesIter.hasNext()) {
                var nodeRef = nodesIter.next();
                if (currentUnpromotableNodes.contains(nodeRef.getKey()) == false) {
                    nodesIter.remove();
                    nodeRef.getValue().close();
                }
            }
        }

        Releasable trackOutstandingUnpromotableShardCommitRef(String nodeId, CompoundCommitBlob compoundCommitBlob) {
            unpromotableShardCommitReferencesByNode.computeIfAbsent(nodeId, UnpromotableShardCommitReferences::new);

            List<Releasable> releasables = new ArrayList<>(compoundCommitBlob.references.size() + 1);

            // TODO: Implement two ref-counting approaches, one to keep track of commits used by the index node and
            // one to keep track of outstanding references
            compoundCommitBlob.incRef();
            for (CompoundCommitBlob referencedCommit : compoundCommitBlob.references) {
                referencedCommit.incRef();
                releasables.add(referencedCommit::decRef);
            }

            releasables.add(compoundCommitBlob::decRef);
            return Releasables.wrap(releasables);
        }

        void registerCommitForUnpromotableRecovery(String nodeId, CompoundCommitBlob compoundCommit) {
            var unpromotableShardCommitRefs = unpromotableShardCommitReferencesByNode.computeIfAbsent(
                nodeId,
                UnpromotableShardCommitReferences::new
            );
            Set<CompoundCommitBlob> currentlyReferencedCommits = new HashSet<>();
            currentlyReferencedCommits.add(compoundCommit);
            currentlyReferencedCommits.addAll(compoundCommit.references);
            unpromotableShardCommitRefs.registerCommitForUnpromotableRecovery(currentlyReferencedCommits);
        }

        public LongConsumer closedLocalReadersForGeneration() {
            return generation -> {
                long primaryTerm = generation == recoveredGeneration ? recoveredPrimaryTerm : allocationPrimaryTerm;
                CompoundCommitBlob compoundCommitBlob = compoundCommitBlobs.get(new PrimaryTermAndGeneration(primaryTerm, generation));
                if (compoundCommitBlob != null) {
                    compoundCommitBlob.closedLocalReaders();
                } // else assume an idempotent call when already deleted.
            };
        }

        /**
         * A ref counted instance representing a compound commit blob in the object store. It can reference some other previous compound
         * commit blob instances (if the commit has external files) which are decRef when the current instance ref count reaches zero.
         */
        private class CompoundCommitBlob extends AbstractRefCounted {
            private final PrimaryTermAndGeneration primaryTermAndGeneration;
            private final Set<String> internalFiles;
            private final Set<CompoundCommitBlob> references;
            private final AtomicBoolean deleted = Assertions.ENABLED ? new AtomicBoolean() : null;
            private final AtomicBoolean readersClosed = new AtomicBoolean();

            CompoundCommitBlob(long primaryTerm, long generation, Set<String> internalFiles) {
                this.primaryTermAndGeneration = new PrimaryTermAndGeneration(primaryTerm, generation);
                this.internalFiles = Set.copyOf(internalFiles);
                this.references = newSetFromMap(new IdentityHashMap<>());
                // we both decRef on delete and on noMoreIndexShardReaders, hence an extra incRef().
                this.incRef();
            }

            public PrimaryTermAndGeneration getPrimaryTermAndGeneration() {
                return primaryTermAndGeneration;
            }

            public void incRef(CompoundCommitBlob other) {
                assert hasReferences() : this;
                assert other.hasReferences() : other;
                // incRef twice since we expect all commits to be both deleted and locally unused.
                other.incRef();
                other.incRef();
                if (references.add(other) == false) {
                    other.decRef();
                    other.decRef();
                }
            }

            public void deleted() {
                assert deleted.compareAndSet(false, true);
                references.forEach(AbstractRefCounted::decRef);
                decRef();
            }

            public void closedLocalReaders() {
                // be idempotent.
                if (readersClosed.compareAndSet(false, true)) {
                    references.forEach(AbstractRefCounted::decRef);
                    decRef();
                }
            }

            @Override
            protected void closeInternal() {
                final CompoundCommitBlob released = this;
                internalFiles.forEach(fileName -> {
                    blobLocations.compute(fileName, (file, commitAndBlobLocation) -> {
                        var existing = commitAndBlobLocation.compoundCommitBlob();
                        if (released != existing) {
                            assert isGenerationalFile(file) : file;
                            assert released.primaryTermAndGeneration.generation() < existing.primaryTermAndGeneration.generation()
                                : fileName + ':' + released + " vs " + existing;
                            return commitAndBlobLocation;
                        }
                        return null;
                    });
                });
                commitCleaner.deleteCommit(new StaleCompoundCommit(shardId, primaryTermAndGeneration, allocationPrimaryTerm));
                var removed = compoundCommitBlobs.remove(primaryTermAndGeneration);
                assert removed == this;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                CompoundCommitBlob that = (CompoundCommitBlob) o;
                return Objects.equals(primaryTermAndGeneration, that.primaryTermAndGeneration);
            }

            @Override
            public int hashCode() {
                return Objects.hash(primaryTermAndGeneration);
            }

            @Override
            public String toString() {
                return "Compound commit blob " + primaryTermAndGeneration;
            }

        }

        private static class UnpromotableShardCommitReferences implements Releasable {
            private final String nodeId;
            private Set<CompoundCommitBlob> referencedCommits = Collections.emptySet();
            private boolean closed = false;

            UnpromotableShardCommitReferences(String nodeId) {
                this.nodeId = nodeId;
            }

            void trackReferencedCommits(Set<CompoundCommitBlob> newReferencedCommits) {
                Set<CompoundCommitBlob> oldReferencedCommits;
                synchronized (this) {
                    if (closed) {
                        return;
                    }
                    oldReferencedCommits = referencedCommits;
                    referencedCommits = Collections.unmodifiableSet(newReferencedCommits);
                    newReferencedCommits.forEach(AbstractRefCounted::incRef);
                }

                oldReferencedCommits.forEach(AbstractRefCounted::decRef);
            }

            void registerCommitForUnpromotableRecovery(Set<CompoundCommitBlob> commitsUsedForRecovery) {
                synchronized (this) {
                    if (closed) {
                        return;
                    }
                    Set<CompoundCommitBlob> allReferencedCommits = new HashSet<>(referencedCommits);
                    allReferencedCommits.addAll(commitsUsedForRecovery);
                    commitsUsedForRecovery.forEach(AbstractRefCounted::incRef);
                    referencedCommits = Collections.unmodifiableSet(allReferencedCommits);
                }
            }

            @Override
            public void close() {
                Set<CompoundCommitBlob> oldReferencedCommits;
                synchronized (this) {
                    if (closed) {
                        return;
                    }
                    closed = true;
                    oldReferencedCommits = referencedCommits;
                    referencedCommits = Collections.emptySet();
                }

                oldReferencedCommits.forEach(AbstractRefCounted::decRef);
            }
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // TODO: maybe give a grace period if the node left?
        if (event.routingTableChanged()) {
            var localShardRouting = event.state().getRoutingNodes().node(event.state().nodes().getLocalNodeId());

            if (localShardRouting == null) {
                return;
            }

            for (ShardRouting shardRouting : localShardRouting) {
                if (shardRouting.primary() == false) {
                    continue;
                }
                var shardId = shardRouting.shardId();
                var shardCommitState = shardsCommitsStates.get(shardId);
                // shardsCommitsStates not registered yet
                if (shardCommitState == null) {
                    continue;
                }

                if (event.indexRoutingTableChanged(shardId.getIndexName())) {
                    var currentShardRoutingTable = event.state().routingTable().shardRoutingTable(shardId);
                    var previousShardRoutingTable = event.previousState().routingTable().shardRoutingTable(shardId);

                    if (currentShardRoutingTable != previousShardRoutingTable) {
                        var currentUnpromotableShards = currentShardRoutingTable.unpromotableShards();
                        var currentUnpromotableShardAssignedNodes = currentUnpromotableShards.stream()
                            .map(ShardRouting::currentNodeId)
                            .collect(Collectors.toSet());
                        shardCommitState.updateUnpromotableShardAssignedNodes(currentUnpromotableShardAssignedNodes);
                    }
                }
            }
        }
    }

    public void registerNewCommitSuccessListener(ShardId shardId, Consumer<Long> listener) {
        var previous = commitNotificationSuccessListeners.put(shardId, listener);
        // For now only the LiveVersionMapArchive uses this
        assert previous == null;
    }

    public void unregisterNewCommitSuccessListener(ShardId shardId) {
        var removed = commitNotificationSuccessListeners.remove(shardId);
        assert removed != null;
    }

    public static boolean isGenerationalFile(String file) {
        return file.startsWith("_") && IndexFileNames.parseGeneration(file) > 0L;
    }

    public PrimaryTermAndGeneration registerCommitForUnpromotableRecovery(PrimaryTermAndGeneration commit, ShardId shardId, String nodeId) {
        var shardCommitsState = getSafe(shardsCommitsStates, shardId);
        var compoundCommit = shardCommitsState.compoundCommitBlobs.get(commit);
        if (compoundCommit == null || compoundCommit.tryIncRef() == false) {
            // If the commit requested to be registered is being deleted, we shouldn't be able to acquire a reference to it.
            // In that case, try the latest commit.
            // TODO: add an accessor for the latest commit (should it also incRef?)
            long lastUploadedGeneration = shardCommitsState.getMaxUploadedGeneration();
            long primaryTerm = lastUploadedGeneration == shardCommitsState.recoveredGeneration
                ? shardCommitsState.recoveredPrimaryTerm
                : shardCommitsState.allocationPrimaryTerm;
            compoundCommit = shardCommitsState.compoundCommitBlobs.get(new PrimaryTermAndGeneration(primaryTerm, lastUploadedGeneration));

            // if the indexing shard is not finished initializing from the object store, we are not
            // able to register the commit for recovery. For now, fail the registration request.
            // TODO (ES-6698): we should be able to handle this case by either retrying the registration or keep the
            // registration and run it after the indexing shard is finished initializing.
            if (compoundCommit == null) {
                throw new NoShardAvailableActionException(shardId, "indexing shard is initializing");
            }
            // TODO: If the search shard has seen a newer commit (w/ newer term) that wants to register and recover from, the stale
            // indexing shard could reply with a commit from the last term. If this is possible, we should explicitly check against
            // this and fail the registration request.
            compoundCommit.incRef();
        }
        try {
            shardCommitsState.registerCommitForUnpromotableRecovery(nodeId, compoundCommit);
        } finally {
            compoundCommit.decRef();
        }
        var proposed = compoundCommit.primaryTermAndGeneration;
        assert proposed.compareTo(commit) >= 0
            : Strings.format("Proposed commit ({}) for unpromotable recovery must be newer that the requested one ({})", proposed, commit);
        return proposed;
    }

    private record CommitAndBlobLocation(ShardCommitState.CompoundCommitBlob compoundCommitBlob, @Nullable BlobLocation blobLocation) {
        @Override
        public String toString() {
            return "CommitAndBlobLocation [compoundCommitBlob=" + compoundCommitBlob + ", blobLocation=" + blobLocation + ']';
        }
    }

    /**
     * An idempotent consumer of generations that the index shard no longer need for readers
     *
     * @param shardId the shard to get consumer for
     * @return consumer of generations
     */
    public LongConsumer closedLocalReadersForGeneration(ShardId shardId) {
        return shardsCommitsStates.get(shardId).closedLocalReadersForGeneration();
    }
}
