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

import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.utils.WaitForVersion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
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
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.recovery.RecoveryCommitTooNewException;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
    private final Client client;

    private final WaitForVersion waitForClusterStateVersion = new WaitForVersion();

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
        this.client = client;
        this.commitCleaner = commitCleaner;
        this.shardInactivityDuration = SHARD_INACTIVITY_DURATION_TIME_SETTING.get(settings);
        this.shardInactivityMonitorInterval = SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING.get(settings);
        this.shardInactivityMonitor = new ShardInactivityMonitor();
    }

    public void markRecoveredCommit(ShardId shardId, StatelessCompoundCommit recoveredCommit, Set<BlobFile> unreferencedFiles) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        assert recoveredCommit != null;
        assert recoveredCommit.shardId().equals(shardId) : recoveredCommit.shardId() + " vs " + shardId;
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
            runInactivityMonitor(threadPool::relativeTimeInMillis);
        }
    }

    // package private for testing
    void runInactivityMonitor(Supplier<Long> time) {
        shardsCommitsStates.forEach((shardId, commitState) -> {
            if (commitState.isClosed == false && commitState.lastNewCommitNotificationSentTimestamp > 0) {
                long elapsed = time.get() - commitState.lastNewCommitNotificationSentTimestamp;
                if (elapsed > shardInactivityDuration.getMillis()) {
                    commitState.maybeResendLatestNewCommitNotification();
                }
            }
        });
    }

    public void onCommitCreation(StatelessCommitRef reference) {
        boolean success = false;
        try {
            var shardId = reference.getShardId();
            var generation = reference.getGeneration();

            ShardCommitState commitState = getSafe(shardsCommitsStates, reference.getShardId());
            if (commitState.recoveredGeneration == reference.getGeneration()) {
                logger.debug("{} skipping upload of recovered commit [{}]", shardId, generation);
                IOUtils.closeWhileHandlingException(reference);
                return;
            }

            logger.debug("{} uploading commit [{}][{}]", shardId, reference.getSegmentsFileName(), generation);
            var blobReference = commitState.markCommitCreated(
                reference.getPrimaryTerm(),
                generation,
                reference.getCommitFiles(),
                reference.getAdditionalFiles()
            );

            // The CommitUpload listener is called after releasing the reference to the Lucene commit,
            // it's possible that due to a slow upload the commit is deleted in the meanwhile, therefore
            // we should acquire a reference to avoid deleting the commit before notifying the unpromotable shards.
            // todo: reevaluate this.
            blobReference.incRef();
            CommitUpload commitUpload = new CommitUpload(commitState, ActionListener.runAfter(ActionListener.wrap(new ActionListener<>() {
                @Override
                public void onResponse(StatelessCompoundCommit commit) {
                    commitState.sendNewCommitNotification(blobReference, commit);
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

            }), () -> {
                IOUtils.closeWhileHandlingException(reference);
                blobReference.decRef();
            }), reference, TimeValue.timeValueMillis(50));
            commitUpload.run();
            success = true;
        } catch (Exception ex) {
            assert false : ex;
            logger.warn(Strings.format("failed to handle new commit [%s], generation [%s]", reference, reference.getGeneration()), ex);
            throw ex;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(reference);
            }
        }
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
                threadPool.executor(ThreadPool.Names.GENERIC)
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

    /**
     * Marks the {@link ShardCommitState} as deleted, which will delete associated blobs upon the forthcoming {@link #unregister(ShardId)}.
     * @param shardId the shard to mark as deleted
     */
    public void delete(ShardId shardId) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        commitState.delete();
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

        /**
         * The highest generation that we received notification response from unpromotables for
         */
        private final AtomicLong generationNotified = new AtomicLong(-1);
        private volatile boolean isClosed;
        private volatile boolean isDeleted;
        // map generations to compound commit blob instances
        private final Map<PrimaryTermAndGeneration, BlobReference> blobReferences = new ConcurrentHashMap<>();
        /**
         * Map from commit to set of search node-ids using the commit. The lifecycle of entries is like this:
         * 1. Initially added on recovery or commit created - with an empty set. Only this adds to the keys of the Map.
         * 2. Add to set of nodes before sending commit notification or when search shard registers during its initialization.
         *    Only these two actions add to the set of node ids.
         * 3. Remove from set of nodes when receiving commit notification response.
         * 4. Remove from set of nodes when a new cluster state indicates a search shard is no longer allocated.
         * 5. Remove from map when nodes is empty, using remove(key, Set.of()) to ensure we are atomic towards a search shard registering.
         *    When successful this dec-refs the BlobReference's external reader ref-count.
         */
        private final Map<BlobReference, Set<String>> unpromotableBlobReferences = new ConcurrentHashMap<>();

        // maps file names to their (maybe future) compound commit blob & blob location
        private final Map<String, CommitAndBlobLocation> blobLocations = new ConcurrentHashMap<>();
        private volatile long lastNewCommitNotificationSentTimestamp = -1;

        private ShardCommitState(ShardId shardId, long allocationPrimaryTerm) {
            this.shardId = shardId;
            this.allocationPrimaryTerm = allocationPrimaryTerm;
        }

        public void markFileUploaded(String fileName, BlobLocation blobLocation) {
            assert isDeleted == false : "shard " + shardId + " is deleted when trying to mark uploaded file " + blobLocation;
            blobLocations.compute(fileName, (ignored, commitAndBlobLocation) -> {
                assert commitAndBlobLocation != null : fileName;
                assert assertBlobLocations(fileName, commitAndBlobLocation, blobLocation);
                return new CommitAndBlobLocation(commitAndBlobLocation.blobReference, blobLocation);
            });
        }

        private boolean assertBlobLocations(String fileName, CommitAndBlobLocation current, BlobLocation uploaded) {
            if (current.blobLocation != null) {
                assert isGenerationalFile(fileName) : fileName + ':' + current;
                assert current.blobLocation.compoundFileGeneration() < uploaded.compoundFileGeneration()
                    : fileName + ':' + current + " vs " + uploaded;
                return true;
            }
            assert current.blobReference().getPrimaryTermAndGeneration().generation() == uploaded.compoundFileGeneration()
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
            assert isDeleted == false : "shard " + shardId + " is deleted when trying to return pending compound commit";
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
            assert nonRecoveredBlobs != null;
            assert blobReferences.isEmpty() : blobReferences;
            assert blobLocations.isEmpty() : blobLocations;

            Map<PrimaryTermAndGeneration, Map<String, BlobLocation>> referencedBlobs = new HashMap<>();
            final var recoveredInternalFiles = recoveredCommit.getInternalFiles();
            for (Map.Entry<String, BlobLocation> referencedBlob : recoveredCommit.commitFiles().entrySet()) {
                if (recoveredInternalFiles.contains(referencedBlob.getKey()) == false) {
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
            var recoveryCommitBlob = new BlobReference(recoveredCommit.primaryTermAndGeneration(), recoveredCommit.getInternalFiles());

            PriorityQueue<BlobReference> nonRecoveredCommits = new PriorityQueue<>(comparing(c -> c.primaryTermAndGeneration));
            for (BlobFile nonRecoveredBlobFile : nonRecoveredBlobs) {
                if (StatelessCompoundCommit.startsWithBlobPrefix(nonRecoveredBlobFile.blobName())) {
                    PrimaryTermAndGeneration nonRecoveredTermGen = new PrimaryTermAndGeneration(
                        nonRecoveredBlobFile.primaryTerm(),
                        StatelessCompoundCommit.parseGenerationFromBlobName(nonRecoveredBlobFile.blobName())
                    );

                    Map<String, BlobLocation> internalFiles = referencedBlobs.getOrDefault(nonRecoveredTermGen, Collections.emptyMap());

                    // create a compound commit blob instance for the new commit
                    var nonRecoveredBlobReference = new BlobReference(nonRecoveredTermGen, internalFiles.keySet());
                    nonRecoveredCommits.add(nonRecoveredBlobReference);

                    // If the recovery commit references files in this commit, ensure we increment a reference
                    if (referencedBlobs.containsKey(nonRecoveredTermGen)) {
                        assert internalFiles.isEmpty() == false;
                        recoveryCommitBlob.incRef(nonRecoveredBlobReference);
                    }

                    internalFiles.forEach((key, value) -> {
                        var previous = blobLocations.put(key, new CommitAndBlobLocation(nonRecoveredBlobReference, value));
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

            ArrayList<BlobReference> previousCommits = new ArrayList<>();
            BlobReference current;
            while ((current = nonRecoveredCommits.poll()) != null) {
                blobReferences.put(current.getPrimaryTermAndGeneration(), current);
                for (BlobReference previous : previousCommits) {
                    current.incRef(previous);
                }

                previousCommits.add(current);
            }

            blobReferences.put(recoveryCommitBlob.getPrimaryTermAndGeneration(), recoveryCommitBlob);

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

            blobReferences.values().forEach(this::initializeUnpromotableCommitReferences);
            blobReferences.values()
                .forEach(commit -> trackOutstandingUnpromotableShardCommitRef(currentUnpromotableShardAssignedNodes, commit));

            // Decrement all of the non-recovered commits since we do not reference them locally
            blobReferences.values()
                .stream()
                .filter(b -> b.getPrimaryTermAndGeneration().equals(recoveryCommitBlob.getPrimaryTermAndGeneration()) == false)
                .forEach(b -> {
                    // Deleted and unused locally such that only the recovery commit retains this compound commit
                    b.deleted();
                    b.closedLocalReaders();
                });

            recoveredPrimaryTerm = recoveredCommit.primaryTerm();
            recoveredGeneration = recoveredCommit.generation();
            assert assertRecoveredCommitFilesHaveBlobLocations(Map.copyOf(recoveredCommit.commitFiles()), Map.copyOf(blobLocations));
            handleUploadedCommit(recoveredCommit);
        }

        public BlobReference markCommitCreated(
            long primaryTerm,
            long generation,
            Collection<String> commitFiles,
            Set<String> additionalFiles
        ) {
            assert primaryTerm == allocationPrimaryTerm;

            pendingUploadGenerations.add(generation);

            return addCommitData(primaryTerm, generation, commitFiles, additionalFiles);
        }

        private BlobReference addCommitData(
            long primaryTerm,
            long generation,
            Collection<String> commitFiles,
            Set<String> additionalFiles
        ) {
            assert isDeleted == false : "shard " + shardId + " is deleted when trying to add commit data";
            // create a compound commit blob instance for the new commit
            var blobReference = new BlobReference(primaryTerm, generation, additionalFiles);
            if (blobReferences.putIfAbsent(blobReference.getPrimaryTermAndGeneration(), blobReference) != null) {
                throw new IllegalArgumentException(blobReference + " already exists");
            }

            // add pending blob locations for new files
            additionalFiles.forEach(fileName -> {
                var previous = blobLocations.put(fileName, new CommitAndBlobLocation(blobReference, null));
                assert previous == null || isGenerationalFile(fileName) : fileName + ':' + previous + ':' + blobReference;
            });

            // if there are external files the new instance must reference the corresponding commit blob instances
            commitFiles.forEach(fileName -> {
                if (additionalFiles.contains(fileName) == false) {
                    final var commitAndBlobLocation = blobLocations.get(fileName);
                    if (commitAndBlobLocation == null) {
                        final var message = Strings.format(
                            """
                                [%s] blobLocations missing [%s]; \
                                primaryTerm=%d, generation=%d, commitFiles=%s, additionalFiles=%s, blobLocations=%s""",
                            shardId,
                            fileName,
                            primaryTerm,
                            generation,
                            commitFiles,
                            additionalFiles,
                            blobLocations.keySet()
                        );
                        assert false : message;
                        throw new IllegalStateException(message);
                    }
                    blobReference.incRef(commitAndBlobLocation.blobReference());
                }
            });
            initializeUnpromotableCommitReferences(blobReference);
            return blobReference;
        }

        private void initializeUnpromotableCommitReferences(BlobReference blobReference) {
            Set<String> previous = unpromotableBlobReferences.put(blobReference, Set.of());
            assert previous == null;
        }

        public void markCommitDeleted(long generation) {
            long primaryTerm = generation == recoveredGeneration ? recoveredPrimaryTerm : allocationPrimaryTerm;
            final var blobReference = blobReferences.get(new PrimaryTermAndGeneration(primaryTerm, generation));
            assert blobReference != null : generation;
            blobReference.deleted();
        }

        public void markCommitUploaded(StatelessCompoundCommit commit) {
            boolean removed = pendingUploadGenerations.remove(commit.generation());
            assert removed;
            handleUploadedCommit(commit);
        }

        private void handleUploadedCommit(StatelessCompoundCommit commit) {
            assert isDeleted == false : "shard " + shardId + " is deleted when trying to handle uploaded commit " + commit;
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

        private void sendNewCommitNotification(BlobReference blobReference, StatelessCompoundCommit commit) {
            assert commit != null;
            var shardRoutingTable = shardRouting.apply(commit.shardId());
            Set<String> nodes = shardRoutingTable.unpromotableShards()
                .stream()
                .filter(ShardRouting::assignedToNode)
                .map(ShardRouting::currentNodeId)
                .collect(Collectors.toSet());
            trackOutstandingUnpromotableShardCommitRef(nodes, blobReference);
            lastNewCommitNotificationSentTimestamp = threadPool.relativeTimeInMillis();
            NewCommitNotificationRequest request = new NewCommitNotificationRequest(shardRoutingTable, commit);
            client.execute(TransportNewCommitNotificationAction.TYPE, request, ActionListener.wrap(response -> {
                onNewCommitNotificationResponse(commit.generation(), nodes, response.getUsedPrimaryTermAndGenerations());
                var consumer = commitNotificationSuccessListeners.get(shardId);
                if (consumer != null) {
                    consumer.accept(commit.generation());
                }
            },
                e -> logger.warn(
                    () -> format("%s failed to notify unpromotables after upload of commit [%s]", shardId, commit.generation()),
                    e
                )
            ));
        }

        private void maybeResendLatestNewCommitNotification() {
            StatelessCompoundCommit latestStatelessCompoundCommitUploaded = null;
            BlobReference latestBlobReference = null;

            // Get latest uploaded stateless compound commit and the respective compound commit blob
            synchronized (this) {
                if (latestUploadedCommit == null) {
                    return;
                }
                latestStatelessCompoundCommitUploaded = latestUploadedCommit;
                PrimaryTermAndGeneration termGen = latestStatelessCompoundCommitUploaded.primaryTermAndGeneration();
                latestBlobReference = blobReferences.get(termGen);
                assert latestBlobReference != null : "could not find latest " + termGen + " in compound commit blobs";
            }

            // Resend new commit notification only if there are unpromotable references for older blob references
            if (unpromotableBlobReferences.size() == 1 && unpromotableBlobReferences.keySet().contains(latestBlobReference)) {
                lastNewCommitNotificationSentTimestamp = threadPool.relativeTimeInMillis();
                return;
            }

            logger.debug("sending new commit notifications for inactive shard [{}]", shardId);
            sendNewCommitNotification(latestBlobReference, latestStatelessCompoundCommitUploaded);
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

            if (isDeleted) {
                updateUnpromotableShardAssignedNodes(Set.of(), Long.MAX_VALUE); // clear all unpromotable references
                blobReferences.values().forEach(blobReference -> {
                    blobReference.closedLocalReaders();
                    blobReference.deleted();
                });
            }
        }

        /**
         * Marks the shard as deleted. Any related {@link ShardCommitState.BlobReference} will be deleted in the upcoming {@link #close()}.
         */
        public void delete() {
            // idempotent
            synchronized (this) {
                isDeleted = true;
            }
        }

        void onNewCommitNotificationResponse(
            long notificationGeneration,
            Set<String> nodes,
            Set<PrimaryTermAndGeneration> usedPrimaryTermAndGenerations
        ) {
            if (isClosed) {
                return;
            }

            if (generationNotified.getAndAccumulate(notificationGeneration, Math::max) > notificationGeneration) {
                // no need to process backwards commit notifications
                // (but would be safe - and we rely on it, this check is just an optimistic check)
                return;
            }
            // TODO: provide Map<nodeId, Set<PrimaryTermAndGeneration>>

            for (BlobReference commit : unpromotableBlobReferences.keySet()) {
                // we are allowed to shrink the set of unpromotable nodes for any generation <= notificationGeneration, since after the
                // notification generation has
                // been refreshed on the search shard, we know that the shard will never add more use of any earlier generations.
                if (commit.getPrimaryTermAndGeneration().generation() <= notificationGeneration
                    && usedPrimaryTermAndGenerations.contains(commit.getPrimaryTermAndGeneration()) == false) {
                    // remove nodes from the set. Any search shard registered during initialization will be left until it starts responding.
                    Set<String> result = unpromotableBlobReferences.computeIfPresent(commit, (k, v) -> Sets.difference(v, nodes));
                    // only mark it closed for readers if it is not the newest commit, since we want a new search shard to be able to use at
                    // least that commit (relevant only in case there are no search shards currently).
                    maybeRemoveAndCloseExternalReaders(notificationGeneration, commit, result);
                }
            }
        }

        void updateUnpromotableShardAssignedNodes(Set<String> currentUnpromotableNodes) {
            updateUnpromotableShardAssignedNodes(currentUnpromotableNodes, this.generationNotified.get());
        }

        void updateUnpromotableShardAssignedNodes(Set<String> currentUnpromotableNodes, long generationNotified) {
            for (Map.Entry<BlobReference, Set<String>> entry : unpromotableBlobReferences.entrySet()) {
                Set<String> result = unpromotableBlobReferences.computeIfPresent(
                    entry.getKey(),
                    (k, v) -> Sets.intersection(v, currentUnpromotableNodes)
                );
                maybeRemoveAndCloseExternalReaders(generationNotified, entry.getKey(), result);
            }
        }

        private void maybeRemoveAndCloseExternalReaders(long notificationGeneration, BlobReference commit, Set<String> result) {
            if (result != null && result.isEmpty() && notificationGeneration > commit.getPrimaryTermAndGeneration().generation()) {
                if (unpromotableBlobReferences.remove(commit, Set.of())) {
                    commit.closedExternalReaders();
                }
            }
        }

        void trackOutstandingUnpromotableShardCommitRef(Set<String> nodes, BlobReference blobReference) {
            boolean success = registerUnpromoteableCommitRefs(nodes, blobReference);
            // it is fine if a newer commit notification removed the registration, since then blobReference cannot be used
            // by search shard readers anymore.
            assert success || blobReference.getPrimaryTermAndGeneration().generation() < generationNotified.get();
        }

        /**
         * Register commit used by unpromotable, returning the commit to use by the unpromotable.
         */
        BlobReference registerCommitForUnpromotableRecovery(ClusterState state, String nodeId, PrimaryTermAndGeneration commit) {
            // Find a commit (starting with the requested one) that could be used for unpromotable recovery
            var compoundCommit = blobReferences.get(commit);
            if (compoundCommit == null) {
                long lastUploadedGeneration = getMaxUploadedGeneration();
                if (lastUploadedGeneration != -1) {
                    long primaryTerm = lastUploadedGeneration == recoveredGeneration ? recoveredPrimaryTerm : allocationPrimaryTerm;
                    compoundCommit = blobReferences.get(new PrimaryTermAndGeneration(primaryTerm, lastUploadedGeneration));
                }
            }
            // If the indexing shard is not finished initializing from the object store, we are not
            // able to register the commit for recovery. For now, fail the registration request.
            // TODO: we should be able to handle this case by either retrying the registration or keep the
            // registration and run it after the indexing shard is finished initializing.
            if (compoundCommit == null) {
                throw new NoShardAvailableActionException(shardId, "indexing shard is initializing");
            }
            if (compoundCommit.primaryTermAndGeneration.compareTo(commit) < 0) {
                var message = Strings.format(
                    "requested commit to register (%s) is newer than the newest known local commit (%s)",
                    commit,
                    compoundCommit
                );
                if (state.getMinTransportVersion().onOrAfter(TransportVersions.RECOVERY_COMMIT_TOO_NEW_EXCEPTION_ADDED)) {
                    throw new RecoveryCommitTooNewException(shardId, message);
                } else {
                    throw new ElasticsearchException(message);
                }
            }
            // Register the commit that is going to be used for unpromotable recovery
            long previousGenerationUploaded = -1;
            while (true) {
                if (compoundCommit != null && registerUnpromoteableCommitRefs(Set.of(nodeId), compoundCommit)) {
                    assert compoundCommit.externalReadersClosed.get() == false;
                    return compoundCommit;
                } else {
                    long generation = getMaxUploadedGeneration();
                    assert generation > previousGenerationUploaded;
                    previousGenerationUploaded = generation;
                    compoundCommit = blobReferences.get(new PrimaryTermAndGeneration(allocationPrimaryTerm, generation));
                    assert compoundCommit != null || getMaxUploadedGeneration() > generation;
                }
            }
        }

        boolean registerUnpromoteableCommitRefs(Set<String> nodes, BlobReference compoundCommit) {
            Set<String> immutableNodes = Set.copyOf(nodes);
            Set<String> result = unpromotableBlobReferences.computeIfPresent(compoundCommit, (k, v) -> Sets.union(v, immutableNodes));
            return result != null;
        }

        public LongConsumer closedLocalReadersForGeneration() {
            return generation -> {
                long primaryTerm = generation == recoveredGeneration ? recoveredPrimaryTerm : allocationPrimaryTerm;
                BlobReference blobReference = blobReferences.get(new PrimaryTermAndGeneration(primaryTerm, generation));
                if (blobReference != null) {
                    blobReference.closedLocalReaders();
                } // else assume an idempotent call when already deleted.
            };
        }

        /**
         * A ref counted instance representing a (compound commit) blob reference to the object store. It can reference some other previous
         * blob reference instances (if the commit has external files) which are decRef when the current instance ref count reaches zero.
         */
        private class BlobReference extends AbstractRefCounted {
            private final PrimaryTermAndGeneration primaryTermAndGeneration;
            private final Set<String> internalFiles;
            private final Set<BlobReference> references;
            private final AtomicBoolean deleted = new AtomicBoolean();
            private final AtomicBoolean readersClosed = new AtomicBoolean();
            private final AtomicBoolean externalReadersClosed = new AtomicBoolean();

            BlobReference(long primaryTerm, long generation, Set<String> internalFiles) {
                this(new PrimaryTermAndGeneration(primaryTerm, generation), internalFiles);
            }

            BlobReference(PrimaryTermAndGeneration primaryTermAndGeneration, Set<String> internalFiles) {
                this.primaryTermAndGeneration = primaryTermAndGeneration;
                this.internalFiles = Set.copyOf(internalFiles);
                this.references = newSetFromMap(new IdentityHashMap<>());
                // we both decRef on delete, closedLocalReaders and closedExternalReaders, hence the extra incRefs (in addition to the
                // 1 ref given by AbstractRefCounted constructor)
                this.incRef();
                this.incRef();
            }

            public PrimaryTermAndGeneration getPrimaryTermAndGeneration() {
                return primaryTermAndGeneration;
            }

            public void incRef(BlobReference other) {
                assert hasReferences() : this;
                assert other.hasReferences() : other;
                // incRef three times since we expect all commits to be both deleted, locally unused and externally unused
                other.incRef();
                other.incRef();
                other.incRef();
                if (references.add(other) == false) {
                    other.decRef();
                    other.decRef();
                    other.decRef();
                }
            }

            public void deleted() {
                // be idempotent.
                if (deleted.compareAndSet(false, true)) {
                    references.forEach(AbstractRefCounted::decRef);
                    decRef();
                }
            }

            public void closedLocalReaders() {
                // be idempotent.
                if (readersClosed.compareAndSet(false, true)) {
                    references.forEach(AbstractRefCounted::decRef);
                    decRef();
                }
            }

            public void closedExternalReaders() {
                if (externalReadersClosed.compareAndSet(false, true)) {
                    references.forEach(AbstractRefCounted::decRef);
                    decRef();
                } else {
                    assert false : "external readers already closed for [" + this + "]";
                }
            }

            @Override
            protected void closeInternal() {
                final BlobReference released = this;
                internalFiles.forEach(fileName -> {
                    blobLocations.compute(fileName, (file, commitAndBlobLocation) -> {
                        var existing = commitAndBlobLocation.blobReference();
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
                var removed = blobReferences.remove(primaryTermAndGeneration);
                assert removed == this;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                BlobReference that = (BlobReference) o;
                return Objects.equals(primaryTermAndGeneration, that.primaryTermAndGeneration);
            }

            @Override
            public int hashCode() {
                return Objects.hash(primaryTermAndGeneration);
            }

            @Override
            public String toString() {
                return "Compound commit blob "
                    + primaryTermAndGeneration
                    + " ["
                    + deleted.get()
                    + ","
                    + readersClosed.get()
                    + ","
                    + externalReadersClosed.get()
                    + ","
                    + refCount()
                    + "]";
            }
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // TODO: maybe give a grace period if the node left?
        try {
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
        } finally {
            waitForClusterStateVersion.notifyVersionProcessed(event.state().version());
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

    /**
     *
     * @param commit the commit to register
     * @param shardId the shard id to register for
     * @param nodeId the nodeId using the commit
     * @param state the cluster state already applied on this node, but possibly not handled in this object yet.
     * @param listener notified when available.
     */
    public void registerCommitForUnpromotableRecovery(
        PrimaryTermAndGeneration commit,
        ShardId shardId,
        String nodeId,
        ClusterState state,
        ActionListener<PrimaryTermAndGeneration> listener
    ) {
        // todo: assert clusterStateVersion <= clusterService.state().version();
        waitForClusterStateProcessed(state.version(), () -> {
            ActionListener.completeWith(listener, () -> {
                var shardCommitsState = getSafe(shardsCommitsStates, shardId);
                var compoundCommit = shardCommitsState.registerCommitForUnpromotableRecovery(state, nodeId, commit);
                var proposed = compoundCommit.primaryTermAndGeneration;
                assert proposed.compareTo(commit) >= 0
                    : Strings.format(
                        "Proposed commit (%s) for unpromotable recovery must be newer that the requested one (%s)",
                        proposed,
                        commit
                    );
                return proposed;
            });
        });
    }

    private record CommitAndBlobLocation(ShardCommitState.BlobReference blobReference, @Nullable BlobLocation blobLocation) {
        @Override
        public String toString() {
            return "CommitAndBlobLocation [blobReference=" + blobReference + ", blobLocation=" + blobLocation + ']';
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

    private void waitForClusterStateProcessed(long clusterStateVersion, Runnable whenDone) {
        waitForClusterStateVersion.waitUntilVersion(clusterStateVersion, () -> threadPool.generic().execute(whenDone));
    }

    private static boolean assertRecoveredCommitFilesHaveBlobLocations(
        Map<String, BlobLocation> recoveredCommitFiles,
        Map<String, CommitAndBlobLocation> blobLocations
    ) {
        for (var commitFile : recoveredCommitFiles.entrySet()) {
            var commitFileName = commitFile.getKey();
            assert blobLocations.containsKey(commitFileName)
                : "Missing blob location for file ["
                    + commitFile
                    + "] referenced at location ["
                    + commitFile.getValue()
                    + "] in recovered commit";
        }
        return true;
    }
}
