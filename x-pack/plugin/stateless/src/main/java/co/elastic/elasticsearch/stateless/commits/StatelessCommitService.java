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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
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
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.core.Strings.format;

public class StatelessCommitService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(StatelessCommitService.class);

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

    public StatelessCommitService(
        ObjectStoreService objectStoreService,
        ClusterService clusterService,
        Client client,
        StatelessCommitCleaner commitCleaner
    ) {
        this(
            objectStoreService,
            () -> clusterService.localNode().getEphemeralId(),
            (shardId) -> clusterService.state().routingTable().shardRoutingTable(shardId),
            clusterService.threadPool(),
            client,
            commitCleaner
        );
    }

    public StatelessCommitService(
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
                var shardRoutingTable = shardRouting.apply(commit.shardId());
                var releasables = shardRoutingTable.unpromotableShards()
                    .stream()
                    .map(
                        shardRouting -> commitState.trackOutstandingUnpromotableShardCommitRef(
                            shardRouting.currentNodeId(),
                            compoundCommitBlob
                        )
                    )
                    .toList();

                NewCommitNotificationRequest request = new NewCommitNotificationRequest(shardRoutingTable, commit);
                client.sendRequest(request, ActionListener.releaseAfter(ActionListener.wrap(response -> {
                    commitState.trackReferencedCommitsByUnpromotableShards(response.getUsedPrimaryTermAndGenerations());
                    var consumer = commitNotificationSuccessListeners.get(shardId);
                    if (consumer != null) {
                        consumer.accept(generation);
                    }
                }, e -> logger.warn(() -> format("%s failed to notify unpromotables after upload of commit [%s]", shardId, generation), e)),
                    Releasables.wrap(releasables)
                ));
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

    public void register(ShardId shardId) {
        ShardCommitState existing = shardsCommitsStates.put(shardId, new ShardCommitState(shardId));
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

    public void addConsumerForNewUploadedCommit(ShardId shardId, Consumer<StatelessCompoundCommit> listener) {
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
        private final Set<Long> pendingUploadGenerations = ConcurrentCollections.newConcurrentSet();
        private List<Tuple<Long, ActionListener<Void>>> generationListeners = null;
        private List<Consumer<StatelessCompoundCommit>> uploadedCommitConsumers = null;
        private volatile long recoveredGeneration = -1;
        private volatile long generationUploaded = -1;
        private volatile boolean isClosed;

        // map generations to compound commit blob instances
        private final Map<Long, CompoundCommitBlob> compoundCommitBlobs = new ConcurrentHashMap<>();
        private final Map<String, UnpromotableShardCommitReferences> unpromotableShardCommitReferencesByNode = new ConcurrentHashMap<>();

        // maps file names to their (maybe future) compound commit blob & blob location
        private final Map<String, CommitAndBlobLocation> blobLocations = new ConcurrentHashMap<>();

        private ShardCommitState(ShardId shardId) {
            this.shardId = shardId;
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

        private void markCommitRecovered(StatelessCompoundCommit recoveredCommit, Set<BlobFile> unreferencedBlobs) {
            assert recoveredCommit != null;
            assert compoundCommitBlobs.isEmpty() : compoundCommitBlobs;
            assert blobLocations.isEmpty() : blobLocations;

            Map<PrimaryTermAndGeneration, Map<String, BlobLocation>> toInit = new HashMap<>();
            for (Map.Entry<String, BlobLocation> referencedBlob : recoveredCommit.commitFiles().entrySet()) {
                if (recoveredCommit.getInternalFiles().contains(referencedBlob.getKey()) == false) {
                    toInit.computeIfAbsent(
                        new PrimaryTermAndGeneration(
                            referencedBlob.getValue().primaryTerm(),
                            referencedBlob.getValue().compoundFileGeneration()
                        ),
                        primaryTermAndGeneration -> new HashMap<>()
                    ).put(referencedBlob.getKey(), referencedBlob.getValue());
                }

            }

            for (BlobFile blobFile : unreferencedBlobs) {
                if (StatelessCompoundCommit.startsWithBlobPrefix(blobFile.blobName())) {
                    PrimaryTermAndGeneration primaryTermAndGeneration = new PrimaryTermAndGeneration(
                        blobFile.primaryTerm(),
                        StatelessCompoundCommit.parseGenerationFromBlobName(blobFile.blobName())
                    );
                    Map<String, BlobLocation> internalFiles = toInit.getOrDefault(primaryTermAndGeneration, Collections.emptyMap());

                    // create a compound commit blob instance for the new commit
                    var compoundCommitBlob = new CompoundCommitBlob(
                        primaryTermAndGeneration.primaryTerm(),
                        primaryTermAndGeneration.generation(),
                        internalFiles.keySet()
                    );
                    compoundCommitBlobs.put(primaryTermAndGeneration.generation(), compoundCommitBlob);

                    internalFiles.forEach((key, value) -> {
                        var previous = blobLocations.put(key, new CommitAndBlobLocation(compoundCommitBlob, value));
                        assert previous == null : key + ':' + previous;
                    });

                } else {
                    logger.warn("Found object store file which does not match compound commit file naming pattern: " + blobFile);
                }
            }

            // create a compound commit blob instance for the new commit
            var compoundCommitBlob = new CompoundCommitBlob(
                recoveredCommit.primaryTerm(),
                recoveredCommit.generation(),
                recoveredCommit.getInternalFiles()
            );

            // TODO: We should maybe make each commit blob reference the prior commit
            compoundCommitBlobs.forEach((generation, toReference) -> compoundCommitBlob.incRef(toReference));

            // Deleted and unused locally such that only the recovery commit retains this compound commit
            compoundCommitBlobs.values().forEach(b -> {
                b.deleted();
                b.closedLocalReaders();
                assert b.refCount() == 2 : "recovery commit is assumed locally used and not deleted " + b.refCount();
            });

            compoundCommitBlobs.put(recoveredCommit.generation(), compoundCommitBlob);

            recoveredCommit.getInternalFiles().forEach(fileName -> {
                var previous = blobLocations.put(
                    fileName,
                    new CommitAndBlobLocation(compoundCommitBlob, recoveredCommit.commitFiles().get(fileName))
                );
                assert previous == null : fileName + ':' + previous;
            });

            var currentUnpromotableShardAssignedNodes = shardRouting.apply(shardId)
                .unpromotableShards()
                .stream()
                .map(ShardRouting::currentNodeId)
                .collect(Collectors.toSet());
            updateUnpromotableShardAssignedNodes(currentUnpromotableShardAssignedNodes);

            HashSet<CompoundCommitBlob> blobsToTrack = new HashSet<>(compoundCommitBlobs.values());
            for (UnpromotableShardCommitReferences unpromotableShardCommitReferences : unpromotableShardCommitReferencesByNode.values()) {
                unpromotableShardCommitReferences.trackReferencedCommits(blobsToTrack);
            }

            recoveredGeneration = recoveredCommit.generation();

            handleUploadedCommit(recoveredCommit);
        }

        public CompoundCommitBlob markCommitCreated(
            long primaryTerm,
            long generation,
            Collection<String> commitFiles,
            Set<String> additionalFiles
        ) {
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
            if (compoundCommitBlobs.putIfAbsent(generation, compoundCommitBlob) != null) {
                throw new IllegalArgumentException(
                    "Compound commit blob [primaryTerm=" + primaryTerm + ", generation=" + generation + " already exists"
                );
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
            final var compoundCommitBlob = compoundCommitBlobs.get(generation);
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
            List<ActionListener<StatelessCompoundCommit>> listenersToFire = null;
            List<Tuple<Long, ActionListener<Void>>> listenersToReregister = null;
            synchronized (this) {
                generationUploaded = Math.max(generationUploaded, newGeneration);

                // Generation did not increase so just bail early
                if (generationUploaded != newGeneration) {
                    return;
                }

                if (generationListeners != null) {
                    for (Tuple<Long, ActionListener<Void>> tuple : generationListeners) {
                        Long generation = tuple.v1();
                        if (generationUploaded >= generation) {
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
                ActionListener.onResponse(listenersToFire, commit);
            }
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
                } else if (generationUploaded >= generation) {
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
        public void addConsumerForNewUploadedCommit(Consumer<StatelessCompoundCommit> consumer) {
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
                    var compoundCommit = compoundCommitBlobs.get(usedPrimaryTermAndGeneration.generation());
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

        public LongConsumer closedLocalReadersForGeneration() {
            return generation -> {
                CompoundCommitBlob compoundCommitBlob = compoundCommitBlobs.get(generation);
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
                commitCleaner.deleteCommit(new StatelessCommitCleaner.StaleCompoundCommit(shardId, primaryTermAndGeneration));
                var removed = compoundCommitBlobs.remove(primaryTermAndGeneration.generation());
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
        var compoundCommit = shardCommitsState.compoundCommitBlobs.get(commit.generation());
        if (compoundCommit == null || compoundCommit.tryIncRef() == false) {
            // If the commit requested to be registered is being deleted, we shouldn't be able to acquire a reference to it.
            // In that case, try the latest commit.
            // TODO: add an accessor for the latest commit (should it also incRef?)
            compoundCommit = shardCommitsState.compoundCommitBlobs.get(shardCommitsState.generationUploaded);
            // if the indexing shard is not finished initializing from the object store, we are not
            // able to register the commit for recovery. For now, fail the registration request.
            // TODO (ES-6698): we should be able to handle this case by either retrying the registration or keep the
            // registration and run it after the indexing shard is finished initializing.
            if (compoundCommit == null) {
                throw new NoShardAvailableActionException(shardId, "indexing shard is initializing");
            }
            compoundCommit.incRef();
        }
        try {
            shardCommitsState.trackOutstandingUnpromotableShardCommitRef(nodeId, compoundCommit);
        } finally {
            compoundCommit.decRef();
        }
        assert compoundCommit.primaryTermAndGeneration.primaryTerm() >= commit.primaryTerm()
            && (compoundCommit.primaryTermAndGeneration.primaryTerm() > commit.primaryTerm()
                || compoundCommit.primaryTermAndGeneration.generation() >= commit.generation());
        return compoundCommit.primaryTermAndGeneration;
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
