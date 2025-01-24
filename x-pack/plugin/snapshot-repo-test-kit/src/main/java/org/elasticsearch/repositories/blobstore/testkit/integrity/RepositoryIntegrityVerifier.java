/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.RateLimitingInputStream;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

class RepositoryIntegrityVerifier {
    private static final Logger logger = LogManager.getLogger(RepositoryIntegrityVerifier.class);

    private final LongSupplier currentTimeMillisSupplier;
    private final BlobStoreRepository blobStoreRepository;
    private final RepositoryVerifyIntegrityResponseChunk.Writer responseChunkWriter;
    private final String repositoryName;
    private final RepositoryVerifyIntegrityParams requestParams;
    private final RepositoryData repositoryData;
    private final BooleanSupplier isCancelledSupplier;
    private final CancellableRunner metadataTaskRunner;
    private final CancellableRunner snapshotTaskRunner;
    private final RateLimiter rateLimiter;

    private final Set<String> unreadableSnapshotInfoUuids = ConcurrentCollections.newConcurrentSet();
    private final long snapshotCount;
    private final AtomicLong snapshotProgress = new AtomicLong();
    private final long indexCount;
    private final AtomicLong indexProgress = new AtomicLong();
    private final long indexSnapshotCount;
    private final AtomicLong indexSnapshotProgress = new AtomicLong();
    private final AtomicLong blobsVerified = new AtomicLong();
    private final AtomicLong blobBytesVerified = new AtomicLong();
    private final AtomicLong throttledNanos;
    private final AtomicLong failedShardSnapshotsCount = new AtomicLong();
    private final Set<String> failedShardSnapshotDescriptions = ConcurrentCollections.newConcurrentSet();

    RepositoryIntegrityVerifier(
        LongSupplier currentTimeMillisSupplier,
        BlobStoreRepository blobStoreRepository,
        RepositoryVerifyIntegrityResponseChunk.Writer responseChunkWriter,
        RepositoryVerifyIntegrityParams requestParams,
        RepositoryData repositoryData,
        CancellableThreads cancellableThreads
    ) {
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
        this.blobStoreRepository = blobStoreRepository;
        this.repositoryName = blobStoreRepository.getMetadata().name();
        this.responseChunkWriter = responseChunkWriter;
        this.requestParams = requestParams;
        this.repositoryData = repositoryData;
        this.isCancelledSupplier = cancellableThreads::isCancelled;
        this.snapshotTaskRunner = new CancellableRunner(
            new ThrottledTaskRunner(
                "verify-blob",
                requestParams.blobThreadPoolConcurrency(),
                blobStoreRepository.threadPool().executor(ThreadPool.Names.SNAPSHOT)
            ),
            cancellableThreads
        );
        this.metadataTaskRunner = new CancellableRunner(
            new ThrottledTaskRunner(
                "verify-metadata",
                requestParams.metaThreadPoolConcurrency(),
                blobStoreRepository.threadPool().executor(ThreadPool.Names.SNAPSHOT_META)
            ),
            cancellableThreads
        );

        this.snapshotCount = repositoryData.getSnapshotIds().size();
        this.indexCount = repositoryData.getIndices().size();
        this.indexSnapshotCount = repositoryData.getIndexSnapshotCount();
        this.rateLimiter = new RateLimiter.SimpleRateLimiter(requestParams.maxBytesPerSec().getMbFrac());

        this.throttledNanos = new AtomicLong(requestParams.verifyBlobContents() ? 1 : 0); // nonzero if verifying so status reported
    }

    RepositoryVerifyIntegrityTask.Status getStatus() {
        return new RepositoryVerifyIntegrityTask.Status(
            repositoryName,
            repositoryData.getGenId(),
            repositoryData.getUuid(),
            snapshotCount,
            snapshotProgress.get(),
            indexCount,
            indexProgress.get(),
            indexSnapshotCount,
            indexSnapshotProgress.get(),
            blobsVerified.get(),
            blobBytesVerified.get(),
            throttledNanos.get()
        );
    }

    void start(ActionListener<RepositoryVerifyIntegrityResponse> listener) {
        logger.info(
            """
                [{}] verifying metadata integrity for index generation [{}]: \
                repo UUID [{}], cluster UUID [{}], snapshots [{}], indices [{}], index snapshots [{}]""",
            repositoryName,
            repositoryData.getGenId(),
            repositoryData.getUuid(),
            repositoryData.getClusterUUID(),
            getSnapshotCount(),
            getIndexCount(),
            getIndexSnapshotCount()
        );

        SubscribableListener
            // first verify the top-level properties of the snapshots
            .newForked(this::verifySnapshots)
            .andThen(this::checkFailedShardSnapshotCount)
            // then verify the restorability of each index
            .andThen(this::verifyIndices)
            .andThenAccept(v -> this.ensureNotCancelled())
            // see if the repository data has changed
            .<RepositoryData>andThen(
                l -> blobStoreRepository.getRepositoryData(blobStoreRepository.threadPool().executor(ThreadPool.Names.MANAGEMENT), l)
            )
            // log the completion and return the result
            .addListener(new ActionListener<>() {
                @Override
                public void onResponse(RepositoryData finalRepositoryData) {
                    logger.info(
                        "[{}] completed verifying metadata integrity for index generation [{}]: repo UUID [{}], cluster UUID [{}]",
                        repositoryName,
                        repositoryData.getGenId(),
                        repositoryData.getUuid(),
                        repositoryData.getClusterUUID()
                    );
                    listener.onResponse(new RepositoryVerifyIntegrityResponse(getStatus(), finalRepositoryData.getGenId()));
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn(
                        () -> Strings.format(
                            "[%s] failed verifying metadata integrity for index generation [%d]: repo UUID [%s], cluster UUID [%s]",
                            repositoryName,
                            repositoryData.getGenId(),
                            repositoryData.getUuid(),
                            repositoryData.getClusterUUID()
                        ),
                        e
                    );
                    listener.onFailure(e);
                }
            });
    }

    private void ensureNotCancelled() {
        if (isCancelledSupplier.getAsBoolean()) {
            throw new TaskCancelledException("task cancelled");
        }
    }

    private void verifySnapshots(ActionListener<Void> listener) {
        new SnapshotsVerifier().run(listener);
    }

    /**
     * Verifies the top-level snapshot metadata in the repo, including {@link SnapshotInfo} and optional {@link Metadata} blobs.
     */
    private class SnapshotsVerifier {
        final Map<String, Set<String>> indexNamesBySnapshotName;

        SnapshotsVerifier() {
            indexNamesBySnapshotName = Maps.newHashMapWithExpectedSize(repositoryData.getIndices().size());
            for (final var indexId : repositoryData.getIndices().values()) {
                for (final var snapshotId : repositoryData.getSnapshots(indexId)) {
                    indexNamesBySnapshotName.computeIfAbsent(snapshotId.getName(), ignored -> new HashSet<>()).add(indexId.getName());
                }
            }
        }

        void run(ActionListener<Void> listener) {
            var listeners = new RefCountingListener(listener);
            runThrottled(
                Iterators.failFast(
                    repositoryData.getSnapshotIds().iterator(),
                    () -> isCancelledSupplier.getAsBoolean() || listeners.isFailing()
                ),
                (releasable, snapshotId) -> new SnapshotVerifier(snapshotId).run(
                    ActionListener.assertOnce(ActionListener.releaseAfter(listeners.acquire(), releasable))
                ),
                requestParams.snapshotVerificationConcurrency(),
                snapshotProgress,
                listeners
            );
        }

        /**
         * Verifies a single snapshot's metadata, including its {@link SnapshotInfo} and optional {@link Metadata} blobs.
         */
        private class SnapshotVerifier {
            private final SnapshotId snapshotId;

            SnapshotVerifier(SnapshotId snapshotId) {
                this.snapshotId = snapshotId;
            }

            void run(ActionListener<Void> listener) {
                if (isCancelledSupplier.getAsBoolean()) {
                    // getSnapshotInfo does its own forking, so we must check for cancellation here
                    listener.onResponse(null);
                    return;
                }

                blobStoreRepository.getSnapshotInfo(snapshotId, new ActionListener<>() {
                    @Override
                    public void onResponse(SnapshotInfo snapshotInfo) {
                        verifySnapshotInfo(snapshotInfo, listener);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        unreadableSnapshotInfoUuids.add(snapshotId.getUUID());
                        anomaly("failed to load snapshot info").snapshotId(snapshotId).exception(e).write(listener);
                    }
                });
            }

            void verifySnapshotInfo(SnapshotInfo snapshotInfo, ActionListener<Void> listener) {
                final var chunkBuilder = new RepositoryVerifyIntegrityResponseChunk.Builder(
                    responseChunkWriter,
                    RepositoryVerifyIntegrityResponseChunk.Type.SNAPSHOT_INFO,
                    currentTimeMillisSupplier.getAsLong()
                ).snapshotInfo(snapshotInfo);

                // record the SnapshotInfo in the response
                final var chunkWrittenStep = SubscribableListener.newForked(chunkBuilder::write);

                if (failedShardSnapshotsCount.get() < requestParams.maxFailedShardSnapshots()) {
                    for (final var shardFailure : snapshotInfo.shardFailures()) {
                        if (failedShardSnapshotsCount.getAndIncrement() < requestParams.maxFailedShardSnapshots()) {
                            failedShardSnapshotDescriptions.add(
                                getShardSnapshotDescription(snapshotId, shardFailure.index(), shardFailure.shardId())
                            );
                        }
                    }
                } else {
                    failedShardSnapshotsCount.addAndGet(snapshotInfo.shardFailures().size());
                }

                // check the indices in the SnapshotInfo match those in RepositoryData
                final var snapshotContentsOkStep = chunkWrittenStep.<Void>andThen(l -> {
                    if (Set.copyOf(snapshotInfo.indices()).equals(indexNamesBySnapshotName.get(snapshotId.getName()))) {
                        l.onResponse(null);
                    } else {
                        anomaly("snapshot contents mismatch").snapshotId(snapshotId).write(l);
                    }
                });

                // check the global metadata is readable if present
                final var globalMetadataOkStep = Boolean.TRUE.equals(snapshotInfo.includeGlobalState())
                    ? snapshotContentsOkStep.<Void>andThen(this::verifySnapshotGlobalMetadata)
                    : snapshotContentsOkStep;

                globalMetadataOkStep.addListener(listener);
            }

            private void verifySnapshotGlobalMetadata(ActionListener<Void> listener) {
                metadataTaskRunner.run(ActionRunnable.wrap(listener, l -> {
                    try {
                        blobStoreRepository.getSnapshotGlobalMetadata(snapshotId);
                        // no checks here, loading it is enough
                        l.onResponse(null);
                    } catch (Exception e) {
                        anomaly("failed to load global metadata").snapshotId(snapshotId).exception(e).write(l);
                    }
                }));
            }
        }
    }

    private void checkFailedShardSnapshotCount(ActionListener<Void> listener) {
        if (failedShardSnapshotDescriptions.size() < failedShardSnapshotsCount.get()) {
            listener.onFailure(
                new RepositoryVerificationException(
                    repositoryName,
                    Strings.format(
                        """
                            Cannot verify the integrity of all index snapshots because this repository contains too many shard snapshot \
                            failures: there are [%d] shard snapshot failures but [?%s] is set to [%d]. \
                            Please increase this limit if it is safe to do so.""",
                        failedShardSnapshotsCount.get(),
                        RepositoryVerifyIntegrityParams.MAX_FAILED_SHARD_SNAPSHOTS,
                        requestParams.maxFailedShardSnapshots()
                    )
                )
            );
        } else {
            listener.onResponse(null);
        }
    }

    private void verifyIndices(ActionListener<Void> listener) {
        var listeners = new RefCountingListener(listener);
        runThrottled(
            Iterators.failFast(
                repositoryData.getIndices().values().iterator(),
                () -> isCancelledSupplier.getAsBoolean() || listeners.isFailing()
            ),
            (releasable, indexId) -> new IndexVerifier(indexId).run(ActionListener.releaseAfter(listeners.acquire(), releasable)),
            requestParams.indexVerificationConcurrency(),
            indexProgress,
            listeners
        );
    }

    /**
     * Verifies the integrity of the snapshots of a specific index
     */
    private class IndexVerifier {
        private final IndexId indexId;
        private final ShardContainerContentsDeduplicator shardContainerContentsDeduplicator = new ShardContainerContentsDeduplicator();
        private final IndexDescriptionsDeduplicator indexDescriptionsDeduplicator = new IndexDescriptionsDeduplicator();
        private final AtomicInteger totalSnapshotCounter = new AtomicInteger();
        private final AtomicInteger restorableSnapshotCounter = new AtomicInteger();

        IndexVerifier(IndexId indexId) {
            this.indexId = indexId;
        }

        void run(ActionListener<Void> listener) {
            SubscribableListener

                .<Void>newForked(l -> {
                    var listeners = new RefCountingListener(1, l);
                    runThrottled(
                        Iterators.failFast(
                            repositoryData.getSnapshots(indexId).iterator(),
                            () -> isCancelledSupplier.getAsBoolean() || listeners.isFailing()
                        ),
                        (releasable, snapshotId) -> verifyIndexSnapshot(
                            snapshotId,
                            ActionListener.releaseAfter(listeners.acquire(), releasable)
                        ),
                        requestParams.indexSnapshotVerificationConcurrency(),
                        indexSnapshotProgress,
                        listeners
                    );
                })
                .<Void>andThen(l -> {
                    ensureNotCancelled();
                    new RepositoryVerifyIntegrityResponseChunk.Builder(
                        responseChunkWriter,
                        RepositoryVerifyIntegrityResponseChunk.Type.INDEX_RESTORABILITY,
                        currentTimeMillisSupplier.getAsLong()
                    ).indexRestorability(indexId, totalSnapshotCounter.get(), restorableSnapshotCounter.get()).write(l);
                })
                .addListener(listener);
        }

        private void verifyIndexSnapshot(SnapshotId snapshotId, ActionListener<Void> listener) {
            totalSnapshotCounter.incrementAndGet();
            indexDescriptionsDeduplicator.get(snapshotId).<Void>andThen((l, indexDescription) -> {
                if (indexDescription == null) {
                    // index metadata was unreadable; anomaly already reported, skip further verification of this index snapshot
                    l.onResponse(null);
                } else {
                    new ShardSnapshotsVerifier(snapshotId, indexDescription).run(l);
                }
            }).addListener(listener);
        }

        /**
         * Information about the contents of the {@code ${REPO}/indices/${INDEX}/${SHARD}/} container, shared across the verifications of
         * each snapshot of this shard.
         *
         * @param shardId the numeric shard ID.
         * @param blobsByName the {@link BlobMetadata} for every blob in the container, keyed by blob name.
         * @param shardGeneration the current {@link ShardGeneration} for this shard, identifying the current {@code index-${UUID}} blob.
         * @param filesByPhysicalNameBySnapshotName a {@link BlobStoreIndexShardSnapshot.FileInfo} for every tracked file, keyed by snapshot
         *                                          name and then by the file's physical name.
         * @param blobContentsListeners a threadsafe mutable map, keyed by file name, for every tracked file that the verification process
         *                              encounters. Used to avoid double-counting the size of any files, and also to deduplicate work to
         *                              verify their contents if {@code ?verify_blob_contents} is set.
         */
        private record ShardContainerContents(
            int shardId,
            Map<String, BlobMetadata> blobsByName,
            @Nullable /* if shard gen is not defined */
            ShardGeneration shardGeneration,
            @Nullable /* if shard gen blob could not be read */
            Map<String, Map<String, BlobStoreIndexShardSnapshot.FileInfo>> filesByPhysicalNameBySnapshotName,
            Map<String, SubscribableListener<Void>> blobContentsListeners
        ) {}

        /**
         * Verifies the integrity of the shard snapshots of a specific index snapshot
         */
        private class ShardSnapshotsVerifier {
            private final SnapshotId snapshotId;
            private final IndexDescription indexDescription;
            private final AtomicInteger restorableShardCount = new AtomicInteger();

            ShardSnapshotsVerifier(SnapshotId snapshotId, IndexDescription indexDescription) {
                this.snapshotId = snapshotId;
                this.indexDescription = indexDescription;
            }

            void run(ActionListener<Void> listener) {
                try (var listeners = new RefCountingListener(1, listener.map(v -> {
                    if (unreadableSnapshotInfoUuids.contains(snapshotId.getUUID()) == false
                        && indexDescription.shardCount() == restorableShardCount.get()) {
                        restorableSnapshotCounter.incrementAndGet();
                    }
                    return v;
                }))) {
                    for (int shardId = 0; shardId < indexDescription.shardCount(); shardId++) {
                        if (failedShardSnapshotDescriptions.contains(getShardSnapshotDescription(snapshotId, indexId.getName(), shardId))) {
                            continue;
                        }

                        shardContainerContentsDeduplicator.get(shardId)
                            // deduplicating reads of shard container contents
                            .<Void>andThen((l, shardContainerContents) -> {
                                if (shardContainerContents == null) {
                                    // shard container contents was unreadable; anomaly already reported, skip further verification
                                    l.onResponse(null);
                                } else {
                                    new ShardSnapshotVerifier(shardContainerContents).run(l);
                                }
                            })
                            .addListener(listeners.acquire());
                    }
                }
            }

            /**
             * Verifies the integrity of a specific shard snapshot
             */
            private class ShardSnapshotVerifier {
                private final ShardContainerContents shardContainerContents;
                private volatile boolean isRestorable = true;

                ShardSnapshotVerifier(ShardContainerContents shardContainerContents) {
                    this.shardContainerContents = shardContainerContents;
                }

                void run(ActionListener<Void> listener) {
                    metadataTaskRunner.run(ActionRunnable.wrap(listener, this::verifyShardSnapshot));
                }

                private void verifyShardSnapshot(ActionListener<Void> listener) {
                    final var shardId = shardContainerContents.shardId();
                    final BlobStoreIndexShardSnapshot blobStoreIndexShardSnapshot;
                    try {
                        blobStoreIndexShardSnapshot = blobStoreRepository.loadShardSnapshot(
                            blobStoreRepository.shardContainer(indexId, shardId),
                            snapshotId
                        );
                    } catch (Exception e) {
                        anomaly("failed to load shard snapshot").snapshotId(snapshotId)
                            .shardDescription(indexDescription, shardId)
                            .exception(e)
                            .write(listener);
                        return;
                    }

                    final var listeners = new RefCountingListener(1, listener.map(v -> {
                        if (isRestorable) {
                            restorableShardCount.incrementAndGet();
                        }
                        return v;
                    }));
                    final var shardGenerationConsistencyListener = listeners.acquire();

                    runThrottled(
                        Iterators.failFast(
                            blobStoreIndexShardSnapshot.indexFiles().iterator(),
                            () -> isCancelledSupplier.getAsBoolean() || listeners.isFailing()
                        ),
                        (releasable, fileInfo) -> verifyFileInfo(fileInfo, ActionListener.releaseAfter(listeners.acquire(), releasable)),
                        1,
                        blobsVerified,
                        listeners
                    );

                    // NB this next step doesn't matter for restorability, it is just verifying that the shard gen blob matches the shard
                    // snapshot blob
                    verifyShardGenerationConsistency(blobStoreIndexShardSnapshot, shardGenerationConsistencyListener);
                }

                /**
                 * Checks that the given {@link org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo} matches
                 * the actual blob in the repository.
                 */
                private void verifyFileInfo(BlobStoreIndexShardSnapshot.FileInfo fileInfo, ActionListener<Void> listener) {
                    if (fileInfo.metadata().hashEqualsContents()) {
                        listener.onResponse(null);
                        return;
                    }

                    for (int partIndex = 0; partIndex < fileInfo.numberOfParts(); partIndex++) {
                        final var blobName = fileInfo.partName(partIndex);
                        final var blobInfo = shardContainerContents.blobsByName().get(blobName);
                        if (blobInfo == null) {
                            isRestorable = false;
                            String physicalFileName = fileInfo.physicalName();
                            anomaly("missing blob").snapshotId(snapshotId)
                                .shardDescription(indexDescription, shardContainerContents.shardId())
                                .blobName(blobName, physicalFileName)
                                .part(partIndex, fileInfo.numberOfParts())
                                .fileLength(ByteSizeValue.ofBytes(fileInfo.length()))
                                .partLength(ByteSizeValue.ofBytes(fileInfo.partBytes(partIndex)))
                                .write(listener);
                            return;
                        } else if (blobInfo.length() != fileInfo.partBytes(partIndex)) {
                            isRestorable = false;
                            String physicalFileName = fileInfo.physicalName();
                            ByteSizeValue blobLength = ByteSizeValue.ofBytes(blobInfo.length());
                            anomaly("mismatched blob length").snapshotId(snapshotId)
                                .shardDescription(indexDescription, shardContainerContents.shardId())
                                .blobName(blobName, physicalFileName)
                                .part(partIndex, fileInfo.numberOfParts())
                                .fileLength(ByteSizeValue.ofBytes(fileInfo.length()))
                                .partLength(ByteSizeValue.ofBytes(fileInfo.partBytes(partIndex)))
                                .blobLength(blobLength)
                                .write(listener);
                            return;
                        }
                    }

                    // NB adding a listener whether ?verify_blob_contents is set or not - we want to track the blob sizes either way
                    blobContentsListeners(indexDescription, shardContainerContents, fileInfo).addListener(
                        listener.delegateResponse((l, e) -> {
                            isRestorable = false;
                            String physicalFileName = fileInfo.physicalName();
                            anomaly("corrupt data blob").snapshotId(snapshotId)
                                .shardDescription(indexDescription, shardContainerContents.shardId())
                                .blobName(fileInfo.name(), physicalFileName)
                                .part(-1, fileInfo.numberOfParts())
                                .fileLength(ByteSizeValue.ofBytes(fileInfo.length()))
                                .exception(e)
                                .write(l);
                        })
                    );
                }

                /**
                 * Checks that the shard generation blob has the right content for this shard snapshot.
                 */
                private void verifyShardGenerationConsistency(
                    BlobStoreIndexShardSnapshot blobStoreIndexShardSnapshot,
                    ActionListener<Void> listener
                ) {
                    final var summaryFilesByPhysicalNameBySnapshotName = shardContainerContents.filesByPhysicalNameBySnapshotName();
                    if (summaryFilesByPhysicalNameBySnapshotName == null) {
                        // couldn't read shard gen blob at all - already reported, nothing more to do here
                        listener.onResponse(null);
                        return;
                    }

                    final var shardId = shardContainerContents.shardId();

                    final var summaryFilesByPhysicalName = summaryFilesByPhysicalNameBySnapshotName.get(snapshotId.getName());
                    if (summaryFilesByPhysicalName == null) {
                        anomaly("snapshot not in shard generation").snapshotId(snapshotId)
                            .shardDescription(indexDescription, shardId)
                            .shardGeneration(shardContainerContents.shardGeneration())
                            .write(listener);
                        return;
                    }

                    final var snapshotFiles = getFilesByPhysicalName(blobStoreIndexShardSnapshot.indexFiles());

                    for (final var summaryFile : summaryFilesByPhysicalName.values()) {
                        final var snapshotFile = snapshotFiles.get(summaryFile.physicalName());
                        if (snapshotFile == null) {
                            anomaly("blob in shard generation but not snapshot").snapshotId(snapshotId)
                                .shardDescription(indexDescription, shardId)
                                .shardGeneration(shardContainerContents.shardGeneration())
                                .physicalFileName(summaryFile.physicalName())
                                .write(listener);
                            return;
                        } else if (summaryFile.isSame(snapshotFile) == false) {
                            anomaly("snapshot shard generation mismatch").snapshotId(snapshotId)
                                .shardDescription(indexDescription, shardId)
                                .shardGeneration(shardContainerContents.shardGeneration())
                                .physicalFileName(summaryFile.physicalName())
                                .write(listener);
                            return;
                        }
                    }

                    for (final var snapshotFile : blobStoreIndexShardSnapshot.indexFiles()) {
                        if (summaryFilesByPhysicalName.get(snapshotFile.physicalName()) == null) {
                            anomaly("blob in snapshot but not shard generation").snapshotId(snapshotId)
                                .shardDescription(indexDescription, shardId)
                                .shardGeneration(shardContainerContents.shardGeneration())
                                .physicalFileName(snapshotFile.physicalName())
                                .write(listener);
                            return;
                        }
                    }

                    listener.onResponse(null);
                }
            }
        }

        /**
         * Exposes {@link IndexDescription} per index-metadata-blob (particularly the shard count), caching the value on first read
         * to avoid duplicate work.
         */
        private class IndexDescriptionsDeduplicator {
            private final Map<String, SubscribableListener<IndexDescription>> listenersByBlobId = newConcurrentMap();

            SubscribableListener<IndexDescription> get(SnapshotId snapshotId) {
                final var indexMetaBlobId = repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotId, indexId);
                return listenersByBlobId.computeIfAbsent(
                    indexMetaBlobId,
                    ignored -> SubscribableListener.newForked(
                        indexDescriptionListener -> metadataTaskRunner.run(
                            ActionRunnable.wrap(indexDescriptionListener, l -> load(snapshotId, indexMetaBlobId, l))
                        )
                    )
                );
            }

            private void load(SnapshotId snapshotId, String indexMetaBlobId, ActionListener<IndexDescription> listener) {
                try {
                    listener.onResponse(
                        new IndexDescription(
                            indexId,
                            indexMetaBlobId,
                            blobStoreRepository.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId).getNumberOfShards()
                        )
                    );
                } catch (Exception e) {
                    anomaly("failed to load index metadata").indexDescription(new IndexDescription(indexId, indexMetaBlobId, 0))
                        .exception(e)
                        .write(listener.map(v -> null));
                }
            }
        }

        /**
         * Exposes {@link ShardContainerContents} per shard, caching the value on the first read to avoid duplicate work.
         */
        private class ShardContainerContentsDeduplicator {
            private final Map<Integer, SubscribableListener<ShardContainerContents>> listenersByShardId = newConcurrentMap();

            SubscribableListener<ShardContainerContents> get(int shardId) {
                return listenersByShardId.computeIfAbsent(
                    shardId,
                    ignored -> SubscribableListener.newForked(
                        shardContainerContentsListener -> metadataTaskRunner.run(
                            ActionRunnable.wrap(shardContainerContentsListener, l -> load(shardId, l))
                        )
                    )
                );
            }

            private void load(int shardId, ActionListener<ShardContainerContents> listener) {
                final var indexDescription = new IndexDescription(indexId, null, 0);

                final Map<String, BlobMetadata> blobsByName;
                try {
                    blobsByName = blobStoreRepository.shardContainer(indexId, shardId).listBlobs(OperationPurpose.REPOSITORY_ANALYSIS);
                } catch (Exception e) {
                    anomaly("failed to list shard container contents").shardDescription(new IndexDescription(indexId, null, 0), shardId)
                        .exception(e)
                        .write(listener.map(v -> null));
                    return;
                }

                final var shardGen = repositoryData.shardGenerations().getShardGen(indexId, shardId);
                if (shardGen == null) {
                    anomaly("shard generation not defined").shardDescription(indexDescription, shardId)
                        .write(
                            listener.map(
                                // NB we don't need the shard gen to do most of the rest of the verification, so we set it to null and
                                // carry on:
                                v -> new ShardContainerContents(shardId, blobsByName, null, null, ConcurrentCollections.newConcurrentMap())
                            )
                        );
                    return;
                }

                SubscribableListener
                    // try and load the shard gen blob
                    .<BlobStoreIndexShardSnapshots>newForked(l -> {
                        try {
                            l.onResponse(blobStoreRepository.getBlobStoreIndexShardSnapshots(indexId, shardId, shardGen));
                        } catch (Exception e) {
                            // failing here is not fatal to snapshot restores, only to creating/deleting snapshots, so we can return null
                            // and carry on with the analysis
                            anomaly("failed to load shard generation").shardDescription(indexDescription, shardId)
                                .shardGeneration(shardGen)
                                .exception(e)
                                .write(l.map(v -> null));
                        }
                    })
                    .andThenApply(
                        blobStoreIndexShardSnapshots -> new ShardContainerContents(
                            shardId,
                            blobsByName,
                            shardGen,
                            getFilesByPhysicalNameBySnapshotName(blobStoreIndexShardSnapshots),
                            ConcurrentCollections.newConcurrentMap()
                        )
                    )
                    .addListener(listener);
            }

            private static Map<String, Map<String, BlobStoreIndexShardSnapshot.FileInfo>> getFilesByPhysicalNameBySnapshotName(
                BlobStoreIndexShardSnapshots blobStoreIndexShardSnapshots
            ) {
                if (blobStoreIndexShardSnapshots == null) {
                    return null;
                }

                final Map<String, Map<String, BlobStoreIndexShardSnapshot.FileInfo>> filesByPhysicalNameBySnapshotName = Maps
                    .newHashMapWithExpectedSize(blobStoreIndexShardSnapshots.snapshots().size());
                for (final var snapshotFiles : blobStoreIndexShardSnapshots.snapshots()) {
                    filesByPhysicalNameBySnapshotName.put(snapshotFiles.snapshot(), getFilesByPhysicalName(snapshotFiles.indexFiles()));
                }
                return filesByPhysicalNameBySnapshotName;
            }
        }

        private SubscribableListener<Void> blobContentsListeners(
            IndexDescription indexDescription,
            ShardContainerContents shardContainerContents,
            BlobStoreIndexShardSnapshot.FileInfo fileInfo
        ) {
            return shardContainerContents.blobContentsListeners().computeIfAbsent(fileInfo.name(), ignored -> {
                if (requestParams.verifyBlobContents()) {
                    return SubscribableListener.newForked(listener -> snapshotTaskRunner.run(ActionRunnable.run(listener, () -> {
                        try (var slicedStream = new SlicedInputStream(fileInfo.numberOfParts()) {
                            @Override
                            protected InputStream openSlice(int slice) throws IOException {
                                return blobStoreRepository.shardContainer(indexDescription.indexId(), shardContainerContents.shardId())
                                    .readBlob(OperationPurpose.REPOSITORY_ANALYSIS, fileInfo.partName(slice));
                            }
                        };
                            var rateLimitedStream = new RateLimitingInputStream(slicedStream, () -> rateLimiter, throttledNanos::addAndGet);
                            var indexInput = new IndexInputWrapper(rateLimitedStream, fileInfo.length())
                        ) {
                            CodecUtil.checksumEntireFile(indexInput);
                        }
                    })));
                } else {
                    blobBytesVerified.addAndGet(fileInfo.length());
                    return SubscribableListener.newSucceeded(null);
                }
            });
        }
    }

    private static String getShardSnapshotDescription(SnapshotId snapshotId, String index, int shardId) {
        return snapshotId.getUUID() + "/" + index + "/" + shardId;
    }

    private static Map<String, BlobStoreIndexShardSnapshot.FileInfo> getFilesByPhysicalName(
        List<BlobStoreIndexShardSnapshot.FileInfo> fileInfos
    ) {
        final Map<String, BlobStoreIndexShardSnapshot.FileInfo> filesByPhysicalName = Maps.newHashMapWithExpectedSize(fileInfos.size());
        for (final var fileInfo : fileInfos) {
            filesByPhysicalName.put(fileInfo.physicalName(), fileInfo);
        }
        return filesByPhysicalName;
    }

    private static <T> void runThrottled(
        Iterator<T> iterator,
        BiConsumer<Releasable, T> itemConsumer,
        int maxConcurrency,
        AtomicLong progressCounter,
        Releasable onCompletion
    ) {
        ThrottledIterator.run(
            iterator,
            (ref, item) -> itemConsumer.accept(Releasables.wrap(progressCounter::incrementAndGet, ref), item),
            maxConcurrency,
            onCompletion::close
        );
    }

    private RepositoryVerifyIntegrityResponseChunk.Builder anomaly(String anomaly) {
        return new RepositoryVerifyIntegrityResponseChunk.Builder(
            responseChunkWriter,
            RepositoryVerifyIntegrityResponseChunk.Type.ANOMALY,
            currentTimeMillisSupplier.getAsLong()
        ).anomaly(anomaly);
    }

    public long getSnapshotCount() {
        return snapshotCount;
    }

    public long getIndexCount() {
        return indexCount;
    }

    public long getIndexSnapshotCount() {
        return indexSnapshotCount;
    }

    private class IndexInputWrapper extends IndexInput {
        private final InputStream inputStream;
        private final long length;
        long filePointer = 0L;

        IndexInputWrapper(InputStream inputStream, long length) {
            super("");
            this.inputStream = inputStream;
            this.length = length;
        }

        @Override
        public byte readByte() throws IOException {
            if (isCancelledSupplier.getAsBoolean()) {
                throw new TaskCancelledException("task cancelled");
            }
            final var read = inputStream.read();
            if (read == -1) {
                throw new EOFException();
            }
            filePointer += 1;
            blobBytesVerified.incrementAndGet();
            return (byte) read;
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            while (len > 0) {
                if (isCancelledSupplier.getAsBoolean()) {
                    throw new TaskCancelledException("task cancelled");
                }
                final var read = inputStream.read(b, offset, len);
                if (read == -1) {
                    throw new EOFException();
                }
                filePointer += read;
                blobBytesVerified.addAndGet(read);
                len -= read;
                offset += read;
            }
        }

        @Override
        public void close() {}

        @Override
        public long getFilePointer() {
            return filePointer;
        }

        @Override
        public void seek(long pos) {
            if (filePointer != pos) {
                assert false : "cannot seek";
                throw new UnsupportedOperationException("seek");
            }
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) {
            assert false;
            throw new UnsupportedOperationException("slice");
        }
    }

    private static class CancellableRunner {
        private final ThrottledTaskRunner delegate;
        private final CancellableThreads cancellableThreads;

        CancellableRunner(ThrottledTaskRunner delegate, CancellableThreads cancellableThreads) {
            this.delegate = delegate;
            this.cancellableThreads = cancellableThreads;
        }

        void run(AbstractRunnable runnable) {
            delegate.enqueueTask(new ActionListener<>() {
                @Override
                public void onResponse(Releasable releasable) {
                    try (releasable) {
                        if (cancellableThreads.isCancelled()) {
                            runnable.onFailure(new TaskCancelledException("task cancelled"));
                        } else {
                            try {
                                cancellableThreads.execute(runnable::run);
                            } catch (RuntimeException e) {
                                runnable.onFailure(e);
                            }
                        }
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    runnable.onFailure(e);
                }
            });
        }
    }
}
