/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.repositories.integrity.VerifyRepositoryIntegrityAction;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.core.Strings.format;

class MetadataVerifier implements Releasable {
    private static final Logger logger = LogManager.getLogger(MetadataVerifier.class);

    private final BlobStoreRepository blobStoreRepository;
    private final ActionListener<List<RepositoryVerificationException>> finalListener;
    private final RefCounted finalRefs = AbstractRefCounted.of(this::onCompletion);
    private final String repositoryName;
    private final VerifyRepositoryIntegrityAction.Request verifyRequest;
    private final RepositoryData repositoryData;
    private final BooleanSupplier isCancelledSupplier;
    private final Queue<RepositoryVerificationException> failures = new ConcurrentLinkedQueue<>();
    private final AtomicLong failureCount = new AtomicLong();
    private final Map<String, Set<SnapshotId>> snapshotsByIndex;
    private final Semaphore threadPoolPermits;
    private final Queue<AbstractRunnable> executorQueue = new ConcurrentLinkedQueue<>();
    private final ProgressLogger snapshotProgressLogger;
    private final ProgressLogger indexProgressLogger;
    private final ProgressLogger indexSnapshotProgressLogger;
    private final Set<String> requestedIndices;

    MetadataVerifier(
        BlobStoreRepository blobStoreRepository,
        VerifyRepositoryIntegrityAction.Request verifyRequest,
        RepositoryData repositoryData,
        BooleanSupplier isCancelledSupplier,
        ActionListener<List<RepositoryVerificationException>> finalListener
    ) {
        this.blobStoreRepository = blobStoreRepository;
        this.repositoryName = blobStoreRepository.metadata.name();
        this.verifyRequest = verifyRequest;
        this.repositoryData = repositoryData;
        this.isCancelledSupplier = isCancelledSupplier;
        this.finalListener = finalListener;
        this.snapshotsByIndex = this.repositoryData.getIndices()
            .values()
            .stream()
            .collect(Collectors.toMap(IndexId::getName, indexId -> Set.copyOf(this.repositoryData.getSnapshots(indexId))));

        this.threadPoolPermits = new Semaphore(Math.max(1, verifyRequest.getThreadpoolConcurrency()));
        this.snapshotProgressLogger = new ProgressLogger("snapshots", repositoryData.getSnapshotIds().size(), 100);
        this.indexProgressLogger = new ProgressLogger("indices", repositoryData.getIndices().size(), 20);
        this.indexSnapshotProgressLogger = new ProgressLogger("index snapshots", repositoryData.getIndexSnapshotCount(), 1000);

        this.requestedIndices = Set.of(verifyRequest.getIndices());
    }

    @Override
    public void close() {
        finalRefs.decRef();
    }

    private void addFailure(String format, Object... args) {
        final var failureNumber = failureCount.incrementAndGet();
        final var failure = format(format, args);
        logger.debug("[{}] found metadata verification failure [{}]: {}", repositoryName, failureNumber, failure);
        if (failureNumber <= verifyRequest.getMaxFailures()) {
            failures.add(new RepositoryVerificationException(repositoryName, failure));
        }
    }

    private void addFailure(Exception exception) {
        if (isCancelledSupplier.getAsBoolean() && exception instanceof TaskCancelledException) {
            return;
        }
        final var failureNumber = failureCount.incrementAndGet();
        logger.debug(() -> format("[%s] exception [%d] during metadata verification", repositoryName, failureNumber), exception);
        if (failureNumber <= verifyRequest.getMaxFailures()) {
            failures.add(
                exception instanceof RepositoryVerificationException rve
                    ? rve
                    : new RepositoryVerificationException(repositoryName, "exception during metadata verification", exception)
            );
        }
    }

    public void run() {
        logger.info(
            "[{}] verifying metadata integrity for index generation [{}]: "
                + "repo UUID [{}], cluster UUID [{}], snapshots [{}], indices [{}], index snapshots [{}]",
            repositoryName,
            repositoryData.getGenId(),
            repositoryData.getUuid(),
            repositoryData.getClusterUUID(),
            snapshotProgressLogger.getExpectedMax(),
            indexProgressLogger.getExpectedMax(),
            indexSnapshotProgressLogger.getExpectedMax()
        );

        if (requestedIndices.isEmpty()) {
            verifySnapshots();
        } else {
            verifyIndices();
        }
    }

    private void verifySnapshots() {
        runThrottled(
            makeVoidListener(finalRefs, this::verifyIndices),
            repositoryData.getSnapshotIds().iterator(),
            this::verifySnapshot,
            verifyRequest.getSnapshotVerificationConcurrency(),
            snapshotProgressLogger
        );
    }

    private void verifySnapshot(RefCounted snapshotRefs, SnapshotId snapshotId) {
        if (verifyRequest.permitMissingSnapshotDetails() == false && repositoryData.hasMissingDetails(snapshotId)) {
            addFailure("snapshot [%s] has missing snapshot details", snapshotId);
        }

        if (isCancelledSupplier.getAsBoolean()) {
            // getSnapshotInfo does its own forking so we must check for cancellation here
            return;
        }

        blobStoreRepository.getSnapshotInfo(snapshotId, makeListener(snapshotRefs, snapshotInfo -> {
            if (snapshotInfo.snapshotId().equals(snapshotId) == false) {
                addFailure("snapshot [%s] has unexpected ID in info blob: [%s]", snapshotId, snapshotInfo.snapshotId());
            }
            for (final var index : snapshotInfo.indices()) {
                if (snapshotsByIndex.get(index).contains(snapshotId) == false) {
                    addFailure("snapshot [%s] contains unexpected index [%s]", snapshotId, index);
                }
            }
        }));

        forkSupply(snapshotRefs, () -> getSnapshotGlobalMetadata(snapshotId), metadata -> {
            if (metadata != null && metadata.indices().isEmpty() == false) {
                addFailure("snapshot [%s] contains unexpected index metadata within global metadata", snapshotId);
            }
        });
    }

    private Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
        try {
            return blobStoreRepository.getSnapshotGlobalMetadata(snapshotId);
        } catch (Exception e) {
            addFailure(
                new RepositoryVerificationException(
                    repositoryName,
                    format("failed to get snapshot global metadata for [%s]", snapshotId),
                    e
                )
            );
            return null;
        }
    }

    private void verifyIndices() {
        final var indicesMap = repositoryData.getIndices();

        for (final var indicesEntry : indicesMap.entrySet()) {
            final var name = indicesEntry.getKey();
            final var indexId = indicesEntry.getValue();
            if (name.equals(indexId.getName()) == false) {
                addFailure("index name [%s] has mismatched name in %s", name, indexId);
            }
        }

        runThrottled(
            makeVoidListener(finalRefs, () -> {}),
            indicesMap.values().iterator(),
            (refCounted, indexId) -> new IndexVerifier(refCounted, indexId).run(),
            verifyRequest.getIndexVerificationConcurrency(),
            indexProgressLogger
        );
    }

    private record ShardContainerContents(
        Map<String, BlobMetadata> blobsByName,
        BlobStoreIndexShardSnapshots blobStoreIndexShardSnapshots
    ) {}

    private class IndexVerifier {
        private final RefCounted indexRefs;
        private final IndexId indexId;
        private final Set<SnapshotId> expectedSnapshots;
        private final Map<Integer, ListenableActionFuture<ShardContainerContents>> shardContainerContentsListener = newConcurrentMap();
        private final Map<String, ListenableActionFuture<Integer>> shardCountListenersByBlobId = newConcurrentMap();
        private final AtomicInteger totalSnapshotCounter = new AtomicInteger();
        private final AtomicInteger restorableSnapshotCounter = new AtomicInteger();

        IndexVerifier(RefCounted indexRefs, IndexId indexId) {
            this.indexRefs = indexRefs;
            this.indexId = indexId;
            this.expectedSnapshots = snapshotsByIndex.get(indexId.getName());
        }

        void run() {
            if (requestedIndices.isEmpty() == false && requestedIndices.contains(indexId.getName()) == false) {
                return;
            }

            runThrottled(
                makeVoidListener(indexRefs, () -> logRestorability(totalSnapshotCounter.get(), restorableSnapshotCounter.get())),
                repositoryData.getSnapshots(indexId).iterator(),
                this::verifyIndexSnapshot,
                verifyRequest.getIndexSnapshotVerificationConcurrency(),
                indexSnapshotProgressLogger
            );
        }

        private void logRestorability(int totalSnapshotCount, int restorableSnapshotCount) {
            if (isCancelledSupplier.getAsBoolean() == false) {
                if (totalSnapshotCount == restorableSnapshotCount) {
                    logger.debug("[{}] index {} is fully restorable from [{}] snapshots", repositoryName, indexId, totalSnapshotCount);
                } else {
                    logger.debug(
                        "[{}] index {} is not fully restorable: of [{}] snapshots, [{}] are restorable and [{}] are not",
                        repositoryName,
                        indexId,
                        totalSnapshotCount,
                        restorableSnapshotCount,
                        totalSnapshotCount - restorableSnapshotCount
                    );
                }
            }
        }

        private void verifyIndexSnapshot(RefCounted indexSnapshotRefs, SnapshotId snapshotId) {
            totalSnapshotCounter.incrementAndGet();

            if (expectedSnapshots.contains(snapshotId) == false) {
                addFailure("index %s has mismatched snapshot [%s]", indexId, snapshotId);
            }

            final var indexMetaBlobId = repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotId, indexId);
            shardCountListenersByBlobId.computeIfAbsent(indexMetaBlobId, ignored -> {
                final var shardCountFuture = new ListenableActionFuture<Integer>();
                forkSupply(() -> {
                    final var shardCount = getNumberOfShards(indexMetaBlobId, snapshotId);
                    for (int i = 0; i < shardCount; i++) {
                        shardContainerContentsListener.computeIfAbsent(i, shardId -> {
                            final var shardContainerContentsFuture = new ListenableActionFuture<ShardContainerContents>();
                            forkSupply(
                                () -> new ShardContainerContents(
                                    blobStoreRepository.shardContainer(indexId, shardId).listBlobs(),
                                    getBlobStoreIndexShardSnapshots(shardId)
                                ),
                                shardContainerContentsFuture
                            );
                            return shardContainerContentsFuture;
                        });
                    }
                    return shardCount;
                }, shardCountFuture);
                return shardCountFuture;
            }).addListener(makeListener(indexSnapshotRefs, shardCount -> {
                final var restorableShardCount = new AtomicInteger();
                try (var shardSnapshotsRefs = wrap(makeVoidListener(indexSnapshotRefs, () -> {
                    if (shardCount > 0 && shardCount == restorableShardCount.get()) {
                        restorableSnapshotCounter.incrementAndGet();
                    }
                }))) {
                    for (int i = 0; i < shardCount; i++) {
                        final var shardId = i;
                        shardContainerContentsListener.get(i)
                            .addListener(
                                makeListener(
                                    shardSnapshotsRefs,
                                    shardContainerContents -> forkSupply(
                                        shardSnapshotsRefs,
                                        () -> getBlobStoreIndexShardSnapshot(snapshotId, shardId),
                                        shardSnapshot -> verifyShardSnapshot(
                                            snapshotId,
                                            shardId,
                                            shardContainerContents,
                                            shardSnapshot,
                                            restorableShardCount::incrementAndGet
                                        )
                                    )
                                )
                            );
                    }
                }
            }));
        }

        private BlobStoreIndexShardSnapshot getBlobStoreIndexShardSnapshot(SnapshotId snapshotId, int shardId) {
            try {
                return blobStoreRepository.loadShardSnapshot(blobStoreRepository.shardContainer(indexId, shardId), snapshotId);
            } catch (Exception e) {
                addFailure(
                    new RepositoryVerificationException(
                        repositoryName,
                        format("failed to load shard %s[%d] snapshot for [%s]", indexId, shardId, snapshotId),
                        e
                    )
                );
                return null;
            }
        }

        private List<SnapshotId> getSnapshotsWithIndexMetadataBlob(String indexMetaBlobId) {
            final var indexMetaDataGenerations = repositoryData.indexMetaDataGenerations();
            return repositoryData.getSnapshotIds()
                .stream()
                .filter(s -> indexMetaBlobId.equals(indexMetaDataGenerations.indexMetaBlobId(s, indexId)))
                .toList();
        }

        private int getNumberOfShards(String indexMetaBlobId, SnapshotId snapshotId) {
            try {
                return blobStoreRepository.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId).getNumberOfShards();
            } catch (Exception e) {
                addFailure(
                    new RepositoryVerificationException(
                        repositoryName,
                        format(
                            "failed to load index %s metadata from blob [%s] for %s",
                            indexId,
                            indexMetaBlobId,
                            getSnapshotsWithIndexMetadataBlob(indexMetaBlobId)
                        ),
                        e
                    )
                );
                return 0;
            }
        }

        private BlobStoreIndexShardSnapshots getBlobStoreIndexShardSnapshots(int shardId) {
            final var shardGen = repositoryData.shardGenerations().getShardGen(indexId, shardId);
            if (shardGen == null) {
                addFailure("unknown shard generation for %s[%d]", indexId, shardId);
                return null;
            }
            try {
                return blobStoreRepository.getBlobStoreIndexShardSnapshots(indexId, shardId, shardGen);
            } catch (Exception e) {
                addFailure(e);
                return null;
            }
        }

        private void verifyShardSnapshot(
            SnapshotId snapshotId,
            int shardId,
            ShardContainerContents shardContainerContents,
            BlobStoreIndexShardSnapshot shardSnapshot,
            Runnable runIfRestorable
        ) {
            if (shardSnapshot == null) {
                return;
            }

            if (shardSnapshot.snapshot().equals(snapshotId.getName()) == false) {
                addFailure(
                    "snapshot [%s] for shard %s[%d] has mismatched name [%s]",
                    snapshotId,
                    indexId,
                    shardId,
                    shardSnapshot.snapshot()
                );
            }

            var restorable = true;
            for (final var fileInfo : shardSnapshot.indexFiles()) {
                restorable &= verifyFileInfo(snapshotId.toString(), shardId, shardContainerContents.blobsByName(), fileInfo);
            }
            if (restorable) {
                runIfRestorable.run();
            }

            final var blobStoreIndexShardSnapshots = shardContainerContents.blobStoreIndexShardSnapshots();
            if (blobStoreIndexShardSnapshots != null) {
                boolean foundSnapshot = false;
                for (SnapshotFiles summary : blobStoreIndexShardSnapshots.snapshots()) {
                    if (summary.snapshot().equals(snapshotId.getName())) {
                        foundSnapshot = true;
                        verifyConsistentShardFiles(snapshotId, shardId, shardSnapshot, summary);
                        break;
                    }
                }

                if (foundSnapshot == false) {
                    addFailure("snapshot [%s] for shard %s[%d] has no entry in the shard-level summary", snapshotId, indexId, shardId);
                }
            }
        }

        private void verifyConsistentShardFiles(
            SnapshotId snapshotId,
            int shardId,
            BlobStoreIndexShardSnapshot shardSnapshot,
            SnapshotFiles summary
        ) {
            final var snapshotFiles = shardSnapshot.indexFiles()
                .stream()
                .collect(Collectors.toMap(BlobStoreIndexShardSnapshot.FileInfo::physicalName, Function.identity()));

            for (final var summaryFile : summary.indexFiles()) {
                final var snapshotFile = snapshotFiles.get(summaryFile.physicalName());
                if (snapshotFile == null) {
                    addFailure(
                        "snapshot [%s] for shard %s[%d] has no entry for file [%s] found in summary",
                        snapshotId,
                        indexId,
                        shardId,
                        summaryFile.physicalName()
                    );
                } else if (summaryFile.isSame(snapshotFile) == false) {
                    addFailure(
                        "snapshot [%s] for shard %s[%d] has a mismatched entry for file [%s]",
                        snapshotId,
                        indexId,
                        shardId,
                        summaryFile.physicalName()
                    );
                }
            }

            final var summaryFiles = summary.indexFiles()
                .stream()
                .collect(Collectors.toMap(BlobStoreIndexShardSnapshot.FileInfo::physicalName, Function.identity()));
            for (final var snapshotFile : shardSnapshot.indexFiles()) {
                if (summaryFiles.get(snapshotFile.physicalName()) == null) {
                    addFailure(
                        "snapshot [%s] for shard %s[%d] has no entry in the shard-level summary for file [%s]",
                        snapshotId,
                        indexId,
                        shardId,
                        snapshotFile.physicalName()
                    );
                }
            }
        }

        private static String formatExact(ByteSizeValue byteSizeValue) {
            if (byteSizeValue.getBytes() >= ByteSizeUnit.KB.toBytes(1)) {
                return format("%s/%dB", byteSizeValue.toString(), byteSizeValue.getBytes());
            } else {
                return byteSizeValue.toString();
            }
        }

        private boolean verifyFileInfo(
            String snapshot,
            int shardId,
            Map<String, BlobMetadata> shardBlobs,
            BlobStoreIndexShardSnapshot.FileInfo fileInfo
        ) {
            final var fileLength = ByteSizeValue.ofBytes(fileInfo.length());
            if (fileInfo.metadata().hashEqualsContents()) {
                final var actualLength = ByteSizeValue.ofBytes(fileInfo.metadata().hash().length);
                if (fileLength.getBytes() != actualLength.getBytes()) {
                    addFailure(
                        "snapshot [%s] for shard %s[%d] has virtual blob [%s] for [%s] with length [%s] instead of [%s]",
                        snapshot,
                        indexId,
                        shardId,
                        fileInfo.name(),
                        fileInfo.physicalName(),
                        formatExact(actualLength),
                        formatExact(fileLength)
                    );
                    return false;
                }
            } else {
                for (int part = 0; part < fileInfo.numberOfParts(); part++) {
                    final var blobName = fileInfo.partName(part);
                    final var blobInfo = shardBlobs.get(blobName);
                    final var partLength = ByteSizeValue.ofBytes(fileInfo.partBytes(part));
                    if (blobInfo == null) {
                        addFailure(
                            "snapshot [%s] for shard %s[%d] has missing blob [%s] for [%s] part [%d/%d]; "
                                + "file length [%s], part length [%s]",
                            snapshot,
                            indexId,
                            shardId,
                            blobName,
                            fileInfo.physicalName(),
                            part,
                            fileInfo.numberOfParts(),
                            formatExact(fileLength),
                            formatExact(partLength)
                        );
                        return false;
                    } else if (blobInfo.length() != partLength.getBytes()) {
                        addFailure(
                            "snapshot [%s] for shard %s[%d] has blob [%s] for [%s] part [%d/%d] with length [%s] instead of [%s]; "
                                + "file length [%s]",
                            snapshot,
                            indexId,
                            shardId,
                            blobName,
                            fileInfo.physicalName(),
                            part,
                            fileInfo.numberOfParts(),
                            formatExact(ByteSizeValue.ofBytes(blobInfo.length())),
                            formatExact(partLength),
                            formatExact(fileLength)
                        );
                        return false;
                    }
                }
            }
            return true;
        }
    }

    private <T> ActionListener<T> makeListener(RefCounted refCounted, CheckedConsumer<T, Exception> consumer) {
        refCounted.incRef();
        return ActionListener.runAfter(ActionListener.wrap(consumer, this::addFailure), refCounted::decRef);
    }

    private ActionListener<Void> makeVoidListener(RefCounted refCounted, CheckedRunnable<Exception> runnable) {
        return makeListener(refCounted, ignored -> runnable.run());
    }

    private <T> void forkSupply(CheckedSupplier<T, Exception> supplier, ActionListener<T> listener) {
        fork(ActionRunnable.supply(listener, supplier));
    }

    private void fork(AbstractRunnable runnable) {
        executorQueue.add(runnable);
        tryProcessQueue();
    }

    private void tryProcessQueue() {
        while (threadPoolPermits.tryAcquire()) {
            final var runnable = executorQueue.poll();
            if (runnable == null) {
                threadPoolPermits.release();
                return;
            }

            if (isCancelledSupplier.getAsBoolean()) {
                try {
                    runnable.onFailure(new TaskCancelledException("task cancelled"));
                    continue;
                } finally {
                    threadPoolPermits.release();
                }
            }

            blobStoreRepository.threadPool().executor(ThreadPool.Names.SNAPSHOT_META).execute(new AbstractRunnable() {
                @Override
                public void onRejection(Exception e) {
                    try {
                        runnable.onRejection(e);
                    } finally {
                        threadPoolPermits.release();
                        // no need to call tryProcessQueue() again here, we're still running it
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        runnable.onFailure(e);
                    } finally {
                        onCompletion();
                    }
                }

                @Override
                protected void doRun() {
                    runnable.run();
                    onCompletion();
                }

                @Override
                public String toString() {
                    return runnable.toString();
                }

                private void onCompletion() {
                    threadPoolPermits.release();
                    tryProcessQueue();
                }
            });
        }
    }

    private <T> void forkSupply(RefCounted refCounted, CheckedSupplier<T, Exception> supplier, CheckedConsumer<T, Exception> consumer) {
        forkSupply(supplier, makeListener(refCounted, consumer));
    }

    private void onCompletion() {
        final var finalFailureCount = failureCount.get();
        if (finalFailureCount > verifyRequest.getMaxFailures()) {
            failures.add(
                new RepositoryVerificationException(
                    repositoryName,
                    format(
                        "found %d verification failures in total, %d suppressed",
                        finalFailureCount,
                        finalFailureCount - verifyRequest.getMaxFailures()
                    )
                )
            );
        }

        if (isCancelledSupplier.getAsBoolean()) {
            failures.add(new RepositoryVerificationException(repositoryName, "verification task cancelled before completion"));
        }

        finalListener.onResponse(failures.stream().toList());
    }

    private interface RefCountedListenerWrapper extends Releasable, RefCounted {}

    private static RefCountedListenerWrapper wrap(ActionListener<Void> listener) {
        final var refCounted = AbstractRefCounted.of(() -> listener.onResponse(null));
        return new RefCountedListenerWrapper() {
            @Override
            public void incRef() {
                refCounted.incRef();
            }

            @Override
            public boolean tryIncRef() {
                return refCounted.tryIncRef();
            }

            @Override
            public boolean decRef() {
                return refCounted.decRef();
            }

            @Override
            public boolean hasReferences() {
                return refCounted.hasReferences();
            }

            @Override
            public void close() {
                decRef();
            }
        };
    }

    private static <T> void runThrottled(
        ActionListener<Void> completionListener,
        Iterator<T> iterator,
        BiConsumer<RefCounted, T> consumer,
        int maxConcurrency,
        ProgressLogger progressLogger
    ) {
        try (var throttledIterator = new ThrottledIterator<>(completionListener, iterator, consumer, maxConcurrency, progressLogger)) {
            throttledIterator.run();
        }
    }

    private static class ThrottledIterator<T> implements Releasable {
        private final RefCountedListenerWrapper throttleRefs;
        private final Iterator<T> iterator;
        private final BiConsumer<RefCounted, T> consumer;
        private final Semaphore permits;
        private final ProgressLogger progressLogger;

        ThrottledIterator(
            ActionListener<Void> completionListener,
            Iterator<T> iterator,
            BiConsumer<RefCounted, T> consumer,
            int maxConcurrency,
            ProgressLogger progressLogger
        ) {
            this.throttleRefs = wrap(completionListener);
            this.iterator = iterator;
            this.consumer = consumer;
            this.permits = new Semaphore(maxConcurrency);
            this.progressLogger = progressLogger;
        }

        void run() {
            while (permits.tryAcquire()) {
                final T item;
                synchronized (iterator) {
                    if (iterator.hasNext()) {
                        item = iterator.next();
                    } else {
                        permits.release();
                        return;
                    }
                }
                try (var itemRefsWrapper = new RefCountedWrapper()) {
                    consumer.accept(itemRefsWrapper.unwrap(), item);
                }
            }
        }

        @Override
        public void close() {
            throttleRefs.close();
        }

        // Wraps a RefCounted with protection against calling back into run() from within safeDecRef()
        private class RefCountedWrapper implements Releasable {
            private boolean isRecursive;
            private final RefCounted itemRefs = AbstractRefCounted.of(this::onItemCompletion);

            RefCountedWrapper() {
                throttleRefs.incRef();
            }

            RefCounted unwrap() {
                return itemRefs;
            }

            private synchronized boolean isRecursive() {
                return isRecursive;
            }

            private void onItemCompletion() {
                progressLogger.maybeLogProgress();
                permits.release();
                try {
                    if (isRecursive() == false) {
                        run();
                    }
                } finally {
                    throttleRefs.decRef();
                }
            }

            @Override
            public synchronized void close() {
                // if the decRef() below releases the last ref and calls onItemCompletion(), no other thread blocks on us; conversely
                // if decRef() doesn't release the last ref then we release this mutex immediately so the closing thread can proceed
                isRecursive = true;
                itemRefs.decRef();
                isRecursive = false;
            }
        }
    }

    private class ProgressLogger {
        private final String type;
        private final long expectedMax;
        private final long logFrequency;
        private final AtomicLong currentCount = new AtomicLong();

        ProgressLogger(String type, long expectedMax, long logFrequency) {
            this.type = type;
            this.expectedMax = expectedMax;
            this.logFrequency = logFrequency;
        }

        long getExpectedMax() {
            return expectedMax;
        }

        void maybeLogProgress() {
            final var count = currentCount.incrementAndGet();
            if (count == expectedMax || count % logFrequency == 0 && isCancelledSupplier.getAsBoolean() == false) {
                logger.info("[{}] processed [{}] of [{}] {}", repositoryName, count, expectedMax, type);
            }
        }
    }
}
