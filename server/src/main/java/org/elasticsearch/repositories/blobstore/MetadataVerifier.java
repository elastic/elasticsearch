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
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
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
    }

    @Override
    public void close() {
        finalRefs.decRef();
    }

    private void addFailure(String format, Object... args) {
        if (failureCount.incrementAndGet() <= verifyRequest.getMaxFailures()) {
            final var failure = format(format, args);
            logger.debug("[{}] found metadata verification failure: {}", repositoryName, failure);
            failures.add(new RepositoryVerificationException(repositoryName, failure));
        }
    }

    private void addFailure(Exception exception) {
        if (isCancelledSupplier.getAsBoolean() && exception instanceof TaskCancelledException) {
            return;
        }
        if (failureCount.incrementAndGet() <= verifyRequest.getMaxFailures()) {
            logger.debug(() -> format("[%s] exception during metadata verification: {}", repositoryName), exception);
            failures.add(
                exception instanceof RepositoryVerificationException rve
                    ? rve
                    : new RepositoryVerificationException(repositoryName, "exception during metadata verification", exception)
            );
        }
    }

    public void run() {
        logger.info(
            "[{}] verifying metadata integrity for index generation [{}]: repo UUID [{}], cluster UUID [{}]",
            repositoryName,
            repositoryData.getGenId(),
            repositoryData.getUuid(),
            repositoryData.getClusterUUID()
        );

        verifySnapshots();
    }

    private void verifySnapshots() {
        runThrottled(
            makeVoidListener(finalRefs, this::verifyIndices),
            repositoryData.getSnapshotIds().iterator(),
            this::verifySnapshot,
            verifyRequest.getSnapshotVerificationConcurrency()
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

        forkSupply(snapshotRefs, () -> blobStoreRepository.getSnapshotGlobalMetadata(snapshotId), metadata -> {
            if (metadata.indices().isEmpty() == false) {
                addFailure("snapshot [%s] contains unexpected index metadata within global metadata", snapshotId);
            }
        });
    }

    private void verifyIndices() {
        final var indicesMap = repositoryData.getIndices();

        for (final var indicesEntry : indicesMap.entrySet()) {
            final var name = indicesEntry.getKey();
            final var indexId = indicesEntry.getValue();
            if (name.equals(indexId.getName()) == false) {
                addFailure("index name [%s] has mismatched name in [%s]", name, indexId);
            }
        }

        runThrottled(
            makeVoidListener(finalRefs, () -> {}),
            indicesMap.values().iterator(),
            (refCounted, indexId) -> new IndexVerifier(refCounted, indexId).run(),
            verifyRequest.getIndexVerificationConcurrency()
        );
    }

    private class IndexVerifier {
        private final RefCounted indexRefs;
        private final IndexId indexId;
        private final Set<SnapshotId> expectedSnapshots;
        private final Map<Integer, ListenableActionFuture<Map<String, BlobMetadata>>> shardBlobsListenersByShard = newConcurrentMap();
        private final Map<String, ListenableActionFuture<Integer>> shardCountListenersByBlobId = new HashMap<>();

        IndexVerifier(RefCounted indexRefs, IndexId indexId) {
            this.indexRefs = indexRefs;
            this.indexId = indexId;
            this.expectedSnapshots = snapshotsByIndex.get(this.indexId.getName());
        }

        void run() {

            // TODO consider distributing the workload, giving each node a subset of indices to process

            final var indexSnapshots = repositoryData.getSnapshots(indexId);

            try (var indexMetadataChecksRef = wrap(makeVoidListener(indexRefs, this::onIndexMetadataChecksComplete))) {
                runThrottled(
                    makeVoidListener(indexMetadataChecksRef, () -> {}),
                    indexSnapshots.iterator(),
                    this::verifyIndexSnapshot,
                    verifyRequest.getIndexSnapshotVerificationConcurrency()
                );
            }
        }

        private void verifyIndexSnapshot(RefCounted indexSnapshotRefs, SnapshotId snapshotId) {
            if (expectedSnapshots.contains(snapshotId) == false) {
                addFailure("index [%s] has mismatched snapshot [%s]", indexId, snapshotId);
            }

            final var indexMetaBlobId = repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotId, indexId);
            shardCountListenersByBlobId.computeIfAbsent(indexMetaBlobId, ignored -> {
                final var shardCountFuture = new ListenableActionFuture<Integer>();
                forkSupply(() -> {
                    final var shardCount = blobStoreRepository.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId)
                        .getNumberOfShards();
                    for (int i = 0; i < shardCount; i++) {
                        shardBlobsListenersByShard.computeIfAbsent(i, shardId -> {
                            final var shardBlobsFuture = new ListenableActionFuture<Map<String, BlobMetadata>>();
                            forkSupply(() -> blobStoreRepository.shardContainer(indexId, shardId).listBlobs(), shardBlobsFuture);
                            return shardBlobsFuture;
                        });
                    }
                    return shardCount;
                }, shardCountFuture);
                return shardCountFuture;
            }).addListener(makeListener(indexSnapshotRefs, shardCount -> {
                for (int i = 0; i < shardCount; i++) {
                    final var shardId = i;
                    shardBlobsListenersByShard.get(i)
                        .addListener(
                            makeListener(
                                indexSnapshotRefs,
                                shardBlobs -> forkSupply(
                                    indexSnapshotRefs,
                                    () -> blobStoreRepository.loadShardSnapshot(
                                        blobStoreRepository.shardContainer(indexId, shardId),
                                        snapshotId
                                    ),
                                    shardSnapshot -> verifyShardSnapshot(snapshotId, shardId, shardBlobs, shardSnapshot)
                                )
                            )
                        );
                }
            }));
        }

        private void verifyShardSnapshot(
            SnapshotId snapshotId,
            int shardId,
            Map<String, BlobMetadata> shardBlobs,
            BlobStoreIndexShardSnapshot shardSnapshot
        ) {
            if (shardSnapshot.snapshot().equals(snapshotId.getName()) == false) {
                addFailure(
                    "snapshot [%s] for shard [%s/%d] has mismatched name [%s]",
                    snapshotId,
                    indexId,
                    shardId,
                    shardSnapshot.snapshot()
                );
            }

            for (final var fileInfo : shardSnapshot.indexFiles()) {
                verifyFileInfo(snapshotId.toString(), shardId, shardBlobs, fileInfo);
            }
        }

        private static String formatExact(ByteSizeValue byteSizeValue) {
            if (byteSizeValue.getBytes() >= ByteSizeUnit.KB.toBytes(1)) {
                return format("%s/%dB", byteSizeValue.toString(), byteSizeValue.getBytes());
            } else {
                return byteSizeValue.toString();
            }
        }

        private void verifyFileInfo(
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
                        "snapshot [%s] for shard [%s/%d] has virtual blob [%s] for [%s] with length [%s] instead of [%s]",
                        snapshot,
                        indexId,
                        shardId,
                        fileInfo.name(),
                        fileInfo.physicalName(),
                        formatExact(actualLength),
                        formatExact(fileLength)
                    );
                }
            } else {
                for (int part = 0; part < fileInfo.numberOfParts(); part++) {
                    final var blobName = fileInfo.partName(part);
                    final var blobInfo = shardBlobs.get(blobName);
                    final var partLength = ByteSizeValue.ofBytes(fileInfo.partBytes(part));
                    if (blobInfo == null) {
                        addFailure(
                            "snapshot [%s] for shard [%s/%d] has missing blob [%s] for [%s] part [%d/%d]; "
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

                    } else if (blobInfo.length() != partLength.getBytes()) {
                        addFailure(
                            "snapshot [%s] for shard [%s/%d] has blob [%s] for [%s] part [%d/%d] with length [%s] instead of [%s]; "
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
                    }
                }
            }
        }

        private void onIndexMetadataChecksComplete() {
            if (shardCountListenersByBlobId.isEmpty()) {
                throw new IllegalStateException(format("index [%s] has no metadata", indexId));
            }

            try (var shardGenerationChecksRef = wrap(makeVoidListener(indexRefs, () -> {}))) {
                for (final var shardEntry : shardBlobsListenersByShard.entrySet()) {
                    verifyShardGenerations(shardGenerationChecksRef, shardEntry);
                }
            }
        }

        private void verifyShardGenerations(
            RefCounted shardGenerationChecksRef,
            Map.Entry<Integer, ListenableActionFuture<Map<String, BlobMetadata>>> shardEntry
        ) {
            final int shardId = shardEntry.getKey();
            shardEntry.getValue()
                .addListener(
                    makeListener(
                        shardGenerationChecksRef,
                        shardBlobs -> forkSupply(
                            shardGenerationChecksRef,
                            () -> blobStoreRepository.getBlobStoreIndexShardSnapshots(
                                indexId,
                                shardId,
                                Objects.requireNonNull(
                                    repositoryData.shardGenerations().getShardGen(indexId, shardId),
                                    "shard generations for " + indexId + "/" + shardId
                                )
                            ),
                            blobStoreIndexShardSnapshots -> {
                                for (final var snapshotFiles : blobStoreIndexShardSnapshots.snapshots()) {
                                    snapshotFiles.snapshot(); // TODO validate
                                    snapshotFiles.shardStateIdentifier(); // TODO validate
                                    for (final var fileInfo : snapshotFiles.indexFiles()) {
                                        verifyFileInfo(snapshotFiles.snapshot(), shardId, shardBlobs, fileInfo);
                                    }
                                }
                            }
                        )
                    )
                );
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
                        onCompletion();
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
        int maxConcurrency
    ) {
        try (var throttledIterator = new ThrottledIterator<>(completionListener, iterator, consumer, maxConcurrency)) {
            throttledIterator.run();
        }
    }

    private static class ThrottledIterator<T> implements Releasable {
        private final RefCountedListenerWrapper refCounted;
        private final Iterator<T> iterator;
        private final BiConsumer<RefCounted, T> consumer;
        private final Semaphore permits;

        ThrottledIterator(
            ActionListener<Void> completionListener,
            Iterator<T> iterator,
            BiConsumer<RefCounted, T> consumer,
            int maxConcurrency
        ) {
            this.refCounted = wrap(completionListener);
            this.iterator = iterator;
            this.consumer = consumer;
            this.permits = new Semaphore(maxConcurrency);
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
                refCounted.incRef();
                final var itemRefCount = AbstractRefCounted.of(() -> {
                    permits.release();
                    try {
                        run();
                    } finally {
                        refCounted.decRef();
                    }
                });
                try {
                    consumer.accept(itemRefCount, item);
                } finally {
                    itemRefCount.decRef();
                }
            }
        }

        @Override
        public void close() {
            refCounted.close();
        }
    }
}
