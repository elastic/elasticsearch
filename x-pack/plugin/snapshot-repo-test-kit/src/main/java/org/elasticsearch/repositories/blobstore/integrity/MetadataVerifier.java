/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.integrity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.RateLimitingInputStream;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

public class MetadataVerifier implements Releasable {
    private static final Logger logger = LogManager.getLogger(MetadataVerifier.class);

    public enum Anomaly {
        FAILED_TO_LOAD_SNAPSHOT_INFO,
        FAILED_TO_LOAD_GLOBAL_METADATA,
        FAILED_TO_LOAD_SHARD_SNAPSHOT,
        FAILED_TO_LOAD_INDEX_METADATA,
        FAILED_TO_LIST_SHARD_CONTAINER,
        FAILED_TO_LOAD_SHARD_GENERATION,
        MISSING_BLOB,
        MISMATCHED_BLOB_LENGTH,
        CORRUPT_DATA_BLOB,
        UNDEFINED_SHARD_GENERATION,
        UNEXPECTED_EXCEPTION,
        FILE_IN_SHARD_GENERATION_NOT_SNAPSHOT,
        SNAPSHOT_SHARD_GENERATION_MISMATCH,
        FILE_IN_SNAPSHOT_NOT_SHARD_GENERATION,
        UNKNOWN_SNAPSHOT_FOR_INDEX,
        SNAPSHOT_NOT_IN_SHARD_GENERATION,
    }

    private static void mappedField(XContentBuilder builder, String fieldName, String type) throws IOException {
        builder.startObject(fieldName).field("type", type).endObject();
    }

    public static void run(
        BlobStoreRepository blobStoreRepository,
        NodeClient client,
        VerifyRepositoryIntegrityAction.Request verifyRequest,
        CancellableThreads cancellableThreads,
        VerifyRepositoryIntegrityAction.Task backgroundTask,
        ActionListener<VerifyRepositoryIntegrityAction.Response> foregroundTaskListener,
        ActionListener<Void> backgroundTaskListener
    ) {
        logger.info(
            "[{}] verifying metadata integrity and writing results to [{}]",
            verifyRequest.getRepository(),
            verifyRequest.getResultsIndex()
        );

        final var repositoryDataFuture = new ListenableActionFuture<RepositoryData>();
        blobStoreRepository.getRepositoryData(repositoryDataFuture);

        final var createIndex = client.admin()
            .indices()
            .prepareCreate(verifyRequest.getResultsIndex())
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1"));

        try (var builder = XContentFactory.jsonBuilder()) {
            builder.startObject().startObject("_doc").field("dynamic", "strict").startObject("properties");

            mappedField(builder, "@timestamp", "date");
            mappedField(builder, "task", "keyword");
            mappedField(builder, "repository", "keyword");
            mappedField(builder, "uuid", "keyword");
            mappedField(builder, "repository_generation", "long");
            mappedField(builder, "final_repository_generation", "long");
            mappedField(builder, "completed", "boolean");
            mappedField(builder, "cancelled", "boolean");
            mappedField(builder, "total_anomalies", "long");

            builder.startObject("error").field("type", "object").field("dynamic", "false").endObject();

            builder.startObject("snapshot").startObject("properties");
            mappedField(builder, "name", "keyword");
            mappedField(builder, "id", "keyword");
            mappedField(builder, "start_time", "date");
            mappedField(builder, "end_time", "date");
            builder.endObject().endObject();

            builder.startObject("index").startObject("properties");
            mappedField(builder, "name", "keyword");
            mappedField(builder, "id", "keyword");
            mappedField(builder, "metadata_blob", "keyword");
            mappedField(builder, "shards", "long");
            builder.endObject().endObject();
            mappedField(builder, "shard", "long");

            mappedField(builder, "anomaly", "keyword");
            mappedField(builder, "blob_name", "keyword");
            mappedField(builder, "part", "long");
            mappedField(builder, "number_of_parts", "long");
            mappedField(builder, "file_name", "keyword");
            mappedField(builder, "file_length_in_bytes", "long");
            mappedField(builder, "part_length_in_bytes", "long");
            mappedField(builder, "actual_length_in_bytes", "long");
            mappedField(builder, "expected_length_in_bytes", "long");

            mappedField(builder, "restorability", "keyword");
            mappedField(builder, "total_snapshots", "long");
            mappedField(builder, "restorable_snapshots", "long");
            mappedField(builder, "unrestorable_snapshots", "long");

            builder.endObject();
            builder.endObject();
            builder.endObject();
            createIndex.setMapping(builder);
        } catch (Exception e) {
            logger.error("error generating index mapping", e);
            foregroundTaskListener.onFailure(e);
            return;
        }

        createIndex.execute(new ActionListener<>() {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                onSuccess();
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ResourceAlreadyExistsException) {
                    onSuccess();
                } else {
                    foregroundTaskListener.onFailure(e);
                }
            }

            private void onSuccess() {
                repositoryDataFuture.addListener(foregroundTaskListener.map(repositoryData -> {
                    try (
                        var metadataVerifier = new MetadataVerifier(
                            blobStoreRepository,
                            client,
                            verifyRequest,
                            repositoryData,
                            cancellableThreads,
                            backgroundTask,
                            createLoggingListener(backgroundTaskListener, repositoryData)
                        )
                    ) {
                        logger.info(
                            "[{}] verifying metadata integrity for index generation [{}]: "
                                + "repo UUID [{}], cluster UUID [{}], snapshots [{}], indices [{}], index snapshots [{}]",
                            verifyRequest.getRepository(),
                            repositoryData.getGenId(),
                            repositoryData.getUuid(),
                            repositoryData.getClusterUUID(),
                            metadataVerifier.getSnapshotCount(),
                            metadataVerifier.getIndexCount(),
                            metadataVerifier.getIndexSnapshotCount()
                        );
                        return metadataVerifier.start();
                    }
                }));
            }

            private ActionListener<Long> createLoggingListener(ActionListener<Void> l, RepositoryData repositoryData) {
                return new ActionListener<>() {
                    @Override
                    public void onResponse(Long anomalyCount) {
                        logger.info(
                            "[{}] completed verifying metadata integrity for index generation [{}]: "
                                + "repo UUID [{}], cluster UUID [{}], anomalies [{}]",
                            verifyRequest.getRepository(),
                            repositoryData.getGenId(),
                            repositoryData.getUuid(),
                            repositoryData.getClusterUUID(),
                            anomalyCount
                        );
                        l.onResponse(null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(
                            () -> Strings.format(
                                "[%s] failed verifying metadata integrity for index generation [%d]: repo UUID [%s], cluster UUID [%s]",
                                verifyRequest.getRepository(),
                                repositoryData.getGenId(),
                                repositoryData.getUuid(),
                                repositoryData.getClusterUUID()
                            )
                        );
                        l.onFailure(e);
                    }
                };
            }
        });
    }

    private final BlobStoreRepository blobStoreRepository;
    private final NodeClient client;
    private final ActionListener<Long> finalListener;
    private final RefCountingRunnable finalRefs = new RefCountingRunnable(this::onCompletion);
    private final String repositoryName;
    private final VerifyRepositoryIntegrityAction.Request verifyRequest;
    private final RepositoryData repositoryData;
    private final BooleanSupplier isCancelledSupplier;
    private final VerifyRepositoryIntegrityAction.Task task;
    private final TaskId taskId;
    private final AtomicLong anomalyCount = new AtomicLong();
    private final Map<String, SnapshotDescription> snapshotDescriptionsById = ConcurrentCollections.newConcurrentMap();
    private final CancellableRunner metadataTaskRunner;
    private final CancellableRunner snapshotTaskRunner;
    private final String resultsIndex;
    private final RateLimiter rateLimiter;

    private final long snapshotCount;
    private final AtomicLong snapshotProgress = new AtomicLong();
    private final long indexCount;
    private final AtomicLong indexProgress = new AtomicLong();
    private final long indexSnapshotCount;
    private final AtomicLong indexSnapshotProgress = new AtomicLong();
    private final AtomicLong blobsVerified = new AtomicLong();
    private final AtomicLong blobBytesVerified = new AtomicLong();
    private final AtomicLong throttledNanos;

    MetadataVerifier(
        BlobStoreRepository blobStoreRepository,
        NodeClient client,
        VerifyRepositoryIntegrityAction.Request verifyRequest,
        RepositoryData repositoryData,
        CancellableThreads cancellableThreads,
        VerifyRepositoryIntegrityAction.Task task,
        ActionListener<Long> finalListener
    ) {
        this.blobStoreRepository = blobStoreRepository;
        this.repositoryName = blobStoreRepository.getMetadata().name();
        this.client = client;
        this.verifyRequest = verifyRequest;
        this.repositoryData = repositoryData;
        this.isCancelledSupplier = cancellableThreads::isCancelled;
        this.task = task;
        this.taskId = new TaskId(client.getLocalNodeId(), task.getId());
        this.finalListener = finalListener;
        this.snapshotTaskRunner = new CancellableRunner(
            new ThrottledTaskRunner(
                "verify-blob",
                verifyRequest.getBlobThreadPoolConcurrency(),
                blobStoreRepository.threadPool().executor(ThreadPool.Names.SNAPSHOT)
            ),
            cancellableThreads
        );
        this.metadataTaskRunner = new CancellableRunner(
            new ThrottledTaskRunner(
                "verify-metadata",
                verifyRequest.getMetaThreadPoolConcurrency(),
                blobStoreRepository.threadPool().executor(ThreadPool.Names.SNAPSHOT_META)
            ),
            cancellableThreads
        );
        this.resultsIndex = verifyRequest.getResultsIndex();

        this.snapshotCount = repositoryData.getSnapshotIds().size();
        this.indexCount = repositoryData.getIndices().size();
        this.indexSnapshotCount = repositoryData.getIndexSnapshotCount();
        this.rateLimiter = new RateLimiter.SimpleRateLimiter(verifyRequest.getMaxBytesPerSec().getMbFrac());

        this.throttledNanos = new AtomicLong(verifyRequest.getVerifyBlobContents() ? 1 : 0); // nonzero if verifying so status reported
    }

    @Override
    public void close() {
        finalRefs.close();
    }

    private VerifyRepositoryIntegrityAction.Status getStatus() {
        return new VerifyRepositoryIntegrityAction.Status(
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
            throttledNanos.get(),
            anomalyCount.get(),
            resultsIndex
        );
    }

    private VerifyRepositoryIntegrityAction.Response start() {
        task.setStatusSupplier(this::getStatus);
        verifySnapshots(this::verifyIndices);
        return new VerifyRepositoryIntegrityAction.Response(
            taskId,
            repositoryName,
            repositoryData.getGenId(),
            repositoryData.getUuid(),
            snapshotCount,
            indexCount,
            indexSnapshotCount,
            resultsIndex
        );
    }

    private void verifySnapshots(Runnable onCompletion) {
        runThrottled(
            repositoryData.getSnapshotIds().iterator(),
            this::verifySnapshot,
            verifyRequest.getSnapshotVerificationConcurrency(),
            snapshotProgress,
            wrapRunnable(finalRefs.acquire(), onCompletion)
        );
    }

    private record SnapshotDescription(SnapshotId snapshotId, long startTimeMillis, long endTimeMillis) {
        void writeXContent(XContentBuilder builder) throws IOException {
            builder.startObject("snapshot");
            builder.field("id", snapshotId.getUUID());
            builder.field("name", snapshotId.getName());
            if (startTimeMillis != 0) {
                builder.field("start_time", dateFormatter.format(Instant.ofEpochMilli(startTimeMillis)));
            }
            if (endTimeMillis != 0) {
                builder.field("end_time", dateFormatter.format(Instant.ofEpochMilli(endTimeMillis)));
            }
            builder.endObject();
        }
    }

    private void verifySnapshot(Releasable releasable, SnapshotId snapshotId) {
        try (var snapshotRefs = new RefCountingRunnable(releasable::close)) {
            if (isCancelledSupplier.getAsBoolean()) {
                // getSnapshotInfo does its own forking so we must check for cancellation here
                return;
            }

            blobStoreRepository.getSnapshotInfo(snapshotId, ActionListener.releaseAfter(new ActionListener<>() {
                @Override
                public void onResponse(SnapshotInfo snapshotInfo) {
                    final var snapshotDescription = new SnapshotDescription(snapshotId, snapshotInfo.startTime(), snapshotInfo.endTime());
                    snapshotDescriptionsById.put(snapshotId.getUUID(), snapshotDescription);
                    metadataTaskRunner.run(ActionRunnable.run(snapshotRefs.acquireListener(), () -> {
                        try {
                            blobStoreRepository.getSnapshotGlobalMetadata(snapshotDescription.snapshotId());
                            // no checks here, loading it is enough
                        } catch (Exception e) {
                            addAnomaly(Anomaly.FAILED_TO_LOAD_GLOBAL_METADATA, snapshotRefs.acquire(), (builder, params) -> {
                                snapshotDescription.writeXContent(builder);
                                ElasticsearchException.generateFailureXContent(builder, params, e, true);
                                return builder;
                            });
                        }
                    }));
                }

                @Override
                public void onFailure(Exception e) {
                    addAnomaly(Anomaly.FAILED_TO_LOAD_SNAPSHOT_INFO, snapshotRefs.acquire(), (builder, params) -> {
                        new SnapshotDescription(snapshotId, 0, 0).writeXContent(builder);
                        ElasticsearchException.generateFailureXContent(builder, params, e, true);
                        return builder;
                    });
                }
            }, snapshotRefs.acquire()));
        }
    }

    private void verifyIndices() {
        runThrottled(
            repositoryData.getIndices().values().iterator(),
            (releasable, indexId) -> new IndexVerifier(releasable, indexId).run(),
            verifyRequest.getIndexVerificationConcurrency(),
            indexProgress,
            wrapRunnable(finalRefs.acquire(), () -> {})
        );
    }

    private record IndexDescription(IndexId indexId, String indexMetadataBlob, int shardCount) {
        void writeXContent(XContentBuilder builder) throws IOException {
            writeIndexId(indexId, builder, b -> b.field("metadata_blob", indexMetadataBlob).field("shards", shardCount));
        }
    }

    private record ShardContainerContents(int shardId, Map<String, BlobMetadata> blobsByName, @Nullable // if it could not be read
    BlobStoreIndexShardSnapshots blobStoreIndexShardSnapshots, Map<String, ListenableActionFuture<Void>> blobContentsListeners) {}

    private class IndexVerifier {
        private final RefCountingRunnable indexRefs;
        private final IndexId indexId;
        private final Map<Integer, ListenableActionFuture<ShardContainerContents>> shardContainerContentsListener = newConcurrentMap();
        private final Map<String, ListenableActionFuture<IndexDescription>> indexDescriptionListenersByBlobId = newConcurrentMap();
        private final AtomicInteger totalSnapshotCounter = new AtomicInteger();
        private final AtomicInteger restorableSnapshotCounter = new AtomicInteger();

        IndexVerifier(Releasable releasable, IndexId indexId) {
            this.indexRefs = new RefCountingRunnable(releasable::close);
            this.indexId = indexId;
        }

        void run() {
            runThrottled(
                repositoryData.getSnapshots(indexId).iterator(),
                this::verifyIndexSnapshot,
                verifyRequest.getIndexSnapshotVerificationConcurrency(),
                indexSnapshotProgress,
                wrapRunnable(indexRefs, () -> recordRestorability(totalSnapshotCounter.get(), restorableSnapshotCounter.get()))
            );
        }

        private void recordRestorability(int totalSnapshotCount, int restorableSnapshotCount) {
            if (isCancelledSupplier.getAsBoolean() == false) {
                addResult(indexRefs.acquire(), (builder, params) -> {
                    writeIndexId(indexId, builder, b -> {});
                    builder.field(
                        "restorability",
                        totalSnapshotCount == restorableSnapshotCount ? "full" : 0 < restorableSnapshotCount ? "partial" : "none"
                    );
                    builder.field("total_snapshots", totalSnapshotCount);
                    builder.field("restorable_snapshots", restorableSnapshotCount);
                    builder.field("unrestorable_snapshots", totalSnapshotCount - restorableSnapshotCount);
                    return builder;
                });
            }
        }

        private void verifyIndexSnapshot(Releasable releasable, SnapshotId snapshotId) {
            try (var indexSnapshotRefs = new RefCountingRunnable(releasable::close)) {
                totalSnapshotCounter.incrementAndGet();

                final var snapshotDescription = snapshotDescriptionsById.get(snapshotId.getUUID());
                if (snapshotDescription == null) {
                    addAnomaly(Anomaly.UNKNOWN_SNAPSHOT_FOR_INDEX, indexSnapshotRefs.acquire(), (builder, params) -> {
                        writeIndexId(indexId, builder, b -> {});
                        new SnapshotDescription(snapshotId, 0, 0).writeXContent(builder);
                        return builder;
                    });
                    return;
                }

                final var indexMetaBlobId = repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotId, indexId);
                indexDescriptionListeners(snapshotId, indexMetaBlobId).addListener(
                    makeListener(
                        indexSnapshotRefs.acquire(),
                        indexDescription -> verifyShardSnapshots(snapshotDescription, indexDescription, indexSnapshotRefs.acquire())
                    )
                );
            }
        }

        private void verifyShardSnapshots(
            SnapshotDescription snapshotDescription,
            IndexDescription indexDescription,
            Releasable releasable
        ) {
            final var restorableShardCount = new AtomicInteger();
            try (var shardSnapshotsRefs = new RefCountingRunnable(wrapRunnable(releasable, () -> {
                if (indexDescription.shardCount() == restorableShardCount.get()) {
                    restorableSnapshotCounter.incrementAndGet();
                }
            }))) {
                for (int shardId = 0; shardId < indexDescription.shardCount(); shardId++) {
                    shardContainerContentsListeners(indexDescription, shardId).addListener(
                        makeListener(
                            shardSnapshotsRefs.acquire(),
                            shardContainerContents -> metadataTaskRunner.run(
                                ActionRunnable.run(
                                    shardSnapshotsRefs.acquireListener(),
                                    () -> verifyShardSnapshot(
                                        snapshotDescription,
                                        indexDescription,
                                        shardContainerContents,
                                        restorableShardCount,
                                        shardSnapshotsRefs
                                    )
                                )
                            )
                        )
                    );
                }
            }
        }

        private void verifyShardSnapshot(
            SnapshotDescription snapshotDescription,
            IndexDescription indexDescription,
            ShardContainerContents shardContainerContents,
            AtomicInteger restorableShardCount,
            RefCountingRunnable shardSnapshotsRefs
        ) throws AnomalyException {
            final var shardId = shardContainerContents.shardId();
            final BlobStoreIndexShardSnapshot blobStoreIndexShardSnapshot;
            try {
                blobStoreIndexShardSnapshot = blobStoreRepository.loadShardSnapshot(
                    blobStoreRepository.shardContainer(indexId, shardId),
                    snapshotDescription.snapshotId()
                );
            } catch (Exception e) {
                addAnomaly(Anomaly.FAILED_TO_LOAD_SHARD_SNAPSHOT, shardSnapshotsRefs.acquire(), (builder, params) -> {
                    snapshotDescription.writeXContent(builder);
                    indexDescription.writeXContent(builder);
                    builder.field("shard", shardId);
                    ElasticsearchException.generateFailureXContent(builder, params, e, true);
                    return builder;
                });
                throw new AnomalyException(e);
            }

            final var restorable = new AtomicBoolean(true);
            runThrottled(
                blobStoreIndexShardSnapshot.indexFiles().iterator(),
                (releasable, fileInfo) -> verifyFileInfo(
                    releasable,
                    snapshotDescription,
                    indexDescription,
                    shardContainerContents,
                    restorable,
                    fileInfo
                ),
                1,
                blobsVerified,
                wrapRunnable(shardSnapshotsRefs.acquire(), () -> {
                    if (restorable.get()) {
                        restorableShardCount.incrementAndGet();
                    }
                })
            );

            verifyConsistentShardFiles(
                snapshotDescription,
                indexDescription,
                shardContainerContents,
                blobStoreIndexShardSnapshot,
                shardSnapshotsRefs
            );
        }

        private void verifyConsistentShardFiles(
            SnapshotDescription snapshotDescription,
            IndexDescription indexDescription,
            ShardContainerContents shardContainerContents,
            BlobStoreIndexShardSnapshot blobStoreIndexShardSnapshot,
            RefCountingRunnable shardSnapshotsRefs
        ) {
            final var blobStoreIndexShardSnapshots = shardContainerContents.blobStoreIndexShardSnapshots();
            if (blobStoreIndexShardSnapshots == null) {
                // already reported
                return;
            }

            final var shardId = shardContainerContents.shardId();
            for (SnapshotFiles summary : blobStoreIndexShardSnapshots.snapshots()) {
                if (summary.snapshot().equals(snapshotDescription.snapshotId().getName()) == false) {
                    continue;
                }

                final var snapshotFiles = blobStoreIndexShardSnapshot.indexFiles()
                    .stream()
                    .collect(Collectors.toMap(BlobStoreIndexShardSnapshot.FileInfo::physicalName, Function.identity()));

                for (final var summaryFile : summary.indexFiles()) {
                    final var snapshotFile = snapshotFiles.get(summaryFile.physicalName());
                    if (snapshotFile == null) {
                        addAnomaly(Anomaly.FILE_IN_SHARD_GENERATION_NOT_SNAPSHOT, shardSnapshotsRefs.acquire(), (builder, params) -> {
                            snapshotDescription.writeXContent(builder);
                            indexDescription.writeXContent(builder);
                            builder.field("shard", shardId);
                            builder.field("file_name", summaryFile.physicalName());
                            return builder;
                        });
                    } else if (summaryFile.isSame(snapshotFile) == false) {
                        addAnomaly(Anomaly.SNAPSHOT_SHARD_GENERATION_MISMATCH, shardSnapshotsRefs.acquire(), (builder, params) -> {
                            snapshotDescription.writeXContent(builder);
                            indexDescription.writeXContent(builder);
                            builder.field("shard", shardId);
                            builder.field("file_name", summaryFile.physicalName());
                            return builder;
                        });
                    }
                }

                final var summaryFiles = summary.indexFiles()
                    .stream()
                    .collect(Collectors.toMap(BlobStoreIndexShardSnapshot.FileInfo::physicalName, Function.identity()));
                for (final var snapshotFile : blobStoreIndexShardSnapshot.indexFiles()) {
                    if (summaryFiles.get(snapshotFile.physicalName()) == null) {
                        addAnomaly(Anomaly.FILE_IN_SNAPSHOT_NOT_SHARD_GENERATION, shardSnapshotsRefs.acquire(), (builder, params) -> {
                            snapshotDescription.writeXContent(builder);
                            indexDescription.writeXContent(builder);
                            builder.field("shard", shardId);
                            builder.field("file_name", snapshotFile.physicalName());
                            return builder;
                        });
                    }
                }

                return;
            }

            addAnomaly(Anomaly.SNAPSHOT_NOT_IN_SHARD_GENERATION, shardSnapshotsRefs.acquire(), (builder, params) -> {
                snapshotDescription.writeXContent(builder);
                indexDescription.writeXContent(builder);
                builder.field("shard", shardId);
                return builder;
            });
        }

        private void verifyFileInfo(
            Releasable releasable,
            SnapshotDescription snapshotDescription,
            IndexDescription indexDescription,
            ShardContainerContents shardContainerContents,
            AtomicBoolean restorable,
            BlobStoreIndexShardSnapshot.FileInfo fileInfo
        ) {
            try (var fileRefs = new RefCountingRunnable(releasable::close)) {
                if (fileInfo.metadata().hashEqualsContents()) {
                    return;
                }

                final var shardId = shardContainerContents.shardId();
                final var shardBlobs = shardContainerContents.blobsByName();
                final var fileLength = ByteSizeValue.ofBytes(fileInfo.length());
                for (int part = 0; part < fileInfo.numberOfParts(); part++) {
                    final var finalPart = part;
                    final var blobName = fileInfo.partName(part);
                    final var blobInfo = shardBlobs.get(blobName);
                    final var partLength = ByteSizeValue.ofBytes(fileInfo.partBytes(part));
                    if (blobInfo == null) {
                        restorable.set(false);
                        addAnomaly(Anomaly.MISSING_BLOB, fileRefs.acquire(), ((builder, params) -> {
                            snapshotDescription.writeXContent(builder);
                            indexDescription.writeXContent(builder);
                            builder.field("shard", shardId);
                            builder.field("blob_name", blobName);
                            builder.field("file_name", fileInfo.physicalName());
                            builder.field("part", finalPart);
                            builder.field("number_of_parts", fileInfo.numberOfParts());
                            builder.humanReadableField("file_length_in_bytes", "file_length", fileLength);
                            builder.humanReadableField("part_length_in_bytes", "part_length", partLength);
                            return builder;
                        }));
                        return;
                    } else if (blobInfo.length() != partLength.getBytes()) {
                        restorable.set(false);
                        addAnomaly(Anomaly.MISMATCHED_BLOB_LENGTH, fileRefs.acquire(), ((builder, params) -> {
                            snapshotDescription.writeXContent(builder);
                            indexDescription.writeXContent(builder);
                            builder.field("shard", shardId);
                            builder.field("blob_name", blobName);
                            builder.field("file_name", fileInfo.physicalName());
                            builder.field("part", finalPart);
                            builder.field("number_of_parts", fileInfo.numberOfParts());
                            builder.humanReadableField("file_length_in_bytes", "file_length", fileLength);
                            builder.humanReadableField("part_length_in_bytes", "part_length", partLength);
                            builder.humanReadableField("actual_length_in_bytes", "actual_length", ByteSizeValue.ofBytes(blobInfo.length()));
                            return builder;
                        }));
                        return;
                    }
                }

                blobContentsListeners(indexDescription, shardContainerContents, fileInfo).addListener(
                    makeListener(fileRefs.acquire(), (Void ignored) -> {}).delegateResponse((l, e) -> {
                        restorable.set(false);
                        addAnomaly(Anomaly.CORRUPT_DATA_BLOB, fileRefs.acquire(), ((builder, params) -> {
                            snapshotDescription.writeXContent(builder);
                            indexDescription.writeXContent(builder);
                            builder.field("shard", shardId);
                            builder.field("blob_name", fileInfo.name());
                            builder.field("file_name", fileInfo.physicalName());
                            builder.field("number_of_parts", fileInfo.numberOfParts());
                            builder.humanReadableField("file_length_in_bytes", "file_length", fileLength);
                            ElasticsearchException.generateFailureXContent(builder, params, e, true);
                            return builder;
                        }));
                        l.onResponse(null);
                    })
                );
            }
        }

        private ListenableActionFuture<IndexDescription> indexDescriptionListeners(SnapshotId snapshotId, String indexMetaBlobId) {
            return indexDescriptionListenersByBlobId.computeIfAbsent(indexMetaBlobId, ignored -> {
                final var indexDescriptionListener = new ListenableActionFuture<IndexDescription>();
                metadataTaskRunner.run(ActionRunnable.supply(indexDescriptionListener, () -> {
                    try {
                        return new IndexDescription(
                            indexId,
                            indexMetaBlobId,
                            blobStoreRepository.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId).getNumberOfShards()
                        );
                    } catch (Exception e) {
                        addAnomaly(Anomaly.FAILED_TO_LOAD_INDEX_METADATA, indexRefs.acquire(), (builder, params) -> {
                            writeIndexId(indexId, builder, b -> b.field("metadata_blob", indexMetaBlobId));
                            ElasticsearchException.generateFailureXContent(builder, params, e, true);
                            return builder;
                        });
                        throw new AnomalyException(e);
                    }
                }));
                return indexDescriptionListener;
            });
        }

        private ListenableActionFuture<ShardContainerContents> shardContainerContentsListeners(
            IndexDescription indexDescription,
            int shardId
        ) {
            return shardContainerContentsListener.computeIfAbsent(shardId, ignored -> {
                final var shardContainerContentsFuture = new ListenableActionFuture<ShardContainerContents>();
                metadataTaskRunner.run(ActionRunnable.supply(shardContainerContentsFuture, () -> {
                    final Map<String, BlobMetadata> blobsByName;
                    try {
                        blobsByName = blobStoreRepository.shardContainer(indexId, shardId).listBlobs();
                    } catch (Exception e) {
                        addAnomaly(Anomaly.FAILED_TO_LIST_SHARD_CONTAINER, indexRefs.acquire(), (builder, params) -> {
                            indexDescription.writeXContent(builder);
                            builder.field("shard", shardId);
                            ElasticsearchException.generateFailureXContent(builder, params, e, true);
                            return builder;
                        });
                        throw new AnomalyException(e);
                    }

                    final var shardGen = repositoryData.shardGenerations().getShardGen(indexId, shardId);
                    if (shardGen == null) {
                        addAnomaly(Anomaly.UNDEFINED_SHARD_GENERATION, indexRefs.acquire(), (builder, params) -> {
                            indexDescription.writeXContent(builder);
                            return builder.field("shard", shardId);
                        });
                        throw new AnomalyException(
                            new ElasticsearchException("undefined shard generation for " + indexId + "[" + shardId + "]")
                        );
                    }

                    return new ShardContainerContents(
                        shardId,
                        blobsByName,
                        loadShardGeneration(indexDescription, shardId, shardGen),
                        ConcurrentCollections.newConcurrentMap()
                    );
                }));
                return shardContainerContentsFuture;
            });
        }

        private BlobStoreIndexShardSnapshots loadShardGeneration(IndexDescription indexDescription, int shardId, ShardGeneration shardGen) {
            try {
                return blobStoreRepository.getBlobStoreIndexShardSnapshots(indexId, shardId, shardGen);
            } catch (Exception e) {
                addAnomaly(Anomaly.FAILED_TO_LOAD_SHARD_GENERATION, indexRefs.acquire(), (builder, params) -> {
                    indexDescription.writeXContent(builder);
                    builder.field("shard", shardId);
                    ElasticsearchException.generateFailureXContent(builder, params, e, true);
                    return builder;
                });
                // failing here is not fatal to snapshot restores, only to creating/deleting snapshots, so we can carry on with the analysis
                return null;
            }
        }

        private ListenableActionFuture<Void> blobContentsListeners(
            IndexDescription indexDescription,
            ShardContainerContents shardContainerContents,
            BlobStoreIndexShardSnapshot.FileInfo fileInfo
        ) {
            return shardContainerContents.blobContentsListeners().computeIfAbsent(fileInfo.name(), ignored -> {
                var listener = new ListenableActionFuture<Void>();

                if (verifyRequest.getVerifyBlobContents()) {
                    // TODO do this on a remote node?
                    snapshotTaskRunner.run(ActionRunnable.run(listener, () -> {
                        try (var slicedStream = new SlicedInputStream(fileInfo.numberOfParts()) {
                            @Override
                            protected InputStream openSlice(int slice) throws IOException {
                                return blobStoreRepository.shardContainer(indexDescription.indexId(), shardContainerContents.shardId())
                                    .readBlob(fileInfo.partName(slice));
                            }
                        };
                            var rateLimitedStream = new RateLimitingInputStream(slicedStream, () -> rateLimiter, throttledNanos::addAndGet);
                            var indexInput = new IndexInputWrapper(rateLimitedStream, fileInfo.length())
                        ) {
                            CodecUtil.checksumEntireFile(indexInput);
                        }
                    }));
                } else {
                    blobBytesVerified.addAndGet(fileInfo.length());
                    listener.onResponse(null);
                }

                return listener;
            });
        }
    }

    private <T> ActionListener<T> makeListener(Releasable releasable, CheckedConsumer<T, Exception> consumer) {
        try (var refs = new RefCountingRunnable(releasable::close)) {
            return ActionListener.releaseAfter(ActionListener.wrap(consumer, exception -> {
                if (isCancelledSupplier.getAsBoolean() && exception instanceof TaskCancelledException) {
                    return;
                }
                if (exception instanceof AnomalyException) {
                    // already reported
                    return;
                }
                addAnomaly(Anomaly.UNEXPECTED_EXCEPTION, refs.acquire(), (builder, params) -> {
                    ElasticsearchException.generateFailureXContent(builder, params, exception, true);
                    return builder;
                });
            }), refs.acquire());
        }
    }

    private Runnable wrapRunnable(Releasable releasable, Runnable runnable) {
        return () -> {
            try (releasable) {
                runnable.run();
            }
        };
    }

    private void onCompletion() {
        try (
            var completionRefs = new RefCountingRunnable(
                () -> client.admin()
                    .indices()
                    .prepareFlush(resultsIndex)
                    .execute(
                        finalListener.delegateFailure(
                            (l1, ignored1) -> client.admin()
                                .indices()
                                .prepareRefresh(resultsIndex)
                                .execute(l1.delegateFailure((l2, ignored2) -> l2.onResponse(anomalyCount.get())))
                        )
                    )
            )
        ) {
            blobStoreRepository.getRepositoryData(makeListener(completionRefs.acquire(), finalRepositoryData -> {
                final var finalRepositoryGeneration = finalRepositoryData.getGenId();
                addResult(completionRefs.acquire(), (builder, params) -> {
                    builder.field("completed", true);
                    builder.field("cancelled", isCancelledSupplier.getAsBoolean());
                    builder.field("final_repository_generation", finalRepositoryGeneration);
                    builder.field("total_anomalies", anomalyCount.get());
                    return builder;
                });
            }));
        }
    }

    private static <T> void runThrottled(
        Iterator<T> iterator,
        BiConsumer<Releasable, T> itemConsumer,
        int maxConcurrency,
        AtomicLong progressCounter,
        Runnable onCompletion
    ) {
        ThrottledIterator.run(iterator, itemConsumer, maxConcurrency, progressCounter::incrementAndGet, onCompletion);
    }

    private final Queue<Tuple<IndexRequest, Runnable>> pendingResults = ConcurrentCollections.newQueue();
    private final Semaphore resultsIndexingSemaphore = new Semaphore(1);

    private void processPendingResults() {
        while (resultsIndexingSemaphore.tryAcquire()) {
            final var bulkRequest = new BulkRequest();
            final var completionActions = new ArrayList<Runnable>();

            Tuple<IndexRequest, Runnable> nextItem;
            while ((nextItem = pendingResults.poll()) != null) {
                bulkRequest.add(nextItem.v1());
                completionActions.add(nextItem.v2());
            }

            if (completionActions.isEmpty()) {
                resultsIndexingSemaphore.release();
                return;
            }

            final var isRecursing = new AtomicBoolean(true);
            client.bulk(bulkRequest, ActionListener.runAfter(ActionListener.wrap(bulkResponse -> {
                for (BulkItemResponse bulkItemResponse : bulkResponse) {
                    if (bulkItemResponse.isFailed()) {
                        logger.error("error indexing result", bulkItemResponse.getFailure().getCause());
                    }
                }
            }, e -> logger.error("error indexing results", e)), () -> {
                resultsIndexingSemaphore.release();
                for (final var completionAction : completionActions) {
                    completionAction.run();
                }
                if (isRecursing.get() == false) {
                    processPendingResults();
                }
            }));
            isRecursing.set(false);
        }
    }

    private static final DateFormatter dateFormatter = DateFormatter.forPattern(FormatNames.ISO8601.getName()).withLocale(Locale.ROOT);

    private IndexRequest buildResultDoc(ToXContent toXContent) {
        try (var builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field(
                "@timestamp",
                dateFormatter.format(Instant.ofEpochMilli(blobStoreRepository.threadPool().absoluteTimeInMillis()))
            );
            builder.field("repository", repositoryName);
            builder.field("uuid", repositoryData.getUuid());
            builder.field("repository_generation", repositoryData.getGenId());
            builder.field("task", taskId.toString());
            toXContent.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return new IndexRequestBuilder(client, IndexAction.INSTANCE, resultsIndex).setSource(builder).request();
        } catch (Exception e) {
            logger.error("error generating failure output", e);
            return null;
        }
    }

    private void addAnomaly(Anomaly anomaly, Releasable releasable, ToXContent toXContent) {
        if (isCancelledSupplier.getAsBoolean()) {
            releasable.close();
        } else {
            anomalyCount.incrementAndGet();
            addResult(releasable, (builder, params) -> toXContent.toXContent(builder.field("anomaly", anomaly.toString()), params));
        }
    }

    private void addResult(Releasable releasable, ToXContent toXContent) {
        IndexRequest indexRequest = buildResultDoc(toXContent);
        if (indexRequest == null) {
            releasable.close();
        } else {
            logger.debug(() -> Strings.format("recording result document: %s", indexRequest.source().utf8ToString()));
            pendingResults.add(Tuple.tuple(indexRequest, releasable::close));
            processPendingResults();
        }
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

    private static void writeIndexId(IndexId indexId, XContentBuilder builder, CheckedConsumer<XContentBuilder, IOException> extra)
        throws IOException {
        builder.startObject("index");
        builder.field("id", indexId.getId());
        builder.field("name", indexId.getName());
        extra.accept(builder);
        builder.endObject();
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
                            cancellableThreads.execute(runnable::run);
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

    private static class AnomalyException extends Exception {
        AnomalyException(Exception cause) {
            super(cause);
        }
    }

}
