/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.shard.GlobalCheckpointListeners;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.util.HexFormat;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * A {@link StatelessCommitService} that writes a merge blob in the blob container of the shard for each BCC it uploads.
 * <p>
 * Nothing writes merge blobs in production yet, but nodes must already cope with them being present in the object store next to BCCs: this
 * service makes integration tests exercise that. {@code TestUtils.StatelessPluginWithTrialLicense} installs it instead of the default
 * commit service when running the {@code internalClusterTestWithMergeBlobs} Gradle task.
 * <p>
 * For each BCC, a merge blob is written either right before the upload starts, right after it completes, or a bit later (whatever the
 * state of the shard is by then, it may even be closed). The moment is chosen randomly, and it is part of the merge blob name (see
 * {@link Moment}). The name is otherwise derived from the term and the generation of the BCC: the generation at which the merge started
 * is lower than, equal to or greater than the one of the BCC, depending on the BCC generation.
 */
public class MergeBlobInjectingStatelessCommitService extends StatelessCommitService {

    private static final Logger logger = LogManager.getLogger(MergeBlobInjectingStatelessCommitService.class);

    private final ObjectStoreService objectStoreService;
    private final ClusterService clusterService;
    private final Set<ShardBcc> bccsToInjectAfterUpload = ConcurrentCollections.newConcurrentSet();

    /**
     * Identifies a BCC that is being uploaded by a shard.
     */
    private record ShardBcc(ShardId shardId, PrimaryTermAndGeneration primaryTermAndGeneration) {}

    public MergeBlobInjectingStatelessCommitService(
        Settings settings,
        ObjectStoreService objectStoreService,
        ClusterService clusterService,
        IndicesService indicesService,
        Client client,
        StatelessCommitCleaner commitCleaner,
        StatelessSharedBlobCacheService cacheService,
        SharedBlobCacheWarmingService cacheWarmingService,
        TelemetryProvider telemetryProvider
    ) {
        super(
            settings,
            objectStoreService,
            clusterService,
            indicesService,
            client,
            commitCleaner,
            cacheService,
            cacheWarmingService,
            telemetryProvider
        );
        this.objectStoreService = objectStoreService;
        this.clusterService = clusterService;
    }

    /**
     * Registers a consumer on the uploaded BCCs of the shard, which writes a merge blob for the BCCs that were selected to get it right
     * after their upload (see {@link #createAndRunCommitUpload}).
     */
    @Override
    protected ShardCommitState createShardCommitState(
        ShardId shardId,
        long primaryTerm,
        BooleanSupplier inititalizingNoSearchSupplier,
        Supplier<MappingLookup> mappingLookupSupplier,
        TriConsumer<Long, GlobalCheckpointListeners.GlobalCheckpointListener, TimeValue> addGlobalCheckpointListenerFunction,
        Runnable triggerTranslogReplicator
    ) {
        final var commitState = super.createShardCommitState(
            shardId,
            primaryTerm,
            inititalizingNoSearchSupplier,
            mappingLookupSupplier,
            addGlobalCheckpointListenerFunction,
            triggerTranslogReplicator
        );
        commitState.addConsumerForNewUploadedBcc(uploadedBcc -> {
            final var primaryTermAndGeneration = uploadedBcc.uploadedBcc().primaryTermAndGeneration();
            if (bccsToInjectAfterUpload.remove(new ShardBcc(shardId, primaryTermAndGeneration))) {
                injectMergeBlob(shardId, primaryTermAndGeneration, Moment.AFTER_UPLOAD);
            }
        });
        return commitState;
    }

    /**
     * Picks randomly the moment at which the merge blob of the BCC is written: before the upload starts (the upload then waits for the
     * write), after it completes, or a few milliseconds after it started.
     */
    @Override
    protected void createAndRunCommitUpload(
        ShardCommitState commitState,
        VirtualBatchedCompoundCommit virtualBcc,
        ShardCommitState.BlobReference blobReference
    ) {
        final var threadPool = clusterService.threadPool();
        final var shardId = virtualBcc.getShardId();
        final var primaryTermAndGeneration = virtualBcc.getPrimaryTermAndGeneration();
        // A single merge blob is written for each BCC, at a randomly chosen moment
        switch (Moment.values()[Randomness.get().nextInt(Moment.values().length)]) {
            case BEFORE_UPLOAD -> {
                // The calling thread cannot access the object store: write on the generic thread pool, and only then start the upload
                threadPool.generic().execute(() -> {
                    try {
                        injectMergeBlob(shardId, primaryTermAndGeneration, Moment.BEFORE_UPLOAD);
                    } finally {
                        super.createAndRunCommitUpload(commitState, virtualBcc, blobReference);
                    }
                });
            }
            case AFTER_UPLOAD -> {
                // injected by the consumer registered in createShardCommitState once the BCC is uploaded
                bccsToInjectAfterUpload.add(new ShardBcc(shardId, primaryTermAndGeneration));
                super.createAndRunCommitUpload(commitState, virtualBcc, blobReference);
            }
            case LATER -> {
                super.createAndRunCommitUpload(commitState, virtualBcc, blobReference);
                // whatever the state of the shard or of the upload is by then
                final var delay = TimeValue.timeValueMillis(5L * (primaryTermAndGeneration.generation() % 10));
                threadPool.schedule(() -> injectMergeBlob(shardId, primaryTermAndGeneration, Moment.LATER), delay, threadPool.generic());
            }
        }
    }

    /**
     * Writes the merge blob of a BCC in the blob container of the shard, for the primary term of the BCC. The write is best effort: it can
     * run while an upload is being completed, or after the shard is closed or deleted, and a failure must never fail anything else.
     */
    private void injectMergeBlob(ShardId shardId, PrimaryTermAndGeneration bccTermAndGeneration, Moment moment) {
        final long generation = bccTermAndGeneration.generation();
        final long mergeStartGeneration = Math.max(1L, generation + (generation % 5) - 2);
        // the segment name differs per moment, which makes it easy to tell when a merge blob was written
        final String blobName = Strings.format(
            "stateless_merge_%d_%s_%s",
            mergeStartGeneration,
            moment.segmentPrefix + Long.toString(generation, Character.MAX_RADIX),
            HexFormat.of().toHexDigits(generation)
        );
        try {
            // This can run while a BCC upload is being completed or after the shard is closed or deleted: a failure here must never
            // fail anything else
            objectStoreService.getProjectBlobContainer(shardId, bccTermAndGeneration.primaryTerm())
                .writeBlob(OperationPurpose.INDICES, blobName, new BytesArray(blobName), false);
        } catch (Exception e) {
            logger.debug(() -> "failed to inject merge blob [" + blobName + "] for " + shardId, e);
        }
    }

    /**
     * The moment, relative to the upload of the BCC, at which a merge blob is written. The prefix is used as the first character of the
     * segment name in the merge blob name, to tell when a merge blob was written.
     */
    private enum Moment {
        BEFORE_UPLOAD("b"),
        AFTER_UPLOAD("a"),
        LATER("l");

        private final String segmentPrefix;

        Moment(String segmentPrefix) {
            this.segmentPrefix = segmentPrefix;
        }
    }
}
