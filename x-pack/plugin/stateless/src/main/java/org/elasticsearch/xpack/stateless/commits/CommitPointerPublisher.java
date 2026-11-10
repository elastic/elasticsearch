/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.stateless.lucene.IndexDirectory;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.recovery.FastSearchActivationSettings;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import static org.elasticsearch.core.Strings.format;

/**
 * Prototype (fast activation, default off, see {@link FastSearchActivationSettings#COMMIT_POINTER_ENABLED_SETTING}): publishes the
 * {@link CommitPointer} of an indexing shard after each batched compound commit upload.
 * <p>
 * Ordering and monotonicity: the pointer of a BCC is built in the uploaded-BCC consumer, which {@link StatelessCommitService} invokes
 * after the BCC is in the object store and in generation order; writes are done off the upload thread, one at a time per shard, always
 * with the newest pending pointer and never with an older generation than the last one written by this shard copy. The pointer may
 * therefore lag the latest upload by one PUT, but it never names a blob that is not uploaded.
 */
public class CommitPointerPublisher implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(CommitPointerPublisher.class);

    private final StatelessCommitService commitService;
    private final ObjectStoreService objectStoreService;
    private final ClusterService clusterService;
    private final Executor executor;
    private final Map<ShardId, ShardPointerState> states = new ConcurrentHashMap<>();

    public CommitPointerPublisher(
        StatelessCommitService commitService,
        ObjectStoreService objectStoreService,
        ClusterService clusterService,
        Executor executor
    ) {
        this.commitService = commitService;
        this.objectStoreService = objectStoreService;
        this.clusterService = clusterService;
        this.executor = executor;
    }

    private boolean enabled() {
        return FastSearchActivationSettings.getOrDefault(
            clusterService.getClusterSettings(),
            FastSearchActivationSettings.COMMIT_POINTER_ENABLED_SETTING
        );
    }

    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        if (indexShard.routingEntry().isPromotableToPrimary() == false || enabled() == false) {
            return;
        }
        // called on the cluster applier thread: blob store access and the first write must not happen here
        executor.execute(() -> startPublishing(indexShard));
    }

    private void startPublishing(IndexShard indexShard) {
        final ShardId shardId = indexShard.shardId();
        if (indexShard.state() == org.elasticsearch.index.shard.IndexShardState.CLOSED) {
            return;
        }
        try {
            final var state = new ShardPointerState(indexShard, objectStoreService.getProjectBlobContainer(shardId));
            final var previous = states.put(shardId, state);
            if (previous != null) {
                previous.close();
            }
            // ranges of the recovered commit (computed from its CC headers during recovery, with replicated header/footer ranges)
            state.addKnownRanges(
                IndexDirectory.unwrapDirectory(indexShard.store().directory()).getBlobStoreCacheDirectory().getFileRangesByName()
            );
            // ranges published by a previous primary of this term (e.g. the source of a relocation): a hollow or relocated primary
            // does not compute replicated ranges for the files it recovered, the previous writer's pointer has them
            try {
                final var existing = CommitPointer.read(state.shardContainer, indexShard.getOperationPrimaryTerm());
                if (existing != null && existing.compoundCommit().shardId().equals(shardId)) {
                    state.addKnownRanges(existing.replicatedFileRanges());
                }
            } catch (Exception e) {
                logger.debug(() -> format("%s could not read the existing commit pointer", shardId), e);
            }
            commitService.addConsumerForNewUploadedBcc(shardId, info -> state.onUploaded(info.uploadedBcc()));
            // publish the latest commit uploaded so far (the recovered one, or one uploaded before the consumer was registered)
            final var latest = commitService.getLatestUploadedBcc(shardId);
            if (latest != null) {
                state.onUploaded(latest);
            }
        } catch (Exception e) {
            logger.warn(() -> format("%s failed to start publishing commit pointers", shardId), e);
        }
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        final var state = states.remove(shardId);
        if (state != null) {
            state.close();
        }
    }

    private final class ShardPointerState {
        private final IndexShard indexShard;
        private final BlobContainer shardContainer;
        // file -> ranges with replicated header/footer ranges, for the files of the latest published commit and newer uploads
        private Map<String, BlobFileRanges> knownRanges = new HashMap<>();
        private CommitPointer pending;
        private boolean writing;
        private boolean closed;
        private long lastWrittenBccGeneration = -1L;

        ShardPointerState(IndexShard indexShard, BlobContainer shardContainer) {
            this.indexShard = indexShard;
            this.shardContainer = shardContainer;
        }

        synchronized void addKnownRanges(Map<String, BlobFileRanges> ranges) {
            for (var entry : ranges.entrySet()) {
                if (entry.getValue().hasReplicatedRanges()) {
                    knownRanges.put(entry.getKey(), entry.getValue());
                }
            }
        }

        void onUploaded(BatchedCompoundCommit bcc) {
            if (enabled() == false) {
                return;
            }
            final long term = indexShard.getOperationPrimaryTerm();
            final String nodeId = clusterService.localNode().getId();
            synchronized (this) {
                if (closed) {
                    return;
                }
                knownRanges.putAll(CommitPointer.computeReplicatedFileRanges(bcc));
                final CommitPointer pointer = CommitPointer.create(term, nodeId, bcc, knownRanges);
                // keep only what later commits can still reference
                final Map<String, BlobFileRanges> retained = new HashMap<>();
                for (String file : bcc.lastCompoundCommit().commitFiles().keySet()) {
                    var r = knownRanges.get(file);
                    if (r != null) {
                        retained.put(file, r);
                    }
                }
                knownRanges = retained;
                if (pending == null
                    || pointer.batchedCompoundCommitTermAndGeneration().after(pending.batchedCompoundCommitTermAndGeneration())) {
                    pending = pointer;
                }
                if (writing) {
                    return;
                }
                writing = true;
            }
            executor.execute(this::drain);
        }

        private void drain() {
            while (true) {
                final CommitPointer pointer;
                synchronized (this) {
                    pointer = pending;
                    pending = null;
                    if (pointer == null || closed) {
                        writing = false;
                        return;
                    }
                }
                final long generation = pointer.batchedCompoundCommitTermAndGeneration().generation();
                if (generation <= lastWrittenBccGeneration) {
                    continue;
                }
                try {
                    final CommitPointer toWrite = withOpenBytes(pointer);
                    final var bytes = toWrite.toBytes();
                    shardContainer.writeBlobAtomic(
                        org.elasticsearch.common.blobstore.OperationPurpose.INDICES,
                        CommitPointer.blobName(toWrite.writerPrimaryTerm()),
                        bytes,
                        false
                    );
                    lastWrittenBccGeneration = generation;
                    logger.debug(
                        "{} published commit pointer for term [{}]: {} in BCC {}, size [{}], open bytes [{}]",
                        indexShard.shardId(),
                        toWrite.writerPrimaryTerm(),
                        toWrite.compoundCommit().toShortDescription(),
                        toWrite.batchedCompoundCommitTermAndGeneration(),
                        bytes.length(),
                        toWrite.openRangesSizeInBytes()
                    );
                } catch (Exception e) {
                    logger.warn(() -> format("%s failed to publish commit pointer of BCC %s", indexShard.shardId(), generation), e);
                }
            }
        }

        synchronized void close() {
            closed = true;
            pending = null;
        }

        // phase 4: aligned ranges and parsed .cfe files read for the previous pointer, reused for the next (only accessed by drain)
        private final Map<CommitPointerOpenBytes.RangeKey, org.elasticsearch.common.bytes.BytesReference> openBytesCache = new HashMap<>();
        private final Map<String, Map<String, org.elasticsearch.xpack.stateless.cache.Lucene90CompoundEntriesReader.FileEntry>> cfeCache =
            new HashMap<>();

        /**
         * Adds the open-time bytes of the commit to the pointer if
         * {@link FastSearchActivationSettings#COMMIT_POINTER_OPEN_BYTES_MAX_SIZE_SETTING}
         * is set and they fit, otherwise returns the pointer unchanged (today's content). The bytes are read from the object store (the
         * named BCCs are uploaded), one GET per group of close ranges, only for ranges not already read for the previous pointer.
         */
        private CommitPointer withOpenBytes(CommitPointer pointer) {
            final long maxBytes = clusterService.getClusterSettings()
                .get(FastSearchActivationSettings.COMMIT_POINTER_OPEN_BYTES_MAX_SIZE_SETTING)
                .getBytes();
            if (maxBytes <= 0L) {
                return pointer;
            }
            final ShardId shardId = indexShard.shardId();
            try {
                final CommitPointerOpenBytes.Stats[] stats = new CommitPointerOpenBytes.Stats[1];
                final var ranges = CommitPointerOpenBytes.collect(
                    pointer.compoundCommit(),
                    pointer.installedFileRanges(),
                    (blobFile, position, length) -> {
                        try (
                            var in = objectStoreService.getProjectBlobContainer(shardId, blobFile.primaryTerm())
                                .readBlob(
                                    org.elasticsearch.common.blobstore.OperationPurpose.INDICES,
                                    blobFile.blobName(),
                                    position,
                                    length
                                )
                        ) {
                            return org.elasticsearch.common.io.Streams.readFully(in);
                        }
                    },
                    maxBytes,
                    openBytesCache,
                    cfeCache,
                    stats
                );
                logger.debug(
                    "{} commit pointer open bytes of {}: {} ({} bytes in {} ranges, {} object store reads of {} bytes)",
                    shardId,
                    pointer.compoundCommit().toShortDescription(),
                    ranges == null ? "over the bound of [" + maxBytes + "] bytes, not carried" : "carried",
                    stats[0].bytes(),
                    stats[0].ranges(),
                    stats[0].reads(),
                    stats[0].bytesRead()
                );
                return ranges == null ? pointer : pointer.withOpenRanges(ranges);
            } catch (Exception e) {
                logger.debug(() -> format("%s failed to collect the open-time bytes of the commit pointer", shardId), e);
                openBytesCache.clear();
                return pointer;
            }
        }
    }
}
