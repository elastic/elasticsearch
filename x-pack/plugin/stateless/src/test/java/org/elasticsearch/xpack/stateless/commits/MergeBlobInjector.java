/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit.MERGE_BLOB_PREFIX;

/// Writes fake `stateless_merge_*` blobs into the shard containers of indexing shards in integration tests, before anything writes such
/// blobs for real. Nodes must ignore them and must never delete them; [#injected()] exposes what was written so tests can check both.
/// Blobs are written when a shard starts, right after a BCC upload, and a few milliseconds after a BCC upload (by which point the shard
/// may be closed or the index deleted, deliberately).
///
/// TODO(szybia): remove once merge blobs are uploaded for real
public class MergeBlobInjector implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(MergeBlobInjector.class);

    public record InjectedBlob(ShardId shardId, long primaryTerm, String blobName) {}

    private enum Moment {
        ON_START('s'),
        AFTER_UPLOAD('u'),
        LATER('l');

        private final char letter;

        Moment(char letter) {
            this.letter = letter;
        }
    }

    private static final Map<InjectedBlob, byte[]> INJECTED = new ConcurrentHashMap<>();

    // Primary term of each shard removed by a restore to fewer shards, at the time it was removed
    private static final Map<ShardId, Long> REMOVED_SHARDS = new ConcurrentHashMap<>();

    public static void reset() {
        INJECTED.clear();
        REMOVED_SHARDS.clear();
    }

    /// Blobs injected since the last [#reset()] with the bytes written. A blob is registered before its write and unregistered if the
    /// write throws, so every entry was written and must still be in the object store. Blobs of removed shards are left out.
    public static Map<InjectedBlob, byte[]> injected() {
        return INJECTED.entrySet()
            .stream()
            .filter(e -> e.getKey().primaryTerm() > REMOVED_SHARDS.getOrDefault(e.getKey().shardId(), 0L))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /// Records the shards removed by a restore to fewer shards: the stale shard GC deletes every blob under them. The shard id can be
    /// reused later under the same index UUID, but only with a higher primary term.
    public static void clusterChanged(ClusterChangedEvent event) {
        if (event.metadataChanged() == false) {
            return;
        }
        for (var project : event.previousState().metadata().projects().values()) {
            for (IndexMetadata previous : project) {
                final var current = event.state().metadata().findIndex(previous.getIndex());
                if (current.isEmpty()) {
                    continue;
                }
                for (int shard = current.get().getNumberOfShards(); shard < previous.getNumberOfShards(); shard++) {
                    REMOVED_SHARDS.merge(new ShardId(previous.getIndex(), shard), previous.primaryTerm(shard), Math::max);
                }
            }
        }
    }

    private final BiFunction<ShardId, Long, BlobContainer> shardBlobContainer;
    private final StatelessCommitService commitService;
    private final ThreadPool threadPool;

    public MergeBlobInjector(
        BiFunction<ShardId, Long, BlobContainer> shardBlobContainer,
        StatelessCommitService commitService,
        ThreadPool threadPool
    ) {
        this.shardBlobContainer = Objects.requireNonNull(shardBlobContainer);
        this.commitService = Objects.requireNonNull(commitService);
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        if (indexShard.routingEntry().isPromotableToPrimary() == false) {
            return;
        }
        final ShardId shardId = indexShard.shardId();
        final long primaryTerm = indexShard.getOperationPrimaryTerm();
        // No BCC generation to relate to yet: pick any plausible merge start generation, including ones above every BCC the shard
        // will upload during a short test, so that listings see merge blobs "newer" than the latest commit.
        final long startGeneration = Randomness.get().nextLong(1, 1000);
        try {
            threadPool.generic().execute(() -> inject(shardId, primaryTerm, Moment.ON_START, startGeneration));
            commitService.addConsumerForNewUploadedBcc(shardId, info -> onBccUploaded(shardId, info));
        } catch (Exception e) {
            // the node may be shutting down, or the shard's commit state already unregistered if the shard closed right after starting
            logger.debug(() -> "failed to register merge blob injector for " + shardId, e);
        }
    }

    private void onBccUploaded(ShardId shardId, StatelessCommitService.UploadedBccInfo info) {
        // Upload consumers must not throw, see StatelessCommitService.ShardCommitState
        try {
            final var termAndGeneration = info.uploadedBcc().primaryTermAndGeneration();
            final long bccGeneration = termAndGeneration.generation();
            // below, equal to, or above the BCC generation, like a merge that started a few commits before it was first committed
            final long startGeneration = Math.max(1L, bccGeneration + (bccGeneration % 5) - 2);
            if (Randomness.get().nextBoolean()) {
                threadPool.generic().execute(() -> inject(shardId, termAndGeneration.primaryTerm(), Moment.AFTER_UPLOAD, startGeneration));
            } else {
                threadPool.schedule(
                    () -> inject(shardId, termAndGeneration.primaryTerm(), Moment.LATER, startGeneration),
                    TimeValue.timeValueMillis(Randomness.get().nextInt(51)),
                    threadPool.generic()
                );
            }
        } catch (Exception e) {
            logger.debug(() -> "failed to schedule merge blob injection for " + shardId, e);
        }
    }

    private void inject(ShardId shardId, long primaryTerm, Moment moment, long startGeneration) {
        final Random random = Randomness.get();
        // stateless_merge_<gen>_<segment>_<discriminator>: the segment is the moment letter plus the generation in base 36, never a '_'
        final String blobName = MERGE_BLOB_PREFIX + startGeneration + '_' + moment.letter + Long.toString(startGeneration, 36) + '_'
            + HexFormat.of().toHexDigits(random.nextLong());
        final byte[] bytes = new byte[random.nextInt(1, 4096)];
        random.nextBytes(bytes);
        final var injectedBlob = new InjectedBlob(shardId, primaryTerm, blobName);
        INJECTED.put(injectedBlob, bytes);
        try {
            shardBlobContainer.apply(shardId, primaryTerm).writeBlob(OperationPurpose.INDICES, blobName, new BytesArray(bytes), false);
            logger.debug("injected merge blob {} ({}, {} bytes)", injectedBlob, moment, bytes.length);
        } catch (Exception e) {
            // shard closed, index deleted, repository closed: all expected in tests. A failed write is not a deleted blob, so forget it.
            INJECTED.remove(injectedBlob);
            logger.debug(() -> "failed to inject merge blob " + injectedBlob + " (" + moment + ")", e);
        }
    }
}
