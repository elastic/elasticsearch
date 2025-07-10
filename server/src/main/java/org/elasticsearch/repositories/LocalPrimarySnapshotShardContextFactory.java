/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.function.Consumer;

import static org.elasticsearch.snapshots.SnapshotShardsService.getShardStateId;

public class LocalPrimarySnapshotShardContextFactory implements SnapshotShardContextFactory {

    private static final Logger logger = LogManager.getLogger(LocalPrimarySnapshotShardContextFactory.class);

    private final IndicesService indicesService;

    public LocalPrimarySnapshotShardContextFactory(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    @Override
    public void asyncCreate(
        ShardId shardId,
        Snapshot snapshot,
        IndexId indexId,
        IndexShardSnapshotStatus snapshotStatus,
        IndexVersion repositoryMetaVersion,
        long snapshotStartTime,
        ActionListener<ShardSnapshotResult> listener,
        Consumer<SnapshotShardContext> snapshotShardContextConsumer
    ) throws IOException {

        final IndexShard indexShard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
        if (indexShard.routingEntry().primary() == false) {
            throw new IndexShardSnapshotFailedException(shardId, "snapshot should be performed only on primary");
        }
        if (indexShard.routingEntry().relocating()) {
            // do not snapshot when in the process of relocation of primaries so we won't get conflicts
            throw new IndexShardSnapshotFailedException(shardId, "cannot snapshot while relocating");
        }

        final IndexShardState indexShardState = indexShard.state();
        if (indexShardState == IndexShardState.CREATED || indexShardState == IndexShardState.RECOVERING) {
            // shard has just been created, or still recovering
            throw new IndexShardSnapshotFailedException(shardId, "shard didn't fully recover yet");
        }

        snapshotStatus.updateStatusDescription("acquiring commit reference from IndexShard: triggers a shard flush");

        SnapshotIndexCommit snapshotIndexCommit = null;
        try {
            snapshotIndexCommit = new SnapshotIndexCommit(indexShard.acquireIndexCommitForSnapshot());
            snapshotStatus.updateStatusDescription("commit reference acquired, proceeding with snapshot");
            final var shardStateId = getShardStateId(indexShard, snapshotIndexCommit.indexCommit()); // not aborted so indexCommit() ok
            snapshotStatus.addAbortListener(makeAbortListener(indexShard.shardId(), snapshot, snapshotIndexCommit));
            snapshotStatus.ensureNotAborted();

            snapshotShardContextConsumer.accept(
                new LocalPrimarySnapshotShardContext(
                    indexShard.store(),
                    indexShard.mapperService(),
                    snapshot.getSnapshotId(),
                    indexId,
                    snapshotIndexCommit,
                    shardStateId,
                    snapshotStatus,
                    repositoryMetaVersion,
                    snapshotStartTime,
                    listener
                )
            );
            snapshotIndexCommit = null;
        } finally {
            if (snapshotIndexCommit != null) {
                snapshotIndexCommit.closingBefore(new ActionListener<Void>() {
                    @Override
                    public void onResponse(Void unused) {}

                    @Override
                    public void onFailure(Exception e) {
                        // we're already failing exceptionally, and prefer to propagate the original exception instead of this one
                        logger.warn(Strings.format("exception closing commit for [%s] in [%s]", shardId, snapshot), e);
                    }
                }).onResponse(null);
            }
        }
    }

    static ActionListener<IndexShardSnapshotStatus.AbortStatus> makeAbortListener(
        ShardId shardId,
        Snapshot snapshot,
        SnapshotIndexCommit snapshotIndexCommit
    ) {
        return new ActionListener<>() {
            @Override
            public void onResponse(IndexShardSnapshotStatus.AbortStatus abortStatus) {
                if (abortStatus == IndexShardSnapshotStatus.AbortStatus.ABORTED) {
                    assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC, ThreadPool.Names.SNAPSHOT);
                    snapshotIndexCommit.onAbort();
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> Strings.format("unexpected failure in %s", description()), e);
                assert false : e;
            }

            @Override
            public String toString() {
                return description();
            }

            private String description() {
                return Strings.format("abort listener for [%s] in [%s]", shardId, snapshot);
            }
        };
    }
}
