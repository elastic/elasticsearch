/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotContext;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotShard;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link BlobStoreSearchableSnapshotShard} exposes shard files stored in a snapshot by accessing directly to the blob store repository
 * where the snapshot is stored.
 */
public class BlobStoreSearchableSnapshotShard extends SearchableSnapshotShard {

    private final RepositoriesService repositories;
    private final ThreadPool threadPool;

    public BlobStoreSearchableSnapshotShard(final SearchableSnapshotContext context,
                                            final RepositoriesService repositories,
                                            final ThreadPool threadPool) {
        super(context);
        this.repositories = Objects.requireNonNull(repositories);
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    @Override
    public Map<String, FileInfo> listSnapshotFiles() throws IOException {
        try {
            BlobStoreIndexShardSnapshot snapshot = execute((blobStoreRepository, blobContainer) ->
                blobStoreRepository.loadShardSnapshot(blobContainer, new SnapshotId("_na", getContext().getSnapshot())));
            return snapshot.indexFiles().stream().collect(Collectors.toMap(FileInfo::physicalName, Function.identity()));
        } catch (final Exception e) {
            throw new IOException("Failed to list snapshot files", e);
        }
    }

    @Override
    public ByteBuffer readSnapshotFile(final String name, final long position, final int length) throws IOException {
        final FileInfo fileInfo = listSnapshotFiles().get(name);

        //
        //    .-------------------------.
        //    |    PARENTAL ADVISORY    |
        //    |                         |
        //    |     over-simplistic     |
        //    |         buggy           |
        //    |     implementation      |
        //    |                         |
        //    '-------------------------'
        //

        try {
            if (fileInfo.numberOfParts() == 1) {
                return execute((blobStoreRepository, blobContainer) ->
                    blobContainer.readBlob(fileInfo.partName(0), position, Math.min(length, (int) fileInfo.partBytes(0))));
            }

            final ByteBuffer buffer = ByteBuffer.allocate(length);

            long pos = position;
            int len = length;
            while (len > 0) {
                final long currentPart = pos / fileInfo.partSize().getBytes();
                int remainingBytesInPart;
                if (currentPart < (fileInfo.numberOfParts() - 1)) {
                    remainingBytesInPart = Math.toIntExact(((currentPart + 1L) * fileInfo.partSize().getBytes()) - pos);
                } else {
                    remainingBytesInPart = Math.toIntExact(fileInfo.length() - pos);
                }
                final int read = Math.min(len, remainingBytesInPart);
                if (read == 0) {
                    break;
                }

                final long from = pos % fileInfo.partSize().getBytes();
                ByteBuffer partBytes = execute((blobStoreRepository, blobContainer) ->
                    blobContainer.readBlob(fileInfo.partName(currentPart), from, read));
                buffer.put(partBytes);

                pos += read;
                len -= read;
            }

            return buffer.limit(length).position(0);
        } catch (final Exception e) {
            throw new IOException("Failed to read [" + length + "] bytes from snapshot file [name: " + name + "] at [" + position + "]", e);
        }
    }

    /**
     * This method uses the {@link SearchableSnapshotContext} to retrieve the appropriate {@link BlobStoreRepository} and
     * {@link BlobContainer} instances to pass to the given function {@code func}. The function is executed using the
     * snapshot thread pool.
     */
    private <T> T execute(final CheckedBiFunction<BlobStoreRepository, BlobContainer, T, IOException> func) throws Exception {
        final Repository repository = repositories.repository(getContext().getRepository());
        if (repository instanceof BlobStoreRepository == false) {
            throw new IllegalArgumentException("Repository [" + repository.getMetadata().name() + "] is not supported");
        }

        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        final IndexId indexId = new IndexId("_na", getContext().getIndex());
        final ShardId shardId = new ShardId("_na", "_na", getContext().getShard());

        final PlainActionFuture<T> future = new PlainActionFuture<>();
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                future.onResponse(func.apply(blobStoreRepository, blobStoreRepository.shardContainer(indexId, shardId)));
            }

            @Override
            public void onFailure(Exception e) {
                future.onFailure(e);
            }
        });
        return future.get();
    }
}
