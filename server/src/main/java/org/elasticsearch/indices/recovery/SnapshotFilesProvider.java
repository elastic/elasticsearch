/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.function.Consumer;

public class SnapshotFilesProvider {
    private final RepositoriesService repositoriesService;

    public SnapshotFilesProvider(RepositoriesService repositoriesService) {
        this.repositoriesService = Objects.requireNonNull(repositoriesService);
    }

    public InputStream getInputStreamForSnapshotFile(String repositoryName,
                                                     IndexId indexId,
                                                     ShardId shardId,
                                                     BlobStoreIndexShardSnapshot.FileInfo fileInfo,
                                                     Consumer<Long> rateLimiterListener) {
        BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repositoriesService.repository(repositoryName);
        StoreFileMetadata storeFileMetadata = fileInfo.metadata();
        final InputStream inputStream;
        if (storeFileMetadata.hashEqualsContents()) {
            BytesRef content = storeFileMetadata.hash();
            inputStream = new ByteArrayInputStream(content.bytes, content.offset, content.length);
        } else {
            BlobContainer container = blobStoreRepository.shardContainer(indexId, shardId.id());
            inputStream = new SlicedInputStream(fileInfo.numberOfParts()) {
                @Override
                protected InputStream openSlice(int slice) throws IOException {
                    return container.readBlob(fileInfo.partName(slice));
                }
            };
        }
        return blobStoreRepository.maybeRateLimitRestores(inputStream, rateLimiterListener::accept);
    }
}
