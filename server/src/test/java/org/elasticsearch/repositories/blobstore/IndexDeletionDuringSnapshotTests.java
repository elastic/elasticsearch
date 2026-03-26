/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class IndexDeletionDuringSnapshotTests extends ESTestCase {

    public void testSnapshotDetectsDeletedIndex() {
        final var projectId = ProjectId.DEFAULT;
        final var index = new Index(randomIndexName(), UUIDs.randomBase64UUID());
        final var shardId = new ShardId(index, 0);
        // Randomize when the index is removed to exercise all ensureNotAborted call sites:
        // 0: before file processing
        // 1: per-file iteration check
        // 2: blob writing check
        final int removalPoint = randomIntBetween(0, 2);

        final var repositoryAndClusterService = TestUtils.createRepositoryAndClusterService(
            projectId,
            createTempDir(),
            xContentRegistry(),
            index
        );
        final var repository = repositoryAndClusterService.repository();
        final var clusterService = repositoryAndClusterService.clusterService();

        final var snapshotId = new SnapshotId(randomSnapshotName(), UUIDs.randomBase64UUID());
        final var indexId = new IndexId(index.getName(), index.getUUID());
        final var status = IndexShardSnapshotStatus.newInitializing(null, 1L);
        final var future = new PlainActionFuture<ShardSnapshotResult>();

        final var fileName = randomIdentifier("_");
        final var fileData = randomByteArrayOfLength(between(1, 100));
        final var fileMetadata = new StoreFileMetadata(
            fileName,
            fileData.length,
            "checksum",
            IndexVersion.current().luceneVersion().toString()
        );

        if (removalPoint == 0) {
            removeIndexFromClusterState(clusterService, projectId, index);
        }

        repository.snapshotShard(
            new SnapshotShardContext(snapshotId, indexId, null, status, IndexVersion.current(), randomMillisUpToYear9999(), future) {
                @Override
                public ShardId shardId() {
                    return shardId;
                }

                @Override
                public boolean isSearchableSnapshot() {
                    return false;
                }

                @Override
                public Store.MetadataSnapshot metadataSnapshot() {
                    return new Store.MetadataSnapshot(Map.of(fileName, fileMetadata), Map.of(), 0);
                }

                @Override
                public Collection<String> fileNames() {
                    if (removalPoint == 1) {
                        removeIndexFromClusterState(clusterService, projectId, index);
                    }
                    return List.of(fileName);
                }

                @Override
                public boolean assertFileContentsMatchHash(BlobStoreIndexShardSnapshot.FileInfo fileInfo) {
                    return true;
                }

                @Override
                public void failStoreIfCorrupted(Exception e) {}

                @Override
                public FileReader fileReader(String file, StoreFileMetadata metadata) throws IOException {
                    if (removalPoint == 2) {
                        removeIndexFromClusterState(clusterService, projectId, index);
                    }
                    return new FileReader() {
                        @Override
                        public InputStream openInput(long limit) {
                            return new ByteArrayInputStream(fileData, 0, Math.toIntExact(Math.min(limit, fileData.length)));
                        }

                        @Override
                        public void verify() {}

                        @Override
                        public void close() {}
                    };
                }
            }
        );

        final var e = expectThrows(IndexShardSnapshotFailedException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("was deleted during snapshot"));
    }

    private static void removeIndexFromClusterState(ClusterService clusterService, ProjectId projectId, Index index) {
        clusterService.submitUnbatchedStateUpdateTask("remove-index", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return ClusterState.builder(currentState)
                    .putProjectMetadata(ProjectMetadata.builder(currentState.metadata().getProject(projectId)).remove(index.getName()))
                    .build();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });
    }
}
