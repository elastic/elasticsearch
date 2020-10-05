/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.test.ESIntegTestCase;

import java.nio.file.Path;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CloneSnapshotIT extends AbstractSnapshotIntegTestCase {

    public void testShardClone() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "repo-name";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "fs", repoPath);

        final boolean useBwCFormat = randomBoolean();
        if (useBwCFormat) {
            initWithSnapshotVersion(repoName, repoPath, SnapshotsService.OLD_SNAPSHOT_FORMAT);
            // Re-create repo to clear repository data cache
            assertAcked(client().admin().cluster().prepareDeleteRepository(repoName).get());
            createRepository(repoName, "fs", repoPath);
        }

        final String indexName = "test-index";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));
        final String sourceSnapshot = "source-snapshot";
        final SnapshotInfo sourceSnapshotInfo = createFullSnapshot(repoName, sourceSnapshot);

        final BlobStoreRepository repository =
                (BlobStoreRepository) internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(repoName);
        final RepositoryData repositoryData = getRepositoryData(repoName);
        final IndexId indexId = repositoryData.resolveIndexId(indexName);
        final int shardId = 0;
        final RepositoryShardId repositoryShardId = new RepositoryShardId(indexId, shardId);

        final SnapshotId targetSnapshotId = new SnapshotId("target-snapshot", UUIDs.randomBase64UUID(random()));

        final String currentShardGen;
        if (useBwCFormat) {
            currentShardGen = null;
        } else {
            currentShardGen = repositoryData.shardGenerations().getShardGen(indexId, shardId);
        }
        final String newShardGeneration = PlainActionFuture.get(f -> repository.cloneShardSnapshot(
                sourceSnapshotInfo.snapshotId(), targetSnapshotId, repositoryShardId, currentShardGen, f));

        if (useBwCFormat) {
            final long gen = Long.parseLong(newShardGeneration);
            assertEquals(gen, 1L); // Initial snapshot brought it to 0, clone increments it to 1
        }

        final BlobStoreIndexShardSnapshot targetShardSnapshot = readShardSnapshot(repository, repositoryShardId, targetSnapshotId);
        final BlobStoreIndexShardSnapshot sourceShardSnapshot =
                readShardSnapshot(repository, repositoryShardId, sourceSnapshotInfo.snapshotId());
        assertThat(targetShardSnapshot.incrementalFileCount(), is(0));
        final List<BlobStoreIndexShardSnapshot.FileInfo> sourceFiles = sourceShardSnapshot.indexFiles();
        final List<BlobStoreIndexShardSnapshot.FileInfo> targetFiles = targetShardSnapshot.indexFiles();
        final int fileCount = sourceFiles.size();
        assertEquals(fileCount, targetFiles.size());
        for (int i = 0; i < fileCount; i++) {
            assertTrue(sourceFiles.get(i).isSame(targetFiles.get(i)));
        }
        final BlobStoreIndexShardSnapshots shardMetadata = readShardGeneration(repository, repositoryShardId, newShardGeneration);
        final List<SnapshotFiles> snapshotFiles = shardMetadata.snapshots();
        assertThat(snapshotFiles, hasSize(2));
        assertTrue(snapshotFiles.get(0).isSame(snapshotFiles.get(1)));

        // verify that repeated cloning is idempotent
        final String newShardGeneration2 = PlainActionFuture.get(f -> repository.cloneShardSnapshot(
                sourceSnapshotInfo.snapshotId(), targetSnapshotId, repositoryShardId, newShardGeneration, f));
        assertEquals(newShardGeneration, newShardGeneration2);
    }

    private static BlobStoreIndexShardSnapshots readShardGeneration(BlobStoreRepository repository, RepositoryShardId repositoryShardId,
                                                                    String generation) {
        return PlainActionFuture.get(f -> repository.threadPool().generic().execute(ActionRunnable.supply(f,
                () -> BlobStoreRepository.INDEX_SHARD_SNAPSHOTS_FORMAT.read(repository.shardContainer(repositoryShardId.index(),
                        repositoryShardId.shardId()), generation, NamedXContentRegistry.EMPTY))));
    }

    private static BlobStoreIndexShardSnapshot readShardSnapshot(BlobStoreRepository repository, RepositoryShardId repositoryShardId,
                                                                 SnapshotId snapshotId) {
        return PlainActionFuture.get(f -> repository.threadPool().generic().execute(ActionRunnable.supply(f,
                () -> repository.loadShardSnapshot(repository.shardContainer(repositoryShardId.index(), repositoryShardId.shardId()),
                        snapshotId))));
    }
}
