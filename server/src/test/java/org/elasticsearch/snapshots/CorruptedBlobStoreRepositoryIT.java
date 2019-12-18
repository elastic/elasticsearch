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

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class CorruptedBlobStoreRepositoryIT extends AbstractSnapshotIntegTestCase {

    public void testConcurrentlyChangeRepositoryContents() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        final String repoName = "test-repo";
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository(repoName)
            .setType("fs").setSettings(Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(true,
            client().prepareIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar"));

        final String snapshot = "test-snap";

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot(repoName, snapshot)
            .setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> move index-N blob to next generation");
        final RepositoryData repositoryData =
            getRepositoryData(internalCluster().getMasterNodeInstance(RepositoriesService.class).repository(repoName));
        Files.move(repo.resolve("index-" + repositoryData.getGenId()), repo.resolve("index-" + (repositoryData.getGenId() + 1)));

        assertRepositoryBlocked(client, repoName, snapshot);

        if (randomBoolean()) {
            logger.info("--> move index-N blob back to initial generation");
            Files.move(repo.resolve("index-" + (repositoryData.getGenId() + 1)), repo.resolve("index-" + repositoryData.getGenId()));

            logger.info("--> verify repository remains blocked");
            assertRepositoryBlocked(client, repoName, snapshot);
        }

        logger.info("--> remove repository");
        assertAcked(client.admin().cluster().prepareDeleteRepository(repoName));

        logger.info("--> recreate repository");
        assertAcked(client.admin().cluster().preparePutRepository(repoName)
            .setType("fs").setSettings(Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        logger.info("--> delete snapshot");
        client.admin().cluster().prepareDeleteSnapshot(repoName, snapshot).get();

        logger.info("--> make sure snapshot doesn't exist");
        expectThrows(SnapshotMissingException.class, () -> client.admin().cluster().prepareGetSnapshots(repoName)
            .addSnapshots(snapshot).get().getSnapshots(repoName));
    }

    public void testConcurrentlyChangeRepositoryContentsInBwCMode() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        final String repoName = "test-repo";
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository(repoName)
            .setType("fs").setSettings(Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put(BlobStoreRepository.ALLOW_CONCURRENT_MODIFICATION.getKey(), true)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(true,
            client().prepareIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar"));

        final String snapshot = "test-snap";

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot(repoName, snapshot)
            .setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        final Repository repository = internalCluster().getMasterNodeInstance(RepositoriesService.class).repository(repoName);

        logger.info("--> move index-N blob to next generation");
        final RepositoryData repositoryData = getRepositoryData(repository);
        final long beforeMoveGen = repositoryData.getGenId();
        Files.move(repo.resolve("index-" + beforeMoveGen), repo.resolve("index-" + (beforeMoveGen + 1)));

        logger.info("--> verify index-N blob is found at the new location");
        assertThat(getRepositoryData(repository).getGenId(), is(beforeMoveGen + 1));

        logger.info("--> delete snapshot");
        client.admin().cluster().prepareDeleteSnapshot(repoName, snapshot).get();

        logger.info("--> verify index-N blob is found at the expected location");
        assertThat(getRepositoryData(repository).getGenId(), is(beforeMoveGen + 2));

        logger.info("--> make sure snapshot doesn't exist");
        expectThrows(SnapshotMissingException.class, () -> client.admin().cluster().prepareGetSnapshots(repoName)
            .addSnapshots(snapshot).get().getSnapshots(repoName));
    }

    public void testFindDanglingLatestGeneration() throws Exception {
        Path repo = randomRepoPath();
        final String repoName = "test-repo";
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client().admin().cluster().preparePutRepository(repoName)
            .setType("fs").setSettings(Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(true,
            client().prepareIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar"));

        final String snapshot = "test-snap";

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot(repoName, snapshot)
            .setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        final Repository repository = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(repoName);

        logger.info("--> move index-N blob to next generation");
        final RepositoryData repositoryData = getRepositoryData(repository);
        final long beforeMoveGen = repositoryData.getGenId();
        Files.move(repo.resolve("index-" + beforeMoveGen), repo.resolve("index-" + (beforeMoveGen + 1)));

        logger.info("--> set next generation as pending in the cluster state");
        final PlainActionFuture<Void> csUpdateFuture = PlainActionFuture.newFuture();
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).submitStateUpdateTask("set pending generation",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return ClusterState.builder(currentState).metaData(MetaData.builder(currentState.getMetaData())
                        .putCustom(RepositoriesMetaData.TYPE,
                            currentState.metaData().<RepositoriesMetaData>custom(RepositoriesMetaData.TYPE).withUpdatedGeneration(
                                repository.getMetadata().name(), beforeMoveGen, beforeMoveGen + 1)).build()).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    csUpdateFuture.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    csUpdateFuture.onResponse(null);
                }
            }
        );
        csUpdateFuture.get();

        logger.info("--> full cluster restart");
        internalCluster().fullRestart();
        ensureGreen();

        Repository repositoryAfterRestart = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(repoName);

        logger.info("--> verify index-N blob is found at the new location");
        assertThat(getRepositoryData(repositoryAfterRestart).getGenId(), is(beforeMoveGen + 1));

        logger.info("--> delete snapshot");
        client().admin().cluster().prepareDeleteSnapshot(repoName, snapshot).get();

        logger.info("--> verify index-N blob is found at the expected location");
        assertThat(getRepositoryData(repositoryAfterRestart).getGenId(), is(beforeMoveGen + 2));

        logger.info("--> make sure snapshot doesn't exist");
        expectThrows(SnapshotMissingException.class, () -> client().admin().cluster().prepareGetSnapshots(repoName)
            .addSnapshots(snapshot).get().getSnapshots(repoName));
    }

    private void assertRepositoryBlocked(Client client, String repo, String existingSnapshot) {
        logger.info("--> try to delete snapshot");
        final RepositoryException repositoryException3 = expectThrows(RepositoryException.class,
            () -> client.admin().cluster().prepareDeleteSnapshot(repo, existingSnapshot).execute().actionGet());
        assertThat(repositoryException3.getMessage(),
            containsString("Could not read repository data because the contents of the repository do not match its expected state."));

        logger.info("--> try to create snapshot");
        final RepositoryException repositoryException4 = expectThrows(RepositoryException.class,
            () -> client.admin().cluster().prepareCreateSnapshot(repo, existingSnapshot).execute().actionGet());
        assertThat(repositoryException4.getMessage(),
            containsString("Could not read repository data because the contents of the repository do not match its expected state."));
    }
}
