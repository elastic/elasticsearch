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

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.repositories.RepositoryDataTests.generateRandomRepoData;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for the {@link BlobStoreRepository} and its subclasses.
 */
public class BlobStoreRepositoryTests extends ESSingleNodeTestCase {

    static final String REPO_TYPE = "fsLike";

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(FsLikeRepoPlugin.class);
    }

    // the reason for this plug-in is to drop any assertSnapshotOrGenericThread as mostly all access in this test goes from test threads
    public static class FsLikeRepoPlugin extends Plugin implements RepositoryPlugin {

        @Override
        public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry,
                                                               ClusterService clusterService, RecoverySettings recoverySettings) {
            return Collections.singletonMap(REPO_TYPE,
                (metadata) -> new FsRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings) {
                    @Override
                    protected void assertSnapshotOrGenericThread() {
                        // eliminate thread name check as we access blobStore on test/main threads
                    }
                });
        }
    }

    public void testRetrieveSnapshots() throws Exception {
        final Client client = client();
        final Path location = ESIntegTestCase.randomRepoPath(node().settings());
        final String repositoryName = "test-repo";

        logger.info("-->  creating repository");
        AcknowledgedResponse putRepositoryResponse =
            client.admin().cluster().preparePutRepository(repositoryName)
                                    .setType(REPO_TYPE)
                                    .setSettings(Settings.builder().put(node().settings()).put("location", location))
                                    .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> creating an index and indexing documents");
        final String indexName = "test-idx";
        createIndex(indexName);
        ensureGreen();
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            client().prepareIndex(indexName).setId(id).setSource("text", "sometext").get();
        }
        client().admin().indices().prepareFlush(indexName).get();

        logger.info("--> create first snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
                                                              .cluster()
                                                              .prepareCreateSnapshot(repositoryName, "test-snap-1")
                                                              .setWaitForCompletion(true)
                                                              .setIndices(indexName)
                                                              .get();
        final SnapshotId snapshotId1 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        logger.info("--> create second snapshot");
        createSnapshotResponse = client.admin()
                                       .cluster()
                                       .prepareCreateSnapshot(repositoryName, "test-snap-2")
                                       .setWaitForCompletion(true)
                                       .setIndices(indexName)
                                       .get();
        final SnapshotId snapshotId2 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        logger.info("--> make sure the node's repository can resolve the snapshots");
        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        final BlobStoreRepository repository =
            (BlobStoreRepository) repositoriesService.repository(repositoryName);
        final List<SnapshotId> originalSnapshots = Arrays.asList(snapshotId1, snapshotId2);

        List<SnapshotId> snapshotIds = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository).getSnapshotIds().stream()
            .sorted((s1, s2) -> s1.getName().compareTo(s2.getName())).collect(Collectors.toList());
        assertThat(snapshotIds, equalTo(originalSnapshots));
    }

    public void testReadAndWriteSnapshotsThroughIndexFile() throws Exception {
        final BlobStoreRepository repository = setupRepo();
        final long pendingGeneration = repository.metadata.pendingGeneration();
        // write to and read from a index file with no entries
        assertThat(ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository).getSnapshotIds().size(), equalTo(0));
        final RepositoryData emptyData = RepositoryData.EMPTY;
        writeIndexGen(repository, emptyData, emptyData.getGenId());
        RepositoryData repoData = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        assertEquals(repoData, emptyData);
        assertEquals(repoData.getIndices().size(), 0);
        assertEquals(repoData.getSnapshotIds().size(), 0);
        assertEquals(pendingGeneration + 1L, repoData.getGenId());

        // write to and read from an index file with snapshots but no indices
        repoData = addRandomSnapshotsToRepoData(repoData, false);
        writeIndexGen(repository, repoData, repoData.getGenId());
        assertEquals(repoData, ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository));

        // write to and read from a index file with random repository data
        repoData = addRandomSnapshotsToRepoData(ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), true);
        writeIndexGen(repository, repoData, repoData.getGenId());
        assertEquals(repoData, ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository));
    }

    public void testIndexGenerationalFiles() throws Exception {
        final BlobStoreRepository repository = setupRepo();
        assertEquals(ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), RepositoryData.EMPTY);

        final long pendingGeneration = repository.metadata.pendingGeneration();

        // write to index generational file
        RepositoryData repositoryData = generateRandomRepoData();
        writeIndexGen(repository, repositoryData, RepositoryData.EMPTY_REPO_GEN);
        assertThat(ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), equalTo(repositoryData));
        final long expectedGeneration = pendingGeneration + 1L;
        assertThat(repository.latestIndexBlobId(), equalTo(expectedGeneration));
        assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(expectedGeneration));

        // adding more and writing to a new index generational file
        repositoryData = addRandomSnapshotsToRepoData(ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), true);
        writeIndexGen(repository, repositoryData, repositoryData.getGenId());
        assertEquals(ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), repositoryData);
        assertThat(repository.latestIndexBlobId(), equalTo(expectedGeneration + 1L));
        assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(expectedGeneration + 1L));

        // removing a snapshot and writing to a new index generational file
        repositoryData = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository).removeSnapshots(
            Collections.singleton(repositoryData.getSnapshotIds().iterator().next()), ShardGenerations.EMPTY);
        writeIndexGen(repository, repositoryData, repositoryData.getGenId());
        assertEquals(ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), repositoryData);
        assertThat(repository.latestIndexBlobId(), equalTo(expectedGeneration + 2L));
        assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(expectedGeneration + 2L));
    }

    public void testRepositoryDataConcurrentModificationNotAllowed() {
        final BlobStoreRepository repository = setupRepo();

        // write to index generational file
        RepositoryData repositoryData = generateRandomRepoData();
        final long startingGeneration = repositoryData.getGenId();
        final PlainActionFuture<RepositoryData> future1 = PlainActionFuture.newFuture();
        repository.writeIndexGen(repositoryData, startingGeneration, Version.CURRENT, Function.identity(),future1);

        // write repo data again to index generational file, errors because we already wrote to the
        // N+1 generation from which this repository data instance was created
        expectThrows(RepositoryException.class,
            () -> writeIndexGen(repository, repositoryData.withGenId(startingGeneration + 1), repositoryData.getGenId()));
    }

    public void testBadChunksize() throws Exception {
        final Client client = client();
        final Path location = ESIntegTestCase.randomRepoPath(node().settings());
        final String repositoryName = "test-repo";

        expectThrows(RepositoryException.class, () ->
            client.admin().cluster().preparePutRepository(repositoryName)
                .setType(REPO_TYPE)
                .setSettings(Settings.builder().put(node().settings())
                    .put("location", location)
                    .put("chunk_size", randomLongBetween(-10, 0), ByteSizeUnit.BYTES))
                .get());
    }

    private static void writeIndexGen(BlobStoreRepository repository, RepositoryData repositoryData, long generation) throws Exception {
        PlainActionFuture.<RepositoryData, Exception>get(
                f -> repository.writeIndexGen(repositoryData, generation, Version.CURRENT, Function.identity(), f));
    }

    private BlobStoreRepository setupRepo() {
        final Client client = client();
        final Path location = ESIntegTestCase.randomRepoPath(node().settings());
        final String repositoryName = "test-repo";

        Settings.Builder repoSettings = Settings.builder().put(node().settings()).put("location", location);
        boolean compress = randomBoolean();
        if (compress == false) {
            repoSettings.put(BlobStoreRepository.COMPRESS_SETTING.getKey(), false);
        }
        AcknowledgedResponse putRepositoryResponse =
            client.admin().cluster().preparePutRepository(repositoryName)
                                    .setType(REPO_TYPE)
                                    .setSettings(repoSettings)
                                    .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        final BlobStoreRepository repository =
            (BlobStoreRepository) repositoriesService.repository(repositoryName);
        assertThat("getBlobContainer has to be lazy initialized", repository.getBlobContainer(), nullValue());
        assertEquals("Compress must be set to", compress, repository.isCompress());
        return repository;
    }

    private RepositoryData addRandomSnapshotsToRepoData(RepositoryData repoData, boolean inclIndices) {
        int numSnapshots = randomIntBetween(1, 20);
        for (int i = 0; i < numSnapshots; i++) {
            SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
            int numIndices = inclIndices ? randomIntBetween(0, 20) : 0;
            final ShardGenerations.Builder builder = ShardGenerations.builder();
            for (int j = 0; j < numIndices; j++) {
                builder.put(new IndexId(randomAlphaOfLength(8), UUIDs.randomBase64UUID()), 0, "1");
            }
            final ShardGenerations shardGenerations = builder.build();
            final Map<IndexId, String> indexLookup =
                shardGenerations.indices().stream().collect(Collectors.toMap(Function.identity(), ind -> randomAlphaOfLength(256)));
            repoData = repoData.addSnapshot(snapshotId,
                randomFrom(SnapshotState.SUCCESS, SnapshotState.PARTIAL, SnapshotState.FAILED), Version.CURRENT, shardGenerations,
                indexLookup,
                indexLookup.values().stream().collect(Collectors.toMap(Function.identity(), ignored -> UUIDs.randomBase64UUID(random()))));
        }
        return repoData;
    }

}
