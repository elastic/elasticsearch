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

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.repositories.RepositoryDataTests.generateRandomRepoData;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@link BlobStoreRepository} and its subclasses.
 */
public class BlobStoreRepositoryTests extends ESSingleNodeTestCase {

    public void testRetrieveSnapshots() throws Exception {
        final Client client = client();
        final Path location = ESIntegTestCase.randomRepoPath(node().settings());
        final String repositoryName = "test-repo";

        logger.info("-->  creating repository");
        PutRepositoryResponse putRepositoryResponse =
            client.admin().cluster().preparePutRepository(repositoryName)
                                    .setType("fs")
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
            client().prepareIndex(indexName, "type1", id).setSource("text", "sometext").get();
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
        @SuppressWarnings("unchecked") final BlobStoreRepository repository =
            (BlobStoreRepository) repositoriesService.repository(repositoryName);
        final List<SnapshotId> originalSnapshots = Arrays.asList(snapshotId1, snapshotId2);

        List<SnapshotId> snapshotIds = repository.getRepositoryData().getSnapshotIds().stream()
            .sorted((s1, s2) -> s1.getName().compareTo(s2.getName())).collect(Collectors.toList());
        assertThat(snapshotIds, equalTo(originalSnapshots));
    }

    public void testReadAndWriteSnapshotsThroughIndexFile() throws Exception {
        final BlobStoreRepository repository = setupRepo();

        // write to and read from a index file with no entries
        assertThat(repository.getRepositoryData().getSnapshotIds().size(), equalTo(0));
        final RepositoryData emptyData = RepositoryData.EMPTY;
        repository.writeIndexGen(emptyData, emptyData.getGenId());
        RepositoryData repoData = repository.getRepositoryData();
        assertEquals(repoData, emptyData);
        assertEquals(repoData.getIndices().size(), 0);
        assertEquals(repoData.getSnapshotIds().size(), 0);
        assertEquals(0L, repoData.getGenId());

        // write to and read from an index file with snapshots but no indices
        repoData = addRandomSnapshotsToRepoData(repoData, false);
        repository.writeIndexGen(repoData, repoData.getGenId());
        assertEquals(repoData, repository.getRepositoryData());

        // write to and read from a index file with random repository data
        repoData = addRandomSnapshotsToRepoData(repository.getRepositoryData(), true);
        repository.writeIndexGen(repoData, repoData.getGenId());
        assertEquals(repoData, repository.getRepositoryData());
    }

    public void testIndexGenerationalFiles() throws Exception {
        final BlobStoreRepository repository = setupRepo();

        // write to index generational file
        RepositoryData repositoryData = generateRandomRepoData();
        repository.writeIndexGen(repositoryData, repositoryData.getGenId());
        assertThat(repository.getRepositoryData(), equalTo(repositoryData));
        assertThat(repository.latestIndexBlobId(), equalTo(0L));
        assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(0L));

        // adding more and writing to a new index generational file
        repositoryData = addRandomSnapshotsToRepoData(repository.getRepositoryData(), true);
        repository.writeIndexGen(repositoryData, repositoryData.getGenId());
        assertEquals(repository.getRepositoryData(), repositoryData);
        assertThat(repository.latestIndexBlobId(), equalTo(1L));
        assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(1L));

        // removing a snapshot and writing to a new index generational file
        repositoryData = repository.getRepositoryData().removeSnapshot(repositoryData.getSnapshotIds().iterator().next());
        repository.writeIndexGen(repositoryData, repositoryData.getGenId());
        assertEquals(repository.getRepositoryData(), repositoryData);
        assertThat(repository.latestIndexBlobId(), equalTo(2L));
        assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(2L));
    }

    public void testRepositoryDataConcurrentModificationNotAllowed() throws IOException {
        final BlobStoreRepository repository = setupRepo();

        // write to index generational file
        RepositoryData repositoryData = generateRandomRepoData();
        repository.writeIndexGen(repositoryData, repositoryData.getGenId());

        // write repo data again to index generational file, errors because we already wrote to the
        // N+1 generation from which this repository data instance was created
        expectThrows(RepositoryException.class, () -> repository.writeIndexGen(repositoryData, repositoryData.getGenId()));
    }

    public void testReadAndWriteIncompatibleSnapshots() throws Exception {
        final BlobStoreRepository repository = setupRepo();

        // write to and read from incompatible snapshots file with no entries
        assertEquals(0, repository.getRepositoryData().getIncompatibleSnapshotIds().size());
        RepositoryData emptyData = RepositoryData.EMPTY;
        repository.writeIndexGen(emptyData, emptyData.getGenId());
        repository.writeIncompatibleSnapshots(emptyData);
        RepositoryData readData = repository.getRepositoryData();
        assertEquals(emptyData, readData);
        assertEquals(0, readData.getIndices().size());
        assertEquals(0, readData.getSnapshotIds().size());

        // write to and read from incompatible snapshots with some number of entries
        final int numSnapshots = randomIntBetween(1, 20);
        final List<SnapshotId> snapshotIds = new ArrayList<>(numSnapshots);
        for (int i = 0; i < numSnapshots; i++) {
            snapshotIds.add(new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID()));
        }
        RepositoryData repositoryData = new RepositoryData(readData.getGenId(),
            Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), snapshotIds);
        repository.blobContainer().deleteBlob("incompatible-snapshots");
        repository.writeIncompatibleSnapshots(repositoryData);
        readData = repository.getRepositoryData();
        assertEquals(repositoryData.getIncompatibleSnapshotIds(), readData.getIncompatibleSnapshotIds());
    }

    public void testIncompatibleSnapshotsBlobExists() throws Exception {
        final BlobStoreRepository repository = setupRepo();
        RepositoryData emptyData = RepositoryData.EMPTY;
        repository.writeIndexGen(emptyData, emptyData.getGenId());
        RepositoryData repoData = repository.getRepositoryData();
        assertEquals(emptyData, repoData);
        assertTrue(repository.blobContainer().blobExists("incompatible-snapshots"));
        repoData = addRandomSnapshotsToRepoData(repository.getRepositoryData(), true);
        repository.writeIndexGen(repoData, repoData.getGenId());
        assertEquals(0, repository.getRepositoryData().getIncompatibleSnapshotIds().size());
    }

    private BlobStoreRepository setupRepo() {
        final Client client = client();
        final Path location = ESIntegTestCase.randomRepoPath(node().settings());
        final String repositoryName = "test-repo";

        PutRepositoryResponse putRepositoryResponse =
            client.admin().cluster().preparePutRepository(repositoryName)
                                    .setType("fs")
                                    .setSettings(Settings.builder().put(node().settings()).put("location", location))
                                    .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        @SuppressWarnings("unchecked") final BlobStoreRepository repository =
            (BlobStoreRepository) repositoriesService.repository(repositoryName);
        return repository;
    }

    private RepositoryData addRandomSnapshotsToRepoData(RepositoryData repoData, boolean inclIndices) {
        int numSnapshots = randomIntBetween(1, 20);
        for (int i = 0; i < numSnapshots; i++) {
            SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
            int numIndices = inclIndices ? randomIntBetween(0, 20) : 0;
            List<IndexId> indexIds = new ArrayList<>(numIndices);
            for (int j = 0; j < numIndices; j++) {
                indexIds.add(new IndexId(randomAlphaOfLength(8), UUIDs.randomBase64UUID()));
            }
            repoData = repoData.addSnapshot(snapshotId,
                randomFrom(SnapshotState.SUCCESS, SnapshotState.PARTIAL, SnapshotState.FAILED), indexIds);
        }
        return repoData;
    }

}
