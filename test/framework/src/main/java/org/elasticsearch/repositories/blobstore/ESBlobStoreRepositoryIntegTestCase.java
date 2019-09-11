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

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration tests for {@link BlobStoreRepository} implementations.
 */
public abstract class ESBlobStoreRepositoryIntegTestCase extends ESIntegTestCase {

    protected abstract String repositoryType();

    protected Settings repositorySettings() {
        final Settings.Builder settings = Settings.builder();
        settings.put("compress", randomBoolean());
        if (randomBoolean()) {
            long size = 1 << randomInt(10);
            settings.put("chunk_size", new ByteSizeValue(size, ByteSizeUnit.KB));
        }
        return settings.build();
    }

    protected final String createRepository(final String name) {
        final boolean verify = randomBoolean();

        logger.debug("-->  creating repository [name: {}, verify: {}]", name, verify);
        assertAcked(client().admin().cluster().preparePutRepository(name)
            .setType(repositoryType())
            .setVerify(verify)
            .setSettings(repositorySettings()));

        internalCluster().getDataOrMasterNodeInstances(RepositoriesService.class).forEach(repositories -> {
            assertThat(repositories.repository(name), notNullValue());
            assertThat(repositories.repository(name), instanceOf(BlobStoreRepository.class));
            assertThat(repositories.repository(name).isReadOnly(), is(false));
            BlobStore blobStore = ((BlobStoreRepository) repositories.repository(name)).getBlobStore();
            assertThat("blob store has to be lazy initialized", blobStore, verify ? is(notNullValue()) : is(nullValue()));
        });

        return name;
    }

    public void testSnapshotAndRestore() throws Exception {
        final String repoName = createRepository(randomName());
        int indexCount = randomIntBetween(1, 5);
        int[] docCounts = new int[indexCount];
        String[] indexNames = generateRandomNames(indexCount);
        for (int i = 0; i < indexCount; i++) {
            docCounts[i] = iterations(10, 1000);
            logger.info("-->  create random index {} with {} records", indexNames[i], docCounts[i]);
            addRandomDocuments(indexNames[i], docCounts[i]);
            assertHitCount(client().prepareSearch(indexNames[i]).setSize(0).get(), docCounts[i]);
        }

        final String snapshotName = randomName();
        logger.info("-->  create snapshot {}:{}", repoName, snapshotName);
        assertSuccessfulSnapshot(client().admin().cluster().prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true).setIndices(indexNames));

        List<String> deleteIndices = randomSubsetOf(randomIntBetween(0, indexCount), indexNames);
        if (deleteIndices.size() > 0) {
            logger.info("-->  delete indices {}", deleteIndices);
            assertAcked(client().admin().indices().prepareDelete(deleteIndices.toArray(new String[deleteIndices.size()])));
        }

        Set<String> closeIndices = new HashSet<>(Arrays.asList(indexNames));
        closeIndices.removeAll(deleteIndices);

        if (closeIndices.size() > 0) {
            for (String index : closeIndices) {
                if (randomBoolean()) {
                    logger.info("--> add random documents to {}", index);
                    addRandomDocuments(index, randomIntBetween(10, 1000));
                } else {
                    int docCount = (int) client().prepareSearch(index).setSize(0).get().getHits().getTotalHits().value;
                    int deleteCount = randomIntBetween(1, docCount);
                    logger.info("--> delete {} random documents from {}", deleteCount, index);
                    for (int i = 0; i < deleteCount; i++) {
                        int doc = randomIntBetween(0, docCount - 1);
                        client().prepareDelete(index, index, Integer.toString(doc)).get();
                    }
                    client().admin().indices().prepareRefresh(index).get();
                }
            }

            // Wait for green so the close does not fail in the edge case of coinciding with a shard recovery that hasn't fully synced yet
            ensureGreen();
            logger.info("-->  close indices {}", closeIndices);
            assertAcked(client().admin().indices().prepareClose(closeIndices.toArray(new String[closeIndices.size()])));
        }

        logger.info("--> restore all indices from the snapshot");
        assertSuccessfulRestore(client().admin().cluster().prepareRestoreSnapshot(repoName, snapshotName).setWaitForCompletion(true));

        ensureGreen();

        for (int i = 0; i < indexCount; i++) {
            assertHitCount(client().prepareSearch(indexNames[i]).setSize(0).get(), docCounts[i]);
        }

        logger.info("-->  delete snapshot {}:{}", repoName, snapshotName);
        assertAcked(client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName).get());

        expectThrows(SnapshotMissingException.class, () ->
            client().admin().cluster().prepareGetSnapshots(repoName).setSnapshots(snapshotName).get().getSnapshots(repoName));

        expectThrows(SnapshotMissingException.class, () ->
            client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName).get());

        expectThrows(SnapshotRestoreException.class, () ->
            client().admin().cluster().prepareRestoreSnapshot(repoName, snapshotName).setWaitForCompletion(randomBoolean()).get());
    }

    public void testMultipleSnapshotAndRollback() throws Exception {
        final String repoName = createRepository(randomName());
        int iterationCount = randomIntBetween(2, 5);
        int[] docCounts = new int[iterationCount];
        String indexName = randomName();
        String snapshotName = randomName();
        assertAcked(client().admin().indices().prepareCreate(indexName).get());
        for (int i = 0; i < iterationCount; i++) {
            if (randomBoolean() && i > 0) { // don't delete on the first iteration
                int docCount = docCounts[i - 1];
                if (docCount > 0) {
                    int deleteCount = randomIntBetween(1, docCount);
                    logger.info("--> delete {} random documents from {}", deleteCount, indexName);
                    for (int j = 0; j < deleteCount; j++) {
                        int doc = randomIntBetween(0, docCount - 1);
                        client().prepareDelete(indexName, indexName, Integer.toString(doc)).get();
                    }
                    client().admin().indices().prepareRefresh(indexName).get();
                }
            } else {
                int docCount = randomIntBetween(10, 1000);
                logger.info("--> add {} random documents to {}", docCount, indexName);
                addRandomDocuments(indexName, docCount);
            }
            // Check number of documents in this iteration
            docCounts[i] = (int) client().prepareSearch(indexName).setSize(0).get().getHits().getTotalHits().value;
            logger.info("-->  create snapshot {}:{} with {} documents", repoName, snapshotName + "-" + i, docCounts[i]);
            assertSuccessfulSnapshot(client().admin().cluster().prepareCreateSnapshot(repoName, snapshotName + "-" + i)
                    .setWaitForCompletion(true).setIndices(indexName));
        }

        int restoreOperations = randomIntBetween(1, 3);
        for (int i = 0; i < restoreOperations; i++) {
            int iterationToRestore = randomIntBetween(0, iterationCount - 1);
            logger.info("-->  performing restore of the iteration {}", iterationToRestore);

            // Wait for green so the close does not fail in the edge case of coinciding with a shard recovery that hasn't fully synced yet
            ensureGreen();
            logger.info("-->  close index");
            assertAcked(client().admin().indices().prepareClose(indexName));

            logger.info("--> restore index from the snapshot");
            assertSuccessfulRestore(client().admin().cluster().prepareRestoreSnapshot(repoName, snapshotName + "-" + iterationToRestore)
                    .setWaitForCompletion(true));

            ensureGreen();
            assertHitCount(client().prepareSearch(indexName).setSize(0).get(), docCounts[iterationToRestore]);
        }

        for (int i = 0; i < iterationCount; i++) {
            logger.info("-->  delete snapshot {}:{}", repoName, snapshotName + "-" + i);
            assertAcked(client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName + "-" + i).get());
        }
    }

    public void testIndicesDeletedFromRepository() throws Exception {
        final String repoName = createRepository("test-repo");
        Client client = client();
        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 20; i++) {
            index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-2", "doc", Integer.toString(i), "foo", "baz" + i);
            index("test-idx-3", "doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();

        logger.info("--> take a snapshot");
        CreateSnapshotResponse createSnapshotResponse =
            client.admin().cluster().prepareCreateSnapshot(repoName, "test-snap").setWaitForCompletion(true).get();
        assertEquals(createSnapshotResponse.getSnapshotInfo().successfulShards(), createSnapshotResponse.getSnapshotInfo().totalShards());

        logger.info("--> indexing more data");
        for (int i = 20; i < 40; i++) {
            index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-2", "doc", Integer.toString(i), "foo", "baz" + i);
            index("test-idx-3", "doc", Integer.toString(i), "foo", "baz" + i);
        }

        logger.info("--> take another snapshot with only 2 of the 3 indices");
        createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot(repoName, "test-snap2")
                                     .setWaitForCompletion(true)
                                     .setIndices("test-idx-1", "test-idx-2")
                                     .get();
        assertEquals(createSnapshotResponse.getSnapshotInfo().successfulShards(), createSnapshotResponse.getSnapshotInfo().totalShards());

        logger.info("--> delete a snapshot");
        assertAcked(client().admin().cluster().prepareDeleteSnapshot(repoName, "test-snap").get());

        logger.info("--> verify index folder deleted from blob container");
        RepositoriesService repositoriesSvc = internalCluster().getInstance(RepositoriesService.class, internalCluster().getMasterName());
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, internalCluster().getMasterName());
        BlobStoreRepository repository = (BlobStoreRepository) repositoriesSvc.repository(repoName);

        final SetOnce<BlobContainer> indicesBlobContainer = new SetOnce<>();
        final SetOnce<RepositoryData> repositoryData = new SetOnce<>();
        final CountDownLatch latch = new CountDownLatch(1);
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
            indicesBlobContainer.set(repository.blobStore().blobContainer(repository.basePath().add("indices")));
            repositoryData.set(repository.getRepositoryData());
            latch.countDown();
        });

        latch.await();
        for (IndexId indexId : repositoryData.get().getIndices().values()) {
            if (indexId.getName().equals("test-idx-3")) {
                assertFalse(BlobStoreTestUtil.blobExists(indicesBlobContainer.get(), indexId.getId())); // deleted index
            }
        }
    }

    protected void addRandomDocuments(String name, int numDocs) throws ExecutionException, InterruptedException {
        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            indexRequestBuilders[i] = client().prepareIndex(name, name, Integer.toString(i))
                .setRouting(randomAlphaOfLength(randomIntBetween(1, 10))).setSource("field", "value");
        }
        indexRandom(true, indexRequestBuilders);
    }

    private String[] generateRandomNames(int num) {
        Set<String> names = new HashSet<>();
        for (int i = 0; i < num; i++) {
            String name;
            do {
                name = randomName();
            } while (names.contains(name));
            names.add(name);
        }
        return names.toArray(new String[num]);
    }

    protected static void assertSuccessfulSnapshot(CreateSnapshotRequestBuilder requestBuilder) {
        CreateSnapshotResponse response = requestBuilder.get();
        assertSuccessfulSnapshot(response);
    }

    private static void assertSuccessfulSnapshot(CreateSnapshotResponse response) {
        assertThat(response.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(response.getSnapshotInfo().successfulShards(), equalTo(response.getSnapshotInfo().totalShards()));
    }

    private static void assertSuccessfulRestore(RestoreSnapshotRequestBuilder requestBuilder) {
        RestoreSnapshotResponse response = requestBuilder.get();
        assertSuccessfulRestore(response);
    }

    private static void assertSuccessfulRestore(RestoreSnapshotResponse response) {
        assertThat(response.getRestoreInfo().successfulShards(), greaterThan(0));
        assertThat(response.getRestoreInfo().successfulShards(), equalTo(response.getRestoreInfo().totalShards()));
    }

    protected static String randomName() {
        return randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
    }
}
