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

package org.elasticsearch.repositories.azure;


import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.microsoft.azure.storage.StorageException;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.cloud.azure.AbstractAzureTestCase;
import org.elasticsearch.cloud.azure.storage.AzureStorageService;
import org.elasticsearch.cloud.azure.storage.AzureStorageServiceImpl;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.repositories.azure.AzureRepository.Repository;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.store.MockFSDirectoryService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URISyntaxException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * This test needs Azure to run and -Dtests.thirdparty=true to be set
 * and -Dtests.config=/path/to/elasticsearch.yml
 * @see AbstractAzureTestCase
 */
@ESIntegTestCase.ClusterScope(
        scope = ESIntegTestCase.Scope.SUITE,
        numDataNodes = 1,
        transportClientRatio = 0.0)
public class AzureSnapshotRestoreTests extends AbstractAzureTestCase {

    private String getRepositoryPath() {
        String testName = "it-".concat(Strings.toUnderscoreCase(getTestName()).replaceAll("_", "-"));
        return testName.contains(" ") ? Strings.split(testName, " ")[0] : testName;
    }

    private static String getContainerName() {
        String testName = "snapshot-itest-".concat(RandomizedTest.getContext().getRunnerSeedAsString().toLowerCase(Locale.ROOT));
        return testName.contains(" ") ? Strings.split(testName, " ")[0] : testName;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                // In snapshot tests, we explicitly disable cloud discovery
                .put("discovery.type", "local")
                .build();
    }

    @Override
    public Settings indexSettings() {
        // During restore we frequently restore index to exactly the same state it was before, that might cause the same
        // checksum file to be written twice during restore operation
        return Settings.builder().put(super.indexSettings())
                .put(MockFSDirectoryService.RANDOM_PREVENT_DOUBLE_WRITE, false)
                .put(MockFSDirectoryService.RANDOM_NO_DELETE_OPEN_FILE, false)
                .build();
    }

    @Before @After
    public final void wipeAzureRepositories() throws StorageException, URISyntaxException {
        wipeRepositories();
        cleanRepositoryFiles(
            getContainerName(),
            getContainerName().concat("-1"),
            getContainerName().concat("-2"));
    }

    @Test
    public void testSimpleWorkflow() {
        Client client = client();
        logger.info("-->  creating azure repository with path [{}]", getRepositoryPath());
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("azure").setSettings(Settings.settingsBuilder()
                        .put(Repository.CONTAINER, getContainerName())
                        .put(Repository.BASE_PATH, getRepositoryPath())
                        .put(Repository.CHUNK_SIZE, randomIntBetween(1000, 10000), ByteSizeUnit.BYTES)
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-2", "doc", Integer.toString(i), "foo", "baz" + i);
            index("test-idx-3", "doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();
        assertThat(client.prepareCount("test-idx-1").get().getCount(), equalTo(100L));
        assertThat(client.prepareCount("test-idx-2").get().getCount(), equalTo(100L));
        assertThat(client.prepareCount("test-idx-3").get().getCount(), equalTo(100L));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx-*", "-test-idx-3").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> delete some data");
        for (int i = 0; i < 50; i++) {
            client.prepareDelete("test-idx-1", "doc", Integer.toString(i)).get();
        }
        for (int i = 50; i < 100; i++) {
            client.prepareDelete("test-idx-2", "doc", Integer.toString(i)).get();
        }
        for (int i = 0; i < 100; i += 2) {
            client.prepareDelete("test-idx-3", "doc", Integer.toString(i)).get();
        }
        refresh();
        assertThat(client.prepareCount("test-idx-1").get().getCount(), equalTo(50L));
        assertThat(client.prepareCount("test-idx-2").get().getCount(), equalTo(50L));
        assertThat(client.prepareCount("test-idx-3").get().getCount(), equalTo(50L));

        logger.info("--> close indices");
        client.admin().indices().prepareClose("test-idx-1", "test-idx-2").get();

        logger.info("--> restore all indices from the snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        ensureGreen();
        assertThat(client.prepareCount("test-idx-1").get().getCount(), equalTo(100L));
        assertThat(client.prepareCount("test-idx-2").get().getCount(), equalTo(100L));
        assertThat(client.prepareCount("test-idx-3").get().getCount(), equalTo(50L));

        // Test restore after index deletion
        logger.info("--> delete indices");
        cluster().wipeIndices("test-idx-1", "test-idx-2");
        logger.info("--> restore one index after deletion");
        restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx-*", "-test-idx-2").execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        ensureGreen();
        assertThat(client.prepareCount("test-idx-1").get().getCount(), equalTo(100L));
        ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
        assertThat(clusterState.getMetaData().hasIndex("test-idx-1"), equalTo(true));
        assertThat(clusterState.getMetaData().hasIndex("test-idx-2"), equalTo(false));
    }

    /**
     * For issue #51: https://github.com/elasticsearch/elasticsearch-cloud-azure/issues/51
     */
    @Test
    public void testMultipleSnapshots() throws URISyntaxException, StorageException {
        final String indexName = "test-idx-1";
        final String typeName = "doc";
        final String repositoryName = "test-repo";
        final String snapshot1Name = "test-snap-1";
        final String snapshot2Name = "test-snap-2";

        Client client = client();

        logger.info("creating index [{}]", indexName);
        createIndex(indexName);
        ensureGreen();

        logger.info("indexing first document");
        index(indexName, typeName, Integer.toString(1), "foo", "bar " + Integer.toString(1));
        refresh();
        assertThat(client.prepareCount(indexName).get().getCount(), equalTo(1L));

        logger.info("creating Azure repository with path [{}]", getRepositoryPath());
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository(repositoryName)
                .setType("azure").setSettings(Settings.settingsBuilder()
                                .put(Repository.CONTAINER, getContainerName())
                                .put(Repository.BASE_PATH, getRepositoryPath())
                                .put(Repository.BASE_PATH, randomIntBetween(1000, 10000), ByteSizeUnit.BYTES)
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("creating snapshot [{}]", snapshot1Name);
        CreateSnapshotResponse createSnapshotResponse1 = client.admin().cluster().prepareCreateSnapshot(repositoryName, snapshot1Name).setWaitForCompletion(true).setIndices(indexName).get();
        assertThat(createSnapshotResponse1.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse1.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse1.getSnapshotInfo().totalShards()));

        assertThat(client.admin().cluster().prepareGetSnapshots(repositoryName).setSnapshots(snapshot1Name).get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        logger.info("indexing second document");
        index(indexName, typeName, Integer.toString(2), "foo", "bar " + Integer.toString(2));
        refresh();
        assertThat(client.prepareCount(indexName).get().getCount(), equalTo(2L));

        logger.info("creating snapshot [{}]", snapshot2Name);
        CreateSnapshotResponse createSnapshotResponse2 = client.admin().cluster().prepareCreateSnapshot(repositoryName, snapshot2Name).setWaitForCompletion(true).setIndices(indexName).get();
        assertThat(createSnapshotResponse2.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse2.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse2.getSnapshotInfo().totalShards()));

        assertThat(client.admin().cluster().prepareGetSnapshots(repositoryName).setSnapshots(snapshot2Name).get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        logger.info("closing index [{}]", indexName);
        client.admin().indices().prepareClose(indexName).get();

        logger.info("attempting restore from snapshot [{}]", snapshot1Name);
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot(repositoryName, snapshot1Name).setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        ensureGreen();
        assertThat(client.prepareCount(indexName).get().getCount(), equalTo(1L));
    }

    @Test
    public void testMultipleRepositories() {
        Client client = client();
        logger.info("-->  creating azure repository with path [{}]", getRepositoryPath());
        PutRepositoryResponse putRepositoryResponse1 = client.admin().cluster().preparePutRepository("test-repo1")
                .setType("azure").setSettings(Settings.settingsBuilder()
                        .put(Repository.CONTAINER, getContainerName().concat("-1"))
                        .put(Repository.BASE_PATH, getRepositoryPath())
                        .put(Repository.CHUNK_SIZE, randomIntBetween(1000, 10000), ByteSizeUnit.BYTES)
                ).get();
        assertThat(putRepositoryResponse1.isAcknowledged(), equalTo(true));
        PutRepositoryResponse putRepositoryResponse2 = client.admin().cluster().preparePutRepository("test-repo2")
                .setType("azure").setSettings(Settings.settingsBuilder()
                        .put(Repository.CONTAINER, getContainerName().concat("-2"))
                        .put(Repository.BASE_PATH, getRepositoryPath())
                        .put(Repository.CHUNK_SIZE, randomIntBetween(1000, 10000), ByteSizeUnit.BYTES)
                ).get();
        assertThat(putRepositoryResponse2.isAcknowledged(), equalTo(true));

        createIndex("test-idx-1", "test-idx-2");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-2", "doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();
        assertThat(client.prepareCount("test-idx-1").get().getCount(), equalTo(100L));
        assertThat(client.prepareCount("test-idx-2").get().getCount(), equalTo(100L));

        logger.info("--> snapshot 1");
        CreateSnapshotResponse createSnapshotResponse1 = client.admin().cluster().prepareCreateSnapshot("test-repo1", "test-snap").setWaitForCompletion(true).setIndices("test-idx-1").get();
        assertThat(createSnapshotResponse1.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse1.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse1.getSnapshotInfo().totalShards()));

        logger.info("--> snapshot 2");
        CreateSnapshotResponse createSnapshotResponse2 = client.admin().cluster().prepareCreateSnapshot("test-repo2", "test-snap").setWaitForCompletion(true).setIndices("test-idx-2").get();
        assertThat(createSnapshotResponse2.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse2.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse2.getSnapshotInfo().totalShards()));

        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo1").setSnapshots("test-snap").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo2").setSnapshots("test-snap").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        // Test restore after index deletion
        logger.info("--> delete indices");
        cluster().wipeIndices("test-idx-1", "test-idx-2");
        logger.info("--> restore one index after deletion from snapshot 1");
        RestoreSnapshotResponse restoreSnapshotResponse1 = client.admin().cluster().prepareRestoreSnapshot("test-repo1", "test-snap").setWaitForCompletion(true).setIndices("test-idx-1").execute().actionGet();
        assertThat(restoreSnapshotResponse1.getRestoreInfo().totalShards(), greaterThan(0));
        ensureGreen();
        assertThat(client.prepareCount("test-idx-1").get().getCount(), equalTo(100L));
        ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
        assertThat(clusterState.getMetaData().hasIndex("test-idx-1"), equalTo(true));
        assertThat(clusterState.getMetaData().hasIndex("test-idx-2"), equalTo(false));

        logger.info("--> restore other index after deletion from snapshot 2");
        RestoreSnapshotResponse restoreSnapshotResponse2 = client.admin().cluster().prepareRestoreSnapshot("test-repo2", "test-snap").setWaitForCompletion(true).setIndices("test-idx-2").execute().actionGet();
        assertThat(restoreSnapshotResponse2.getRestoreInfo().totalShards(), greaterThan(0));
        ensureGreen();
        assertThat(client.prepareCount("test-idx-2").get().getCount(), equalTo(100L));
        clusterState = client.admin().cluster().prepareState().get().getState();
        assertThat(clusterState.getMetaData().hasIndex("test-idx-1"), equalTo(true));
        assertThat(clusterState.getMetaData().hasIndex("test-idx-2"), equalTo(true));
    }

    /**
     * For issue #26: https://github.com/elasticsearch/elasticsearch-cloud-azure/issues/26
     */
    @Test
    public void testListBlobs_26() throws StorageException, URISyntaxException {
        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-2", "doc", Integer.toString(i), "foo", "baz" + i);
            index("test-idx-3", "doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();

        ClusterAdminClient client = client().admin().cluster();
        logger.info("-->  creating azure repository without any path");
        PutRepositoryResponse putRepositoryResponse = client.preparePutRepository("test-repo").setType("azure")
                .setSettings(Settings.settingsBuilder()
                        .put(Repository.CONTAINER, getContainerName())
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        // Get all snapshots - should be empty
        assertThat(client.prepareGetSnapshots("test-repo").get().getSnapshots().size(), equalTo(0));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.prepareCreateSnapshot("test-repo", "test-snap-26").setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));

        // Get all snapshots - should have one
        assertThat(client.prepareGetSnapshots("test-repo").get().getSnapshots().size(), equalTo(1));

        // Clean the snapshot
        client.prepareDeleteSnapshot("test-repo", "test-snap-26").get();
        client.prepareDeleteRepository("test-repo").get();

        logger.info("-->  creating azure repository path [{}]", getRepositoryPath());
        putRepositoryResponse = client.preparePutRepository("test-repo").setType("azure")
                .setSettings(Settings.settingsBuilder()
                        .put(Repository.CONTAINER, getContainerName())
                        .put(Repository.BASE_PATH, getRepositoryPath())
        ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        // Get all snapshots - should be empty
        assertThat(client.prepareGetSnapshots("test-repo").get().getSnapshots().size(), equalTo(0));

        logger.info("--> snapshot");
        createSnapshotResponse = client.prepareCreateSnapshot("test-repo", "test-snap-26").setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));

        // Get all snapshots - should have one
        assertThat(client.prepareGetSnapshots("test-repo").get().getSnapshots().size(), equalTo(1));


    }

    /**
     * For issue #28: https://github.com/elasticsearch/elasticsearch-cloud-azure/issues/28
     */
    @Test
    public void testGetDeleteNonExistingSnapshot_28() throws StorageException, URISyntaxException {
        ClusterAdminClient client = client().admin().cluster();
        logger.info("-->  creating azure repository without any path");
        PutRepositoryResponse putRepositoryResponse = client.preparePutRepository("test-repo").setType("azure")
                .setSettings(Settings.settingsBuilder()
                        .put(Repository.CONTAINER, getContainerName())
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        try {
            client.prepareGetSnapshots("test-repo").addSnapshots("nonexistingsnapshotname").get();
            fail("Shouldn't be here");
        } catch (SnapshotMissingException ex) {
            // Expected
        }

        try {
            client.prepareDeleteSnapshot("test-repo", "nonexistingsnapshotname").get();
            fail("Shouldn't be here");
        } catch (SnapshotMissingException ex) {
            // Expected
        }
    }

    /**
     * For issue #21: https://github.com/elasticsearch/elasticsearch-cloud-azure/issues/21
     */
    @Test
    public void testForbiddenContainerName() throws Exception {
        checkContainerName("", false);
        checkContainerName("es", false);
        checkContainerName("-elasticsearch", false);
        checkContainerName("elasticsearch--integration", false);
        checkContainerName("elasticsearch_integration", false);
        checkContainerName("ElAsTicsearch_integration", false);
        checkContainerName("123456789-123456789-123456789-123456789-123456789-123456789-1234", false);
        checkContainerName("123456789-123456789-123456789-123456789-123456789-123456789-123", true);
        checkContainerName("elasticsearch-integration", true);
        checkContainerName("elasticsearch-integration-007", true);
    }

    /**
     * Create repository with wrong or correct container name
     * @param container Container name we want to create
     * @param correct Is this container name correct
     */
    private void checkContainerName(final String container, final boolean correct) throws Exception {
        logger.info("-->  creating azure repository with container name [{}]", container);
        // It could happen that we just removed from a previous test the same container so
        // we can not create it yet.
        assertBusy(new Runnable() {

            public void run() {
                try {
                    PutRepositoryResponse putRepositoryResponse = client().admin().cluster().preparePutRepository("test-repo")
                            .setType("azure").setSettings(Settings.settingsBuilder()
                                            .put(Repository.CONTAINER, container)
                                            .put(Repository.BASE_PATH, getRepositoryPath())
                                            .put(Repository.CHUNK_SIZE, randomIntBetween(1000, 10000), ByteSizeUnit.BYTES)
                            ).get();
                    client().admin().cluster().prepareDeleteRepository("test-repo").get();
                    try {
                        logger.info("--> remove container [{}]", container);
                        cleanRepositoryFiles(container);
                    } catch (StorageException | URISyntaxException e) {
                        // We can ignore that as we just try to clean after the test
                    }
                    assertTrue(putRepositoryResponse.isAcknowledged() == correct);
                } catch (RepositoryVerificationException e) {
                    if (correct) {
                        logger.debug(" -> container is being removed. Let's wait a bit...");
                        fail();
                    }
                }
            }
        }, 5, TimeUnit.MINUTES);
    }

    /**
     * Test case for issue #23: https://github.com/elasticsearch/elasticsearch-cloud-azure/issues/23
     */
    @Test
    public void testNonExistingRepo_23() {
        Client client = client();
        logger.info("-->  creating azure repository with path [{}]", getRepositoryPath());
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("azure").setSettings(Settings.settingsBuilder()
                        .put(Repository.CONTAINER, getContainerName())
                        .put(Repository.BASE_PATH, getRepositoryPath())
                        .put(Repository.CHUNK_SIZE, randomIntBetween(1000, 10000), ByteSizeUnit.BYTES)
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> restore non existing snapshot");
        try {
            client.admin().cluster().prepareRestoreSnapshot("test-repo", "no-existing-snapshot").setWaitForCompletion(true).execute().actionGet();
            fail("Shouldn't be here");
        } catch (SnapshotMissingException ex) {
            // Expected
        }
    }

    /**
     * When a user remove a container you can not immediately create it again.
     */
    @Test
    public void testRemoveAndCreateContainer() throws Exception {
        final String container = getContainerName().concat("-testremove");
        final AzureStorageService storageService = internalCluster().getInstance(AzureStorageService.class);

        // It could happen that we run this test really close to a previous one
        // so we might need some time to be able to create the container
        assertBusy(new Runnable() {

            public void run()  {
                try {
                    storageService.createContainer(container);
                    logger.debug(" -> container created...");
                } catch (URISyntaxException e) {
                    // Incorrect URL. This should never happen.
                    fail();
                } catch (StorageException e) {
                    // It could happen. Let's wait for a while.
                    logger.debug(" -> container is being removed. Let's wait a bit...");
                    fail();
                }
            }
        }, 30, TimeUnit.SECONDS);
        storageService.removeContainer(container);

        ClusterAdminClient client = client().admin().cluster();
        logger.info("-->  creating azure repository while container is being removed");
        try {
            client.preparePutRepository("test-repo").setType("azure")
                    .setSettings(Settings.settingsBuilder()
                            .put(Repository.CONTAINER, container)
                    ).get();
            fail("we should get a RepositoryVerificationException");
        } catch (RepositoryVerificationException e) {
            // Fine we expect that
        }
    }

    /**
     * Deletes repositories, supports wildcard notation.
     */
    public static void wipeRepositories(String... repositories) {
        // if nothing is provided, delete all
        if (repositories.length == 0) {
            repositories = new String[]{"*"};
        }
        for (String repository : repositories) {
            try {
                client().admin().cluster().prepareDeleteRepository(repository).execute().actionGet();
            } catch (RepositoryMissingException ex) {
                // ignore
            }
        }
    }

    /**
     * Purge the test containers
     */
    public void cleanRepositoryFiles(String... containers) throws StorageException, URISyntaxException {
        Settings settings = readSettingsFromFile();
        AzureStorageService client = new AzureStorageServiceImpl(settings);
        for (String container : containers) {
            client.removeContainer(container);
        }
    }
}
