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
import com.microsoft.azure.storage.LocationMode;
import com.microsoft.azure.storage.StorageException;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.cloud.azure.storage.AzureStorageService;
import org.elasticsearch.cloud.azure.storage.AzureStorageServiceImpl;
import org.elasticsearch.cloud.azure.storage.AzureStorageSettings;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.plugin.repository.azure.AzureRepositoryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.repositories.azure.AzureRepository.Repository;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.ThirdParty;
import org.elasticsearch.test.store.MockFSDirectoryService;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cloud.azure.AzureTestUtils.generateMockSecureSettings;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Those integration tests need an Azure access and must be run with
 * {@code -Dtests.thirdparty=true -Dtests.azure.account=AzureStorageAccount -Dtests.azure.key=AzureStorageKey}
 * options
 */
@ClusterScope(
        scope = ESIntegTestCase.Scope.SUITE,
        supportsDedicatedMasters = false, numDataNodes = 1,
        transportClientRatio = 0.0)
@ThirdParty
public class AzureSnapshotRestoreTests extends ESBlobStoreRepositoryIntegTestCase {

    private static Settings.Builder generateMockSettings() {
        return Settings.builder().setSecureSettings(generateMockSecureSettings());
    }

    private static AzureStorageService getAzureStorageService() {
        return new AzureStorageServiceImpl(generateMockSettings().build(),
            AzureStorageSettings.load(generateMockSettings().build()));
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return generateMockSettings()
            .put(super.nodeSettings(nodeOrdinal))
            .build();
    }

    private static String getContainerName() {
        /* Have a different name per test so that there is no possible race condition. As the long can be negative,
         * there mustn't be a hyphen between the 2 concatenated numbers
         * (can't have 2 consecutives hyphens on Azure containers)
         */
        String testName = "snapshot-itest-"
            .concat(RandomizedTest.getContext().getRunnerSeedAsString().toLowerCase(Locale.ROOT));
        return testName.contains(" ") ? Strings.split(testName, " ")[0] : testName;
    }

    @BeforeClass
    public static void createTestContainers() throws Exception {
        createTestContainer(getContainerName());
        // This is needed for testMultipleRepositories() test case
        createTestContainer(getContainerName() + "-1");
        createTestContainer(getContainerName() + "-2");
    }

    @AfterClass
    public static void removeContainer() throws Exception {
        removeTestContainer(getContainerName());
        // This is needed for testMultipleRepositories() test case
        removeTestContainer(getContainerName() + "-1");
        removeTestContainer(getContainerName() + "-2");
    }

    /**
     * Create a test container in Azure
     * @param containerName container name to use
     */
    private static void createTestContainer(String containerName) throws Exception {
        // It could happen that we run this test really close to a previous one
        // so we might need some time to be able to create the container
        assertBusy(() -> {
            getAzureStorageService().createContainer("default", LocationMode.PRIMARY_ONLY, containerName);
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Remove a test container in Azure
     * @param containerName container name to use
     */
    private static void removeTestContainer(String containerName) throws URISyntaxException, StorageException {
        getAzureStorageService().removeContainer("default", LocationMode.PRIMARY_ONLY, containerName);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(AzureRepositoryPlugin.class, MockFSIndexStore.TestPlugin.class);
    }

    private String getRepositoryPath() {
        String testName = "it-" + getTestName();
        return testName.contains(" ") ? Strings.split(testName, " ")[0] : testName;
    }

    @Override
    public Settings indexSettings() {
        // During restore we frequently restore index to exactly the same state it was before, that might cause the same
        // checksum file to be written twice during restore operation
        return Settings.builder().put(super.indexSettings())
                .put(MockFSDirectoryService.RANDOM_PREVENT_DOUBLE_WRITE_SETTING.getKey(), false)
                .put(MockFSDirectoryService.RANDOM_NO_DELETE_OPEN_FILE_SETTING.getKey(), false)
                .build();
    }

    @After
    public final void wipeAzureRepositories() {
        try {
            client().admin().cluster().prepareDeleteRepository("*").get();
        } catch (RepositoryMissingException ignored) {
        }
    }

    public void testMultipleRepositories() {
        Client client = client();
        logger.info("-->  creating azure repository with path [{}]", getRepositoryPath());
        PutRepositoryResponse putRepositoryResponse1 = client.admin().cluster().preparePutRepository("test-repo1")
                .setType("azure").setSettings(Settings.builder()
                        .put(Repository.CONTAINER_SETTING.getKey(), getContainerName().concat("-1"))
                        .put(Repository.BASE_PATH_SETTING.getKey(), getRepositoryPath())
                        .put(Repository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(1000, 10000), ByteSizeUnit.BYTES)
                ).get();
        assertThat(putRepositoryResponse1.isAcknowledged(), equalTo(true));
        PutRepositoryResponse putRepositoryResponse2 = client.admin().cluster().preparePutRepository("test-repo2")
                .setType("azure").setSettings(Settings.builder()
                        .put(Repository.CONTAINER_SETTING.getKey(), getContainerName().concat("-2"))
                        .put(Repository.BASE_PATH_SETTING.getKey(), getRepositoryPath())
                        .put(Repository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(1000, 10000), ByteSizeUnit.BYTES)
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
        assertThat(client.prepareSearch("test-idx-1").setSize(0).get().getHits().getTotalHits(), equalTo(100L));
        assertThat(client.prepareSearch("test-idx-2").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        logger.info("--> snapshot 1");
        CreateSnapshotResponse createSnapshotResponse1 = client.admin().cluster().prepareCreateSnapshot("test-repo1", "test-snap")
            .setWaitForCompletion(true).setIndices("test-idx-1").get();
        assertThat(createSnapshotResponse1.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse1.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse1.getSnapshotInfo().totalShards()));

        logger.info("--> snapshot 2");
        CreateSnapshotResponse createSnapshotResponse2 = client.admin().cluster().prepareCreateSnapshot("test-repo2", "test-snap")
            .setWaitForCompletion(true).setIndices("test-idx-2").get();
        assertThat(createSnapshotResponse2.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse2.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse2.getSnapshotInfo().totalShards()));

        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo1").setSnapshots("test-snap").get().getSnapshots().get(0).state(),
            equalTo(SnapshotState.SUCCESS));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo2").setSnapshots("test-snap").get().getSnapshots().get(0).state(),
            equalTo(SnapshotState.SUCCESS));

        // Test restore after index deletion
        logger.info("--> delete indices");
        cluster().wipeIndices("test-idx-1", "test-idx-2");
        logger.info("--> restore one index after deletion from snapshot 1");
        RestoreSnapshotResponse restoreSnapshotResponse1 = client.admin().cluster().prepareRestoreSnapshot("test-repo1", "test-snap")
            .setWaitForCompletion(true).setIndices("test-idx-1").get();
        assertThat(restoreSnapshotResponse1.getRestoreInfo().totalShards(), greaterThan(0));
        ensureGreen();
        assertThat(client.prepareSearch("test-idx-1").setSize(0).get().getHits().getTotalHits(), equalTo(100L));
        ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
        assertThat(clusterState.getMetaData().hasIndex("test-idx-1"), equalTo(true));
        assertThat(clusterState.getMetaData().hasIndex("test-idx-2"), equalTo(false));

        logger.info("--> restore other index after deletion from snapshot 2");
        RestoreSnapshotResponse restoreSnapshotResponse2 = client.admin().cluster().prepareRestoreSnapshot("test-repo2", "test-snap")
            .setWaitForCompletion(true).setIndices("test-idx-2").get();
        assertThat(restoreSnapshotResponse2.getRestoreInfo().totalShards(), greaterThan(0));
        ensureGreen();
        assertThat(client.prepareSearch("test-idx-2").setSize(0).get().getHits().getTotalHits(), equalTo(100L));
        clusterState = client.admin().cluster().prepareState().get().getState();
        assertThat(clusterState.getMetaData().hasIndex("test-idx-1"), equalTo(true));
        assertThat(clusterState.getMetaData().hasIndex("test-idx-2"), equalTo(true));
    }

    /**
     * For issue #26: https://github.com/elastic/elasticsearch-cloud-azure/issues/26
     */
    public void testListBlobs_26() throws StorageException, URISyntaxException {
        final String repositoryName="test-repo-26";
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
        PutRepositoryResponse putRepositoryResponse = client.preparePutRepository(repositoryName).setType("azure")
                .setSettings(Settings.builder()
                        .put(Repository.CONTAINER_SETTING.getKey(), getContainerName())
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        // Get all snapshots - should be empty
        assertThat(client.prepareGetSnapshots(repositoryName).get().getSnapshots().size(), equalTo(0));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.prepareCreateSnapshot(repositoryName, "test-snap-26")
            .setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));

        // Get all snapshots - should have one
        assertThat(client.prepareGetSnapshots(repositoryName).get().getSnapshots().size(), equalTo(1));

        // Clean the snapshot
        client.prepareDeleteSnapshot(repositoryName, "test-snap-26").get();
        client.prepareDeleteRepository(repositoryName).get();

        logger.info("-->  creating azure repository path [{}]", getRepositoryPath());
        putRepositoryResponse = client.preparePutRepository(repositoryName).setType("azure")
                .setSettings(Settings.builder()
                        .put(Repository.CONTAINER_SETTING.getKey(), getContainerName())
                        .put(Repository.BASE_PATH_SETTING.getKey(), getRepositoryPath())
        ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        // Get all snapshots - should be empty
        assertThat(client.prepareGetSnapshots(repositoryName).get().getSnapshots().size(), equalTo(0));

        logger.info("--> snapshot");
        createSnapshotResponse = client.prepareCreateSnapshot(repositoryName, "test-snap-26").setWaitForCompletion(true)
            .setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));

        // Get all snapshots - should have one
        assertThat(client.prepareGetSnapshots(repositoryName).get().getSnapshots().size(), equalTo(1));
    }

    /**
     * For issue #28: https://github.com/elastic/elasticsearch-cloud-azure/issues/28
     */
    public void testGetDeleteNonExistingSnapshot_28() throws StorageException, URISyntaxException {
        final String repositoryName="test-repo-28";
        ClusterAdminClient client = client().admin().cluster();
        logger.info("-->  creating azure repository without any path");
        PutRepositoryResponse putRepositoryResponse = client.preparePutRepository(repositoryName).setType("azure")
                .setSettings(Settings.builder()
                        .put(Repository.CONTAINER_SETTING.getKey(), getContainerName())
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        try {
            client.prepareGetSnapshots(repositoryName).addSnapshots("nonexistingsnapshotname").get();
            fail("Shouldn't be here");
        } catch (SnapshotMissingException ex) {
            // Expected
        }

        try {
            client.prepareDeleteSnapshot(repositoryName, "nonexistingsnapshotname").get();
            fail("Shouldn't be here");
        } catch (SnapshotMissingException ex) {
            // Expected
        }
    }

    /**
     * Test case for issue #23: https://github.com/elastic/elasticsearch-cloud-azure/issues/23
     */
    public void testNonExistingRepo_23() {
        final String repositoryName = "test-repo-test23";
        Client client = client();
        logger.info("-->  creating azure repository with path [{}]", getRepositoryPath());
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository(repositoryName)
                .setType("azure").setSettings(Settings.builder()
                        .put(Repository.CONTAINER_SETTING.getKey(), getContainerName())
                        .put(Repository.BASE_PATH_SETTING.getKey(), getRepositoryPath())
                        .put(Repository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(1000, 10000), ByteSizeUnit.BYTES)
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> restore non existing snapshot");
        try {
            client.admin().cluster().prepareRestoreSnapshot(repositoryName, "no-existing-snapshot").setWaitForCompletion(true).get();
            fail("Shouldn't be here");
        } catch (SnapshotRestoreException ex) {
            // Expected
        }
    }

    /**
     * When a user remove a container you can not immediately create it again.
     */
    public void testRemoveAndCreateContainer() throws Exception {
        final String container = getContainerName().concat("-testremove");

        createTestContainer(container);
        removeTestContainer(container);

        ClusterAdminClient client = client().admin().cluster();
        logger.info("-->  creating azure repository while container is being removed");
        try {
            client.preparePutRepository("test-repo").setType("azure")
                    .setSettings(Settings.builder()
                            .put(Repository.CONTAINER_SETTING.getKey(), container)
                    ).get();
            fail("we should get a RepositoryVerificationException");
        } catch (RepositoryVerificationException e) {
            // Fine we expect that
        }
    }

    /**
     * Test that you can snapshot on the primary repository and list the available snapshots
     * from the secondary repository.
     *
     * Note that this test requires an Azure storage account which must be a Read-access geo-redundant
     * storage (RA-GRS) account type.
     * @throws Exception If anything goes wrong
     */
    public void testGeoRedundantStorage() throws Exception {
        Client client = client();
        logger.info("-->  creating azure primary repository");
        PutRepositoryResponse putRepositoryResponsePrimary = client.admin().cluster().preparePutRepository("primary")
            .setType("azure").setSettings(Settings.builder()
                .put(Repository.CONTAINER_SETTING.getKey(), getContainerName())
            ).get();
        assertThat(putRepositoryResponsePrimary.isAcknowledged(), equalTo(true));

        logger.info("--> start get snapshots on primary");
        long startWait = System.currentTimeMillis();
        client.admin().cluster().prepareGetSnapshots("primary").get();
        long endWait = System.currentTimeMillis();
        // definitely should be done in 30s, and if its not working as expected, it takes over 1m
        assertThat(endWait - startWait, lessThanOrEqualTo(30000L));

        logger.info("-->  creating azure secondary repository");
        PutRepositoryResponse putRepositoryResponseSecondary = client.admin().cluster().preparePutRepository("secondary")
            .setType("azure").setSettings(Settings.builder()
                .put(Repository.CONTAINER_SETTING.getKey(), getContainerName())
                .put(Repository.LOCATION_MODE_SETTING.getKey(), "secondary_only")
            ).get();
        assertThat(putRepositoryResponseSecondary.isAcknowledged(), equalTo(true));

        logger.info("--> start get snapshots on secondary");
        startWait = System.currentTimeMillis();
        client.admin().cluster().prepareGetSnapshots("secondary").get();
        endWait = System.currentTimeMillis();
        logger.info("--> end of get snapshots on secondary. Took {} ms", endWait - startWait);
        assertThat(endWait - startWait, lessThanOrEqualTo(30000L));
    }

    @Override
    protected void createTestRepository(String name) {
        assertAcked(client().admin().cluster().preparePutRepository(name)
            .setType(AzureRepository.TYPE)
            .setSettings(Settings.builder()
                .put(Repository.CONTAINER_SETTING.getKey(), getContainerName())
                .put(Repository.BASE_PATH_SETTING.getKey(), getRepositoryPath())
                .put(Repository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));
    }
}
