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


import com.microsoft.windowsazure.services.core.ServiceException;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cloud.azure.AbstractAzureTest;
import org.elasticsearch.cloud.azure.AzureStorageService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.store.MockDirectoryHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URISyntaxException;

import static org.hamcrest.Matchers.*;

/**
 * This test needs Azure to run and -Dtests.azure=true to be set
 * and -Dtests.config=/path/to/elasticsearch.yml
 * @see org.elasticsearch.cloud.azure.AbstractAzureTest
 */
@AbstractAzureTest.AzureTest
@ElasticsearchIntegrationTest.ClusterScope(
        scope = ElasticsearchIntegrationTest.Scope.SUITE,
        numDataNodes = 2,
        transportClientRatio = 0.0)
public class AzureSnapshotRestoreITest extends AbstractAzureTest {

    private final String basePath;

    public AzureSnapshotRestoreITest() {
        basePath = "/snapshot-itest/repo-" + randomInt();
    }

    @Override
    public Settings indexSettings() {
        // During restore we frequently restore index to exactly the same state it was before, that might cause the same
        // checksum file to be written twice during restore operation
        return ImmutableSettings.builder().put(super.indexSettings())
                .put(MockDirectoryHelper.RANDOM_PREVENT_DOUBLE_WRITE, false)
                .put(MockDirectoryHelper.RANDOM_NO_DELETE_OPEN_FILE, false)
                .build();
    }

    @Before
    public final void wipeBefore() throws StorageException, ServiceException, URISyntaxException {
        wipeRepositories();
        cleanRepositoryFiles(basePath);
    }

    @After
    public final void wipeAfter() throws StorageException, ServiceException, URISyntaxException {
        wipeRepositories();
        cleanRepositoryFiles(basePath);
    }

    @Test
    public void testSimpleWorkflow() {
        Client client = client();
        logger.info("-->  creating azure repository with path [{}]", basePath);
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("azure").setSettings(ImmutableSettings.settingsBuilder()
                        .put(AzureStorageService.Fields.CONTAINER, "elasticsearch-integration")
                        .put(AzureStorageService.Fields.BASE_PATH, basePath)
                        .put(AzureStorageService.Fields.CHUNK_SIZE, randomIntBetween(1000, 10000))
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

    @Test
    public void testMultipleRepositories() {
        Client client = client();
        logger.info("-->  creating azure repository with path [{}]", basePath);
        PutRepositoryResponse putRepositoryResponse1 = client.admin().cluster().preparePutRepository("test-repo1")
                .setType("azure").setSettings(ImmutableSettings.settingsBuilder()
                        .put(AzureStorageService.Fields.CONTAINER, "elasticsearch-integration1")
                        .put(AzureStorageService.Fields.BASE_PATH, basePath)
                        .put(AzureStorageService.Fields.CHUNK_SIZE, randomIntBetween(1000, 10000))
                ).get();
        assertThat(putRepositoryResponse1.isAcknowledged(), equalTo(true));
        PutRepositoryResponse putRepositoryResponse2 = client.admin().cluster().preparePutRepository("test-repo2")
                .setType("azure").setSettings(ImmutableSettings.settingsBuilder()
                        .put(AzureStorageService.Fields.CONTAINER, "elasticsearch-integration2")
                        .put(AzureStorageService.Fields.BASE_PATH, basePath)
                        .put(AzureStorageService.Fields.CHUNK_SIZE, randomIntBetween(1000, 10000))
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
     * For issue #21: https://github.com/elasticsearch/elasticsearch-cloud-azure/issues/21
     */
    @Test @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elasticsearch/elasticsearch-cloud-azure/issues/21")
    public void testForbiddenContainerName() {
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
    private void checkContainerName(String container, boolean correct) {
        logger.info("-->  creating azure repository with container name [{}]", container);
        try {
            PutRepositoryResponse putRepositoryResponse = client().admin().cluster().preparePutRepository("test-repo")
                    .setType("azure").setSettings(ImmutableSettings.settingsBuilder()
                                    .put(AzureStorageService.Fields.CONTAINER, container)
                                    .put(AzureStorageService.Fields.BASE_PATH, basePath)
                                    .put(AzureStorageService.Fields.CHUNK_SIZE, randomIntBetween(1000, 10000))
                    ).get();
            assertThat(putRepositoryResponse.isAcknowledged(), is(correct));
            client().admin().cluster().prepareDeleteRepository("test-repo").get();
        } catch (RepositoryException e) {
            if (correct) {
                // We did not expect any exception here :(
                throw e;
            }
        }
    }

    /**
     * Test case for issue #23: https://github.com/elasticsearch/elasticsearch-cloud-azure/issues/23
     */
    @Test
    public void testNonExistingRepo_23() {
        Client client = client();
        logger.info("-->  creating azure repository with path [{}]", basePath);
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("azure").setSettings(ImmutableSettings.settingsBuilder()
                                .put(AzureStorageService.Fields.CONTAINER, "elasticsearch-integration")
                                .put(AzureStorageService.Fields.BASE_PATH, basePath)
                                .put(AzureStorageService.Fields.CHUNK_SIZE, randomIntBetween(1000, 10000))
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
     * Purge the test container
     */
    public void cleanRepositoryFiles(String path) throws StorageException, ServiceException, URISyntaxException {
        String container = internalCluster().getInstance(Settings.class).get("repositories.azure.container",
                AzureRepository.CONTAINER_DEFAULT);
        logger.info("--> remove blobs in container [{}], path [{}]", container, path);
        AzureStorageService client = internalCluster().getInstance(AzureStorageService.class);

        // Remove starting / if any
        path = Strings.trimLeadingCharacter(path, '/');

        client.deleteFiles(container, path);
    }
}
