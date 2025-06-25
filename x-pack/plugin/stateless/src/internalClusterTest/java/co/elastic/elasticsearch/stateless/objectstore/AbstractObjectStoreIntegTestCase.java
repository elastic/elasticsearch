/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.objectstore;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.metering.action.GetBlobStoreStatsNodeResponse;
import co.elastic.elasticsearch.stateless.metering.action.GetBlobStoreStatsNodesRequest;
import co.elastic.elasticsearch.stateless.metering.action.GetBlobStoreStatsNodesResponse;

import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for {@link ObjectStoreService} types.
 */
public abstract class AbstractObjectStoreIntegTestCase extends AbstractStatelessIntegTestCase {

    protected abstract String repositoryType();

    protected Settings repositorySettings() {
        return Settings.builder().put("compress", randomBoolean()).build();
    }

    protected Settings repositorySettings(ProjectId projectId) {
        return repositorySettings();
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    public void testBlobCreateVerifyDelete() throws Exception {
        startMasterAndIndexNode();
        var objectStoreService = getCurrentMasterObjectStoreService();
        BlobStoreRepository repository = randomBoolean()
            ? objectStoreService.getClusterObjectStore()
            : objectStoreService.getProjectObjectStore(ProjectId.DEFAULT);

        logger.info("--> About to write test blob");
        BlobContainer blobContainer = repository.blobStore().blobContainer(repository.basePath().add("subpath"));
        BytesArray whatToWrite = new BytesArray(randomByteArrayOfLength(randomIntBetween(100, 1000)));
        blobContainer.writeBlob(operationPurpose, "test.txt", whatToWrite, true);
        logger.info("--> Wrote blob");

        assertEquals(1, blobContainer.listBlobs(operationPurpose).size());
        try (InputStream is = blobContainer.readBlob(operationPurpose, "test.txt")) {
            assertArrayEquals(whatToWrite.array(), is.readAllBytes());
        }

        logger.info("--> Deleting");
        blobContainer.delete(operationPurpose);
        assertEquals(0, blobContainer.listBlobs(operationPurpose).size());
    }

    public void testBlobStoreStats() throws IOException {
        startMasterAndIndexNode();
        var objectStoreService = getCurrentMasterObjectStoreService();
        BlobStoreRepository repository = randomBoolean()
            ? objectStoreService.getClusterObjectStore()
            : objectStoreService.getProjectObjectStore(ProjectId.DEFAULT);

        // randomly read, write and delete some data
        var withRandomCrud = randomBoolean();
        if (withRandomCrud) {
            BlobContainer blobContainer = repository.blobStore().blobContainer(repository.basePath().add("subpath"));
            BytesArray whatToWrite = new BytesArray(randomByteArrayOfLength(randomIntBetween(100, 1000)));
            blobContainer.writeBlob(operationPurpose, "test.txt", whatToWrite, true);
            try (InputStream is = blobContainer.readBlob(operationPurpose, "test.txt")) {
                is.readAllBytes();
            }
            blobContainer.delete(operationPurpose);
        }

        // Create a repository and perform some snapshot actions
        createRepository("backup", repositorySettings());
        clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "backup", "snapshot")
            .setIncludeGlobalState(true)
            .setWaitForCompletion(true)
            .get();
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "backup", "snapshot")
            .setRestoreGlobalState(true)
            .setWaitForCompletion(true)
            .get();
        assertThat(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, "backup", "snapshot").get().isAcknowledged(), is(true));

        GetBlobStoreStatsNodesResponse getBlobStoreStatsNodesResponse = client().execute(
            Stateless.GET_BLOB_STORE_STATS_ACTION,
            new GetBlobStoreStatsNodesRequest()
        ).actionGet();

        final List<GetBlobStoreStatsNodeResponse> nodeResponses = getBlobStoreStatsNodesResponse.getNodes();

        assertEquals(1, nodeResponses.size());
        assertRepositoryStats(nodeResponses.get(0).getRepositoryStats(), withRandomCrud, operationPurpose);
        assertObsRepositoryStatsSnapshots(nodeResponses.get(0).getObsRepositoryStats());
    }

    protected abstract void assertRepositoryStats(RepositoryStats repositoryStats, boolean withRandomCrud, OperationPurpose randomPurpose);

    protected abstract void assertObsRepositoryStatsSnapshots(RepositoryStats repositoryStats);

    protected void createRepository(String repoName, Settings repoSettings) {
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                .setType(repositoryType())
                .setVerify(randomBoolean())
                .setSettings(repoSettings)
        );
    }

    // The following methods are used only when the test class run with multi-project enabled.
    protected static List<ProjectId> allProjects;

    @BeforeClass
    public static void initProjects() throws Exception {
        allProjects = randomList(2, 3, ESTestCase::randomUniqueProjectId);
    }

    @Override
    protected boolean multiProjectIntegrationTest() {
        return false;
    }

    protected Settings projectSettings(ProjectId projectId) {
        return Settings.EMPTY;
    }

    protected Settings projectSecrets(ProjectId projectId) {
        return Settings.EMPTY;
    }

    protected void putProjects(List<ProjectId> allProjects) throws Exception {
        // Ensure that the projects are created before each test
        for (ProjectId projectId : allProjects) {
            putProject(projectId, projectSettings(projectId), projectSecrets(projectId));
        }
    }

    protected void removeProjects(List<ProjectId> allProjects) throws Exception {
        // Clean up projects after each test
        for (ProjectId projectId : allProjects) {
            removeProject(projectId);
        }
    }

    public void testObjectStoreSeparation() throws Exception {
        assumeTrue("multi-project not enabled", multiProjectIntegrationTest());
        final var indexNodeName = startMasterAndIndexNode();
        final var searchNodeName = startSearchNode();
        ensureStableCluster(2);
        final var indexName = randomIdentifier();

        // simulate eagerly created system indices in the default project
        final Index defaultProjectIndex = createIndexWithDocs(client(), indexName);

        putProjects(allProjects);
        final CountDownLatch latch = new CountDownLatch(allProjects.size());

        try {
            runInParallel(allProjects.size(), i -> {
                final ProjectId projectId = allProjects.get(i);
                final var projectClient = client().projectClient(projectId);
                try {
                    final Index projectIndex = createIndexWithDocs(projectClient, indexName);

                    Stream.of(indexNodeName, searchNodeName).forEach(nodeName -> {
                        final var indexNodeObjectStoreService = getObjectStoreService(nodeName);
                        final BlobStoreRepository projectObjectStore = indexNodeObjectStoreService.getProjectObjectStore(projectId);
                        assertProjectObjectStoreContent(projectObjectStore, projectIndex);
                    });
                } catch (Exception e) {
                    fail(e);
                } finally {
                    latch.countDown();
                }
            });

            safeAwait(latch);
            ensureGreen();
            Stream.of(indexNodeName, searchNodeName).forEach(nodeName -> {
                assertClusterObjectStoreContent(getObjectStoreService(nodeName).getClusterObjectStore(), defaultProjectIndex);
            });
        } finally {
            removeProjects(allProjects);
        }
    }

    public void testBackupRepositories() throws Exception {
        assumeTrue("multi-project not enabled", multiProjectIntegrationTest());
        final var indexNodeName = startMasterAndIndexNode();
        final var searchNodeName = startSearchNode();
        ensureStableCluster(2);
        final var repoName = "backup";
        putProjects(allProjects);
        final CountDownLatch latch = new CountDownLatch(allProjects.size());

        try {
            runInParallel(allProjects.size(), i -> {
                final ProjectId projectId = allProjects.get(i);
                final var projectClient = client().projectClient(projectId);
                try {
                    // PUT repository
                    safeGet(
                        projectClient.admin()
                            .cluster()
                            .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                            .setType(repositoryType())
                            .setSettings(repositorySettings(projectId))
                            .setVerify(true)
                            .execute()
                    );

                    // GET repository
                    final var getRepositoriesResponse = safeGet(
                        projectClient.admin().cluster().prepareGetRepositories(TEST_REQUEST_TIMEOUT, repoName).execute()
                    );
                    assertThat(getRepositoriesResponse.repositories(), hasSize(1));
                    final RepositoryMetadata repositoryMetadata = getRepositoriesResponse.repositories().iterator().next();
                    assertThat(repositoryMetadata.type(), equalTo(repositoryType()));
                    assertThat(repositoryMetadata.name(), equalTo(repoName));
                    assertBackupRepositorySettings(repositoryMetadata, projectId);

                    // Verify repository
                    final var verifyRepositoryResponse = safeGet(
                        projectClient.admin()
                            .cluster()
                            .prepareVerifyRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                            .execute()
                    );
                    assertThat(
                        verifyRepositoryResponse.getNodes().stream().map(VerifyRepositoryResponse.NodeView::getName).toList(),
                        containsInAnyOrder(indexNodeName, searchNodeName)
                    );

                    // Delete repository
                    safeGet(
                        projectClient.admin()
                            .cluster()
                            .prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                            .execute()
                    );

                } catch (Exception e) {
                    fail(e);
                } finally {
                    latch.countDown();
                }
            });

            safeAwait(latch);
        } finally {
            removeProjects(allProjects);
        }
    }

    protected void assertBackupRepositorySettings(RepositoryMetadata repositoryMetadata, ProjectId projectId) {
        throw new UnsupportedOperationException("This method should be overridden in the test class where it is called");
    }

    private Index createIndexWithDocs(Client client, String indexName) throws Exception {
        final boolean recoveryFromIndexNode = randomBoolean();

        safeGet(client.admin().indices().prepareCreate(indexName).setSettings(indexSettings(1, recoveryFromIndexNode ? 0 : 1)).execute());

        indexDocsAndRefresh(client, indexName, between(10, 50));
        safeGet(client.admin().indices().prepareFlush(indexName).execute());

        if (recoveryFromIndexNode) {
            safeGet(
                client.admin()
                    .indices()
                    .prepareUpdateSettings(indexName)
                    .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
                    .execute()
            );
            indexDocsAndRefresh(client, indexName, between(10, 50));
            safeGet(client.admin().indices().prepareFlush(indexName).execute());
        }

        final var getIndexResponse = safeGet(
            client.admin().indices().prepareGetIndex(TEST_REQUEST_TIMEOUT).setIndices(indexName).execute()
        );

        final String indexUuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        return new Index(indexName, indexUuid);
    }

    private void assertProjectObjectStoreContent(BlobStoreRepository objectStore, Index expectedIndex) {
        try {
            final var blobContainer = objectStore.blobStore().blobContainer(objectStore.basePath());
            final OperationPurpose purpose = randomFrom(OperationPurpose.values());
            final var children = blobContainer.children(purpose);
            assertThat(children.keySet(), equalTo(Set.of("indices")));
            final var childrenOfIndices = children.get("indices").children(purpose);
            assertThat(childrenOfIndices.keySet(), equalTo(Set.of(expectedIndex.getUUID())));
            // TODO: assert project lease file after ES-11934, see also ES-11206
        } catch (Exception e) {
            fail(e);
        }
    }

    private void assertClusterObjectStoreContent(BlobStoreRepository objectStore, Index expectedIndex) {
        try {
            final var blobContainer = objectStore.blobStore().blobContainer(objectStore.basePath());
            final OperationPurpose purpose = randomFrom(OperationPurpose.values());
            final var children = blobContainer.children(purpose);
            assertThat(children.keySet(), equalTo(Set.of("cluster_state", "indices", "nodes")));
            final var childrenOfIndices = children.get("indices").children(purpose);
            assertThat(childrenOfIndices.keySet(), equalTo(Set.of(expectedIndex.getUUID())));
        } catch (Exception e) {
            fail(e);
        }
    }
}
