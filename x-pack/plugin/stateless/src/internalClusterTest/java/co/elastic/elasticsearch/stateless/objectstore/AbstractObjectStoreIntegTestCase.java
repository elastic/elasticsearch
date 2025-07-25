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

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.snapshots.UpdateIndexShardSnapshotStatusRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
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
        assumeFalse("restore not yet working in multi-project mode", multiProjectIntegrationTest());
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

    protected void putProjects(Collection<ProjectId> allProjects) throws Exception {
        // Ensure that the projects are created before each test
        for (ProjectId projectId : allProjects) {
            putProject(projectId, projectSettings(projectId), projectSecrets(projectId), null);
        }
    }

    protected void removeProjects(Collection<ProjectId> allProjects) throws Exception {
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

    public void testMultiProjectSnapshots() throws Exception {
        assumeTrue("multi-project not enabled", multiProjectIntegrationTest());
        startMasterAndIndexNode();
        startIndexNode();
        ensureStableCluster(2);
        final var repoName = "backup";
        final CountDownLatch latch = new CountDownLatch(allProjects.size());

        final Map<ProjectId, Integer> projectNumDocs = new HashMap<>();
        for (ProjectId projectId : allProjects) {
            // Create the project with reserved repository
            putProject(
                projectId,
                projectSettings(projectId),
                projectSecrets(projectId),
                new RepositoryMetadata(repoName, repositoryType(), repositorySettings(projectId))
            );
            final var projectClient = client().projectClient(projectId);
            // Create index with some docs
            final var indexName = "index-" + projectId.id().toLowerCase(Locale.ROOT);
            safeGet(
                projectClient.admin()
                    .indices()
                    .prepareCreate(indexName)
                    .setSettings(indexSettings(2, 0).put("index.routing.allocation.total_shards_per_node", 1))
                    .execute()
            );
            final int numDocs = between(10, 50);
            indexDocsAndRefresh(projectClient, indexName, numDocs);
            projectNumDocs.put(projectId, numDocs);
        }

        final var projectToDelaySnapshot = randomFrom(allProjects);
        logger.info("--> project to delay snapshot: [{}]", projectToDelaySnapshot);
        final AtomicReference<CountDownLatch> updateShardLatchRef = new AtomicReference<>();
        final AtomicReference<CheckedRunnable<Exception>> updateShardRunnableRef = new AtomicReference<>();
        // Delay one snapshot so that it can be observed as in-progress
        MockTransportService.getInstance(internalCluster().getMasterName())
            .addRequestHandlingBehavior(SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME, (handler, request, channel, task) -> {
                final var updateRequest = asInstanceOf(UpdateIndexShardSnapshotStatusRequest.class, request);
                final var updateShardLatch = updateShardLatchRef.get();
                if (updateShardLatch != null && projectToDelaySnapshot.equals(updateRequest.snapshot().getProjectId())) {
                    if (updateShardRunnableRef.compareAndSet(null, () -> handler.messageReceived(request, channel, task))) {
                        updateShardLatch.countDown();
                        logger.info(
                            "--> delaying snapshot update for project [{}], shard {}",
                            projectToDelaySnapshot,
                            updateRequest.shardId()
                        );
                        return;
                    }
                }
                handler.messageReceived(request, channel, task);
            });

        final String snapshotName = "snapshot";
        try {
            runInParallel(allProjects.size(), i -> {
                final ProjectId projectId = allProjects.get(i);
                final var projectClient = client().projectClient(projectId);
                final String indexName = "index-" + projectId.id().toLowerCase(Locale.ROOT);
                try {
                    // Create snapshot
                    final var createSnapshotResponse = safeGet(
                        projectClient.admin()
                            .cluster()
                            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                            .setIndices(randomFrom("*", "index*"))
                            .setWaitForCompletion(true)
                            .execute()
                    );
                    assertNotNull(createSnapshotResponse.getSnapshotInfo());
                    assertSnapshotInfo(createSnapshotResponse.getSnapshotInfo(), projectId, repoName, SnapshotState.SUCCESS);

                    // Clone snapshot
                    safeGet(
                        projectClient.admin()
                            .cluster()
                            .prepareCloneSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName, "clone-" + snapshotName)
                            .setIndices(indexName)
                            .execute()
                    );

                    // Create another snapshot and maybe delay it so that it is visible as in-progress
                    final boolean shouldDelaySnapshot = projectId.equals(projectToDelaySnapshot);
                    final ActionFuture<CreateSnapshotResponse> future;
                    if (shouldDelaySnapshot) {
                        logger.info("--> delaying snapshot for project [{}]", projectId);
                        final var updateShardLatch = new CountDownLatch(1);
                        updateShardLatchRef.set(updateShardLatch);
                        future = projectClient.admin()
                            .cluster()
                            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "another-" + snapshotName)
                            .setIndices(randomFrom("*", "index*"))
                            .setWaitForCompletion(false)
                            .execute();
                        safeAwait(updateShardLatch);
                    } else {
                        future = null;
                        safeGet(
                            projectClient.admin()
                                .cluster()
                                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "another-" + snapshotName)
                                .setIndices(randomFrom("*", "index*"))
                                .setWaitForCompletion(true)
                                .execute()
                        );
                    }

                    // Get snapshot
                    final var getSnapshotsResponse = safeGet(
                        projectClient.admin().cluster().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repoName).setSnapshots("_all").execute()
                    );
                    assertThat(getSnapshotsResponse.getSnapshots(), hasSize(3));
                    assertThat(
                        getSnapshotsResponse.getSnapshots().stream().map(info -> info.snapshot().getSnapshotId().getName()).toList(),
                        containsInAnyOrder(snapshotName, "clone-" + snapshotName, "another-" + snapshotName)
                    );
                    for (var snapshotInfo : getSnapshotsResponse.getSnapshots()) {
                        if (snapshotInfo.snapshot().getSnapshotId().getName().equals("another-" + snapshotName)) {
                            assertSnapshotInfo(
                                snapshotInfo,
                                projectId,
                                repoName,
                                shouldDelaySnapshot ? SnapshotState.IN_PROGRESS : SnapshotState.SUCCESS
                            );
                        } else {
                            assertSnapshotInfo(snapshotInfo, projectId, repoName, SnapshotState.SUCCESS);
                        }
                    }

                    // Get snapshot status
                    final var getSnapshotStatusResponse = safeGet(
                        projectClient.admin()
                            .cluster()
                            .prepareSnapshotStatus(TEST_REQUEST_TIMEOUT, repoName)
                            .setSnapshots(snapshotName, "clone-" + snapshotName, "another-" + snapshotName)
                            .execute()
                    );
                    assertThat(getSnapshotStatusResponse.getSnapshots(), hasSize(3));
                    assertThat(
                        getSnapshotStatusResponse.getSnapshots()
                            .stream()
                            .map(status -> status.getSnapshot().getSnapshotId().getName())
                            .toList(),
                        containsInAnyOrder(snapshotName, "clone-" + snapshotName, "another-" + snapshotName)
                    );
                    for (var snapshotStatus : getSnapshotStatusResponse.getSnapshots()) {
                        if (snapshotStatus.getSnapshot().getSnapshotId().getName().equals("another-" + snapshotName)) {
                            assertSnapshotStatus(
                                snapshotStatus,
                                projectId,
                                repoName,
                                shouldDelaySnapshot ? SnapshotsInProgress.State.STARTED : SnapshotsInProgress.State.SUCCESS
                            );
                        } else {
                            assertSnapshotStatus(snapshotStatus, projectId, repoName, SnapshotsInProgress.State.SUCCESS);
                        }
                    }

                    // Unblock "another-snapshot" and it should complete
                    if (shouldDelaySnapshot) {
                        logger.info("--> unblocking snapshot for project [{}]", projectId);
                        updateShardRunnableRef.get().run();
                        safeGet(future);
                    }

                    // Delete snapshot
                    safeGet(projectClient.admin().cluster().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName).execute());

                    final var restoreClonedSnapshot = randomBoolean();
                    safeGet(projectClient.admin().indices().prepareDelete(indexName).execute());

                    if (restoreClonedSnapshot) {
                        safeGet(
                            projectClient.admin()
                                .cluster()
                                .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "clone-" + snapshotName)
                                .setIndices(randomFrom("*", "index*", indexName))
                                .setWaitForCompletion(true)
                                .execute()
                        );
                    } else {
                        // Restore snapshot
                        safeGet(
                            projectClient.admin()
                                .cluster()
                                .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "another-" + snapshotName)
                                .setIndices(randomFrom("*", "index*", indexName))
                                .setWaitForCompletion(true)
                                .execute()
                        );
                    }

                    final var indicesStatsResponse = safeGet(
                        projectClient.admin().indices().prepareStats(indexName).setDocs(true).execute()
                    );
                    assertThat(indicesStatsResponse.getTotal().getDocs().getCount(), equalTo(projectNumDocs.get(projectId).longValue()));

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

    public void testCannotRestoreForNonExistingProject() {
        assumeTrue("multi-project not enabled", multiProjectIntegrationTest());
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        final ProjectId projectId = randomUniqueProjectId();
        final var projectClient = client().projectClient(projectId);
        final var e = expectThrows(
            SnapshotRestoreException.class,
            () -> projectClient.admin()
                .cluster()
                .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, randomIdentifier(), randomIdentifier())
                .setWaitForCompletion(true)
                .execute()
                .actionGet()
        );
        assertThat(e.getMessage(), containsString("project [" + projectId + "] does not exist"));
    }

    public void testRestoreProjectMetadata() throws Exception {
        assumeTrue("multi-project not enabled", multiProjectIntegrationTest());
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        final var repoName = "backup";
        final var projectId = randomFrom(allProjects);
        putProject(
            projectId,
            projectSettings(projectId),
            projectSecrets(projectId),
            new RepositoryMetadata(repoName, repositoryType(), repositorySettings(projectId))
        );
        final var projectClient = client().projectClient(projectId);
        final var indexName = "index";
        final String snapshotName = "snapshot";
        final String componentTemplateName = "test_component_template";

        try {
            logger.info("--> create index and index some docs");
            safeGet(projectClient.admin().indices().prepareCreate(indexName).setSettings(indexSettings(1, 1)).execute());
            final int numDocs = between(10, 50);
            indexDocsAndRefresh(projectClient, indexName, numDocs);

            logger.info("--> creating component template");
            final var componentTemplate = new ComponentTemplate(new Template(null, new CompressedXContent("""
                    {
                      "_doc":{
                        "dynamic":"strict",
                        "properties":{
                          "field1":{
                            "type":"text"
                          }
                        }
                      }
                    }
                """), null), 3L, null);
            safeGet(
                projectClient.execute(
                    PutComponentTemplateAction.INSTANCE,
                    new PutComponentTemplateAction.Request(componentTemplateName).componentTemplate(componentTemplate)
                )
            );

            // Snapshot the index and project level state
            safeGet(
                projectClient.admin()
                    .cluster()
                    .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                    .setIncludeGlobalState(true)
                    .setWaitForCompletion(true)
                    .execute()
            );

            // Delete the index and component template
            safeGet(projectClient.admin().indices().prepareDelete(indexName).execute());
            safeGet(
                projectClient.execute(
                    TransportDeleteComponentTemplateAction.TYPE,
                    new TransportDeleteComponentTemplateAction.Request(componentTemplateName)
                )
            );

            final boolean restoreProjectState = randomBoolean();
            logger.info("--> restore snapshot {} project level state", restoreProjectState ? "with" : "without");
            safeGet(
                projectClient.admin()
                    .cluster()
                    .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                    .setRestoreGlobalState(restoreProjectState)
                    .setWaitForCompletion(true)
                    .execute()
            );
            safeGet(projectClient.admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT, indexName).setWaitForGreenStatus().execute());

            // Index is restored
            assertResponse(
                projectClient.prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setSize(0),
                response -> assertHitCount(response, numDocs)
            );

            // Component template is restored if requested or it should not exist
            if (restoreProjectState) {
                final GetComponentTemplateAction.Response response = safeGet(
                    projectClient.execute(
                        GetComponentTemplateAction.INSTANCE,
                        new GetComponentTemplateAction.Request(TEST_REQUEST_TIMEOUT, componentTemplateName)
                    )
                );
                assertThat(response.getComponentTemplates().get(componentTemplateName), equalTo(componentTemplate));
            } else {
                expectThrows(
                    ResourceNotFoundException.class,
                    () -> projectClient.execute(
                        GetComponentTemplateAction.INSTANCE,
                        new GetComponentTemplateAction.Request(TEST_REQUEST_TIMEOUT, componentTemplateName)
                    ).actionGet()
                );
            }
        } finally {
            removeProject(projectId);
        }

    }

    private void assertSnapshotInfo(SnapshotInfo snapshotInfo, ProjectId projectId, String repoName, SnapshotState state) {
        assertThat(snapshotInfo.projectId(), equalTo(projectId));
        assertThat(snapshotInfo.repository(), equalTo(repoName));
        assertThat(snapshotInfo.state(), equalTo(state));
        assertThat(snapshotInfo.indices(), containsInAnyOrder("index-" + projectId.id().toLowerCase(Locale.ROOT)));
    }

    private void assertSnapshotStatus(
        SnapshotStatus snapshotStatus,
        ProjectId projectId,
        String repoName,
        SnapshotsInProgress.State state
    ) {
        assertThat(snapshotStatus.getSnapshot().getProjectId(), equalTo(projectId));
        assertThat(snapshotStatus.getSnapshot().getRepository(), equalTo(repoName));
        assertThat(snapshotStatus.getState(), equalTo(state));
        assertThat(snapshotStatus.getShards(), hasSize(2));
    }

    public void testReservedBackupRepositories() throws Exception {
        assumeTrue("multi-project not enabled", multiProjectIntegrationTest());
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);
        final var repoName = "backup";
        final var projectId = randomUniqueProjectId();
        // Create the project with reserved repository
        putProject(
            projectId,
            projectSettings(projectId),
            projectSecrets(projectId),
            new RepositoryMetadata(repoName, repositoryType(), repositorySettings(projectId))
        );

        try {
            final var projectClient = client().projectClient(projectId);
            // GET repository
            final var getRepositoriesResponse = safeGet(
                projectClient.admin().cluster().prepareGetRepositories(TEST_REQUEST_TIMEOUT, repoName).execute()
            );
            assertThat(getRepositoriesResponse.repositories(), hasSize(1));
            final RepositoryMetadata repositoryMetadata = getRepositoriesResponse.repositories().iterator().next();
            assertThat(repositoryMetadata.type(), equalTo(repositoryType()));
            assertThat(repositoryMetadata.name(), equalTo(repoName));
            assertBackupRepositorySettings(repositoryMetadata, projectId);

            // Cannot delete reserved repository
            final var e = expectThrows(
                ExecutionException.class,
                () -> projectClient.admin()
                    .cluster()
                    .prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                    .execute()
                    .get()
            );
            assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), containsString("[backup] set as read-only by [file_settings]"));
        } finally {
            removeProject(projectId);
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
