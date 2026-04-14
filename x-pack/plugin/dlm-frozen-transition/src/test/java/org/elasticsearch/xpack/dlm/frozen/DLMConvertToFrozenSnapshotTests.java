/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DLMConvertToFrozenSnapshotTests extends ESTestCase {

    private static final String REPO_NAME = "my-repo";

    private ProjectId projectId;
    private String indexName;
    private XPackLicenseState licenseState;
    private ThreadPool threadPool;
    private ClusterService clusterService;

    private AtomicReference<GetSnapshotsRequest> capturedGetSnapshotsRequest;
    private AtomicReference<GetSnapshotsResponse> mockGetSnapshotsResponse;
    private AtomicReference<Exception> mockGetSnapshotsFailure;

    private AtomicReference<DeleteSnapshotRequest> capturedDeleteSnapshotRequest;
    private AtomicReference<AcknowledgedResponse> mockDeleteSnapshotResponse;
    private AtomicReference<Exception> mockDeleteSnapshotFailure;

    private AtomicReference<CreateSnapshotRequest> capturedCreateSnapshotRequest;
    private AtomicReference<CreateSnapshotResponse> mockCreateSnapshotResponse;
    private AtomicReference<Exception> mockCreateSnapshotFailure;

    private Clock clock;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = createClusterService(threadPool);
        projectId = randomProjectIdOrDefault();
        indexName = randomAlphaOfLength(10);
        licenseState = new XPackLicenseState(
            System::currentTimeMillis,
            new XPackLicenseStatus(License.OperationMode.ENTERPRISE, true, null)
        );
        clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        capturedGetSnapshotsRequest = new AtomicReference<>();
        mockGetSnapshotsResponse = new AtomicReference<>();
        mockGetSnapshotsFailure = new AtomicReference<>();

        capturedDeleteSnapshotRequest = new AtomicReference<>();
        mockDeleteSnapshotResponse = new AtomicReference<>();
        mockDeleteSnapshotFailure = new AtomicReference<>();

        capturedCreateSnapshotRequest = new AtomicReference<>();
        mockCreateSnapshotResponse = new AtomicReference<>();
        mockCreateSnapshotFailure = new AtomicReference<>();
    }

    @After
    public void cleanup() {
        clusterService.close();
        terminate(threadPool);
    }

    private NoOpClient createMockClient() {
        return new NoOpClient(threadPool, TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext())) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof GetSnapshotsRequest getSnapshotsRequest) {
                    capturedGetSnapshotsRequest.set(getSnapshotsRequest);
                    if (mockGetSnapshotsFailure.get() != null) {
                        listener.onFailure(mockGetSnapshotsFailure.get());
                    } else if (mockGetSnapshotsResponse.get() != null) {
                        listener.onResponse((Response) mockGetSnapshotsResponse.get());
                    }
                } else if (request instanceof DeleteSnapshotRequest deleteSnapshotRequest) {
                    capturedDeleteSnapshotRequest.set(deleteSnapshotRequest);
                    if (mockDeleteSnapshotFailure.get() != null) {
                        listener.onFailure(mockDeleteSnapshotFailure.get());
                    } else if (mockDeleteSnapshotResponse.get() != null) {
                        listener.onResponse((Response) mockDeleteSnapshotResponse.get());
                    }
                } else if (request instanceof CreateSnapshotRequest createSnapshotRequest) {
                    capturedCreateSnapshotRequest.set(createSnapshotRequest);
                    if (mockCreateSnapshotFailure.get() != null) {
                        listener.onFailure(mockCreateSnapshotFailure.get());
                    } else if (mockCreateSnapshotResponse.get() != null) {
                        listener.onResponse((Response) mockCreateSnapshotResponse.get());
                    }
                } else {
                    fail("Unexpected request type: " + request.getClass());
                }
            }
        };
    }

    private DLMConvertToFrozen createConverter() {
        return new DLMConvertToFrozen(indexName, projectId, createMockClient(), clusterService, licenseState, clock);
    }

    private CreateSnapshotResponse createSuccessfulSnapshotResponse() {
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        SnapshotInfo snapshotInfo = new SnapshotInfo(
            new Snapshot(projectId, REPO_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            null,
            0L,
            1,
            List.of(),
            false,
            null,
            clock.millis(),
            Map.of()
        );
        return new CreateSnapshotResponse(snapshotInfo);
    }

    /**
     * Creates a ProjectState with the target index and a configured repository, but no in-progress snapshots.
     */
    private ProjectState createProjectState() {
        return createProjectState(SnapshotsInProgress.EMPTY);
    }

    private ProjectState createProjectState(SnapshotsInProgress snapshotsInProgress) {
        RepositoryMetadata repo = new RepositoryMetadata(REPO_NAME, "fs", Settings.EMPTY);

        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(
                IndexMetadata.builder(indexName)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putCustom(
                        DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                        Map.of(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, REPO_NAME)
                    )
                    .build(),
                false
            )
            .putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(List.of(repo)));

        Metadata metadata = Metadata.builder()
            .persistentSettings(Settings.builder().put("repositories.default_repository", REPO_NAME).build())
            .put(projectMetadataBuilder)
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress)
            .build();

        return clusterState.projectState(projectId);
    }

    /**
     * Creates a ProjectState with the target index, a configured repository, and
     * an in-progress snapshot for the index that started at the given time.
     */
    private ProjectState createProjectStateWithInProgressSnapshot(long snapshotStartTime) {
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        IndexId indexId = new IndexId(indexName, randomAlphaOfLength(10));
        ShardId shardId = new ShardId(new Index(indexName, indexId.getId()), 0);
        SnapshotsInProgress.ShardSnapshotStatus initStatus = new SnapshotsInProgress.ShardSnapshotStatus(randomAlphaOfLength(10), null);
        SnapshotsInProgress.Entry entry = SnapshotsInProgress.Entry.snapshot(
            new Snapshot(projectId, REPO_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            false,
            false,
            SnapshotsInProgress.State.STARTED,
            Map.of(indexName, indexId),
            List.of(),
            List.of(),
            snapshotStartTime,
            randomNonNegativeLong(),
            Map.of(shardId, initStatus),
            null,
            Map.of("dlm-managed", true),
            IndexVersion.current()
        );
        SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.EMPTY.withAddedEntry(entry);
        return createProjectState(snapshotsInProgress);
    }

    private void setClusterState(ProjectState projectState) {
        setState(clusterService, projectState.cluster());
    }

    private SnapshotInfo createSnapshotInfo(SnapshotState state, int failedShards) {
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        List<SnapshotShardFailure> shardFailures = new ArrayList<>();
        for (int i = 0; i < failedShards; i++) {
            shardFailures.add(new SnapshotShardFailure(null, new ShardId(indexName, randomAlphaOfLength(10), i), "test failure"));
        }
        int totalShards = Math.max(1, failedShards);
        return new SnapshotInfo(
            new Snapshot(projectId, REPO_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            state == SnapshotState.FAILED ? "simulated failure" : null,
            IndexVersion.current(),
            clock.millis(),
            clock.millis(),
            totalShards,
            totalShards - failedShards,
            shardFailures,
            false,
            null,
            state,
            Map.of()
        );
    }

    private void assertGetSnapshotsRequest(String expectedRepo, String expectedSnapshotName) {
        GetSnapshotsRequest request = capturedGetSnapshotsRequest.get();
        assertThat(request, is(notNullValue()));
        assertThat(request.repositories(), is(new String[] { expectedRepo }));
        assertThat(request.snapshots(), is(new String[] { expectedSnapshotName }));
    }

    private void assertDeleteSnapshotRequest(String expectedRepo, String expectedSnapshotName) {
        DeleteSnapshotRequest request = capturedDeleteSnapshotRequest.get();
        assertThat(request, is(notNullValue()));
        assertThat(request.repository(), is(expectedRepo));
        assertThat(request.snapshots(), is(new String[] { expectedSnapshotName }));
    }

    private void assertCreateSnapshotRequest(String expectedRepo, String expectedSnapshotName, String expectedIndex) {
        CreateSnapshotRequest request = capturedCreateSnapshotRequest.get();
        assertThat(request, is(notNullValue()));
        assertThat(request.repository(), is(expectedRepo));
        assertThat(request.snapshot(), is(expectedSnapshotName));
        assertThat(request.indices(), is(new String[] { expectedIndex }));
        assertThat(request.includeGlobalState(), is(false));
        assertThat(request.waitForCompletion(), is(true));
    }

    private GetSnapshotsResponse emptyGetSnapshotsResponse() {
        return new GetSnapshotsResponse(List.of(), null, 0, 0);
    }

    private GetSnapshotsResponse getSnapshotsResponseWith(SnapshotInfo info) {
        return new GetSnapshotsResponse(List.of(info), null, 1, 0);
    }

    // --- checkSnapshotInfoSuccess tests ---

    public void testCheckSnapshotInfoSuccess_succeeds() {
        SnapshotInfo info = createSnapshotInfo(SnapshotState.SUCCESS, 0);
        // Should not throw
        DLMConvertToFrozen.checkSnapshotInfoSuccess(indexName, "snap", info);
    }

    public void testCheckSnapshotInfoPartialState_failsWithFailedShards() {
        SnapshotInfo info = createSnapshotInfo(SnapshotState.PARTIAL, 2);
        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> DLMConvertToFrozen.checkSnapshotInfoSuccess(indexName, "snap", info)
        );
        assertThat(e.getMessage(), containsString("failed shards"));
    }

    public void testCheckSnapshotInfoFailedState_failsWithZeroFailedShards() {
        SnapshotInfo info = createSnapshotInfo(SnapshotState.FAILED, 0);
        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> DLMConvertToFrozen.checkSnapshotInfoSuccess(indexName, "snap", info)
        );
        assertThat(e.getMessage(), containsString("FAILED"));
    }

    public void testCheckSnapshotInfoSuccess_failsWithNull() {
        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> DLMConvertToFrozen.checkSnapshotInfoSuccess(indexName, "snap", null)
        );
        assertThat(e.getMessage(), containsString("did not return snapshot info"));
    }

    // --- createSnapshot tests ---

    public void testCreateSnapshot_success() {
        ProjectState projectState = createProjectState();
        setClusterState(projectState);
        mockCreateSnapshotResponse.set(createSuccessfulSnapshotResponse());

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        converter.createSnapshot(indexName, REPO_NAME, snapshotName);

        assertCreateSnapshotRequest(REPO_NAME, snapshotName, indexName);
    }

    public void testCreateSnapshot_failureThrows() {
        ProjectState projectState = createProjectState();
        setClusterState(projectState);
        mockCreateSnapshotFailure.set(new RuntimeException("snapshot failed"));

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> converter.createSnapshot(indexName, REPO_NAME, snapshotName)
        );
        assertThat(e.getMessage(), containsString("DLM failed to start snapshot"));
        assertThat(e.getCause().getMessage(), containsString("snapshot failed"));
    }

    // --- snapshotName tests ---

    public void testSnapshotName() {
        assertThat(DLMConvertToFrozen.snapshotName("my-index"), is("dlm-frozen-my-index"));
    }

    // --- deleteSnapshotIfExists tests ---

    public void testDeleteSnapshotIfExists_success() {
        setClusterState(createProjectState());
        mockDeleteSnapshotResponse.set(AcknowledgedResponse.TRUE);

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        converter.deleteSnapshotIfExists(REPO_NAME, snapshotName, indexName);

        assertDeleteSnapshotRequest(REPO_NAME, snapshotName);
    }

    public void testDeleteSnapshotIfExists_notAcknowledgedThrows() {
        setClusterState(createProjectState());
        mockDeleteSnapshotResponse.set(AcknowledgedResponse.FALSE);

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> converter.deleteSnapshotIfExists(REPO_NAME, snapshotName, indexName)
        );
        assertThat(e.getMessage(), containsString("Failed to acknowledge delete"));
        assertDeleteSnapshotRequest(REPO_NAME, snapshotName);
    }

    public void testDeleteSnapshotIfExists_snapshotMissingSilentlySucceeds() {
        setClusterState(createProjectState());
        mockDeleteSnapshotFailure.set(new SnapshotMissingException(REPO_NAME, "missing"));

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);

        // Should not throw — missing snapshot is silently ignored
        converter.deleteSnapshotIfExists(REPO_NAME, snapshotName, indexName);

        assertDeleteSnapshotRequest(REPO_NAME, snapshotName);
    }

    public void testDeleteSnapshotIfExists_otherFailureThrows() {
        setClusterState(createProjectState());
        mockDeleteSnapshotFailure.set(new RuntimeException("connection lost"));

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> converter.deleteSnapshotIfExists(REPO_NAME, snapshotName, indexName)
        );
        assertThat(e.getMessage(), containsString("DLM failed to delete stale snapshot"));
        assertThat(e.getCause().getMessage(), containsString("connection lost"));
        assertDeleteSnapshotRequest(REPO_NAME, snapshotName);
    }

    // --- getSnapshot tests ---

    public void testGetSnapshot_snapshotMissingReturnsNull() {
        setClusterState(createProjectState());
        mockGetSnapshotsFailure.set(new SnapshotMissingException(REPO_NAME, "missing"));

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        SnapshotInfo result = converter.getSnapshot(REPO_NAME, snapshotName, indexName);

        assertThat(result, is(nullValue()));
        assertGetSnapshotsRequest(REPO_NAME, snapshotName);
    }

    public void testGetSnapshot_otherFailureThrows() {
        setClusterState(createProjectState());
        mockGetSnapshotsFailure.set(new RuntimeException("connection lost"));

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> converter.getSnapshot(REPO_NAME, snapshotName, indexName)
        );
        assertThat(e.getMessage(), containsString("DLM failed while checking snapshots for index"));
        assertThat(e.getCause().getMessage(), containsString("connection lost"));
        assertGetSnapshotsRequest(REPO_NAME, snapshotName);
    }

    // --- deleteAndRestartSnapshot tests ---

    public void testDeleteAndRestartSnapshot_deleteThenCreates() {
        ProjectState projectState = createProjectState();
        setClusterState(projectState);
        mockDeleteSnapshotResponse.set(AcknowledgedResponse.TRUE);
        mockCreateSnapshotResponse.set(createSuccessfulSnapshotResponse());

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        converter.deleteSnapshotIfExists(REPO_NAME, snapshotName, indexName);
        converter.createSnapshot(indexName, REPO_NAME, snapshotName);

        assertDeleteSnapshotRequest(REPO_NAME, snapshotName);
        assertCreateSnapshotRequest(REPO_NAME, snapshotName, indexName);
    }

    public void testDeleteAndRestartSnapshot_deleteNotAcknowledgedThrows() {
        ProjectState projectState = createProjectState();
        setClusterState(projectState);
        mockDeleteSnapshotResponse.set(AcknowledgedResponse.FALSE);

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);

        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> {
            converter.deleteSnapshotIfExists(REPO_NAME, snapshotName, indexName);
            converter.createSnapshot(indexName, REPO_NAME, snapshotName);
        });
        assertThat(e.getMessage(), containsString("Failed to acknowledge delete"));
    }

    public void testDeleteAndRestartSnapshot_snapshotMissingStillCreates() {
        ProjectState projectState = createProjectState();
        setClusterState(projectState);
        mockDeleteSnapshotFailure.set(new SnapshotMissingException(REPO_NAME, "missing"));
        mockCreateSnapshotResponse.set(createSuccessfulSnapshotResponse());

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        converter.deleteSnapshotIfExists(REPO_NAME, snapshotName, indexName);
        converter.createSnapshot(indexName, REPO_NAME, snapshotName);

        assertDeleteSnapshotRequest(REPO_NAME, snapshotName);
        assertCreateSnapshotRequest(REPO_NAME, snapshotName, indexName);
    }

    // --- checkForOrphanedSnapshotAndStart tests ---

    public void testCheckForOrphanedSnapshot_noExistingSnapshot_createsNew() {
        ProjectState projectState = createProjectState();
        setClusterState(projectState);
        mockGetSnapshotsResponse.set(emptyGetSnapshotsResponse());
        mockCreateSnapshotResponse.set(createSuccessfulSnapshotResponse());

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        converter.checkForOrphanedSnapshotAndStart(indexName, REPO_NAME, snapshotName);

        assertGetSnapshotsRequest(REPO_NAME, snapshotName);
        assertCreateSnapshotRequest(REPO_NAME, snapshotName, indexName);
        assertThat(capturedDeleteSnapshotRequest.get(), is(nullValue()));
    }

    public void testCheckForOrphanedSnapshot_validOrphaned_skipsCreate() {
        ProjectState projectState = createProjectState();
        setClusterState(projectState);
        SnapshotInfo validSnapshot = createSnapshotInfo(SnapshotState.SUCCESS, 0);
        mockGetSnapshotsResponse.set(getSnapshotsResponseWith(validSnapshot));

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        converter.checkForOrphanedSnapshotAndStart(indexName, REPO_NAME, snapshotName);

        assertGetSnapshotsRequest(REPO_NAME, snapshotName);
        assertThat(capturedDeleteSnapshotRequest.get(), is(nullValue()));
        assertThat(capturedCreateSnapshotRequest.get(), is(nullValue()));
    }

    public void testCheckForOrphanedSnapshot_invalidOrphaned_deletesAndRecreates() {
        ProjectState projectState = createProjectState();
        setClusterState(projectState);
        SnapshotInfo failedSnapshot = createSnapshotInfo(SnapshotState.FAILED, 1);
        mockGetSnapshotsResponse.set(getSnapshotsResponseWith(failedSnapshot));
        mockDeleteSnapshotResponse.set(AcknowledgedResponse.TRUE);
        mockCreateSnapshotResponse.set(createSuccessfulSnapshotResponse());

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        converter.checkForOrphanedSnapshotAndStart(indexName, REPO_NAME, snapshotName);

        assertGetSnapshotsRequest(REPO_NAME, snapshotName);
        assertDeleteSnapshotRequest(REPO_NAME, snapshotName);
        assertCreateSnapshotRequest(REPO_NAME, snapshotName, indexName);
    }

    public void testCheckForOrphanedSnapshot_snapshotMissing_createsNew() {
        ProjectState projectState = createProjectState();
        setClusterState(projectState);
        mockGetSnapshotsFailure.set(new SnapshotMissingException(REPO_NAME, "missing"));
        mockCreateSnapshotResponse.set(createSuccessfulSnapshotResponse());

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        converter.checkForOrphanedSnapshotAndStart(indexName, REPO_NAME, snapshotName);

        assertCreateSnapshotRequest(REPO_NAME, snapshotName, indexName);
    }

    public void testCheckForOrphanedSnapshot_getFailure_throws() {
        ProjectState projectState = createProjectState();
        setClusterState(projectState);
        mockGetSnapshotsFailure.set(new RuntimeException("get failed"));

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> converter.checkForOrphanedSnapshotAndStart(indexName, REPO_NAME, snapshotName)
        );
        assertThat(e.getMessage(), containsString("DLM failed while checking snapshots for index"));
        assertThat(e.getCause().getMessage(), containsString("get failed"));
    }

    public void testHandleInProgressSnapshot_exceededTimeout_deletesAndRestarts() {
        // Snapshot started longer ago than SNAPSHOT_TIMEOUT (12h)
        long oldStartTime = clock.millis() - TimeValue.timeValueHours(13).millis(); // exceeds 12h SNAPSHOT_TIMEOUT
        ProjectState projectState = createProjectStateWithInProgressSnapshot(oldStartTime);
        setClusterState(projectState);

        mockDeleteSnapshotResponse.set(AcknowledgedResponse.TRUE);
        mockCreateSnapshotResponse.set(createSuccessfulSnapshotResponse());

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        converter.handleInProgressSnapshot(indexName, REPO_NAME, snapshotName, oldStartTime);

        assertDeleteSnapshotRequest(REPO_NAME, snapshotName);
        assertCreateSnapshotRequest(REPO_NAME, snapshotName, indexName);
    }

    public void testWaitForSnapshotCompletion_snapshotAlreadyComplete_succeeds() {
        // Set up cluster state with NO in-progress snapshot so the predicate is immediately true
        ProjectState projectState = createProjectState();
        setClusterState(projectState);

        // Mock getSnapshot to return a successful snapshot
        SnapshotInfo successSnapshot = createSnapshotInfo(SnapshotState.SUCCESS, 0);
        mockGetSnapshotsResponse.set(getSnapshotsResponseWith(successSnapshot));

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        long snapshotStartTime = clock.millis() - TimeValue.timeValueMinutes(5).millis();

        // Should complete without error — predicate satisfied immediately, then getSnapshot validates
        converter.waitForSnapshotCompletion(indexName, REPO_NAME, snapshotName, snapshotStartTime);

        assertGetSnapshotsRequest(REPO_NAME, snapshotName);
    }

    public void testWaitForSnapshotCompletion_snapshotDisappeared_throws() {
        // No in-progress snapshot — predicate immediately satisfied
        ProjectState projectState = createProjectState();
        setClusterState(projectState);

        // Mock getSnapshot to return no results (snapshot disappeared)
        mockGetSnapshotsResponse.set(emptyGetSnapshotsResponse());

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        long snapshotStartTime = clock.millis() - TimeValue.timeValueMinutes(5).millis();

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> converter.waitForSnapshotCompletion(indexName, REPO_NAME, snapshotName, snapshotStartTime)
        );
        assertThat(e.getMessage(), containsString("disappeared while waiting for completion"));
        assertGetSnapshotsRequest(REPO_NAME, snapshotName);
    }

    public void testWaitForSnapshotCompletion_snapshotFailed_throws() {
        // No in-progress snapshot — predicate immediately satisfied
        ProjectState projectState = createProjectState();
        setClusterState(projectState);

        // Mock getSnapshot to return a failed snapshot
        SnapshotInfo failedSnapshot = createSnapshotInfo(SnapshotState.FAILED, 1);
        mockGetSnapshotsResponse.set(getSnapshotsResponseWith(failedSnapshot));

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        long snapshotStartTime = clock.millis() - TimeValue.timeValueMinutes(5).millis();

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> converter.waitForSnapshotCompletion(indexName, REPO_NAME, snapshotName, snapshotStartTime)
        );
        assertThat(e.getMessage(), containsString("failed shards"));
        assertGetSnapshotsRequest(REPO_NAME, snapshotName);
    }

    // --- maybeTakeSnapshot tests ---

    public void testMaybeTakeSnapshot_noInProgress_noExisting_createsNew() throws InterruptedException {
        ProjectState projectState = createProjectState();
        setClusterState(projectState);
        mockGetSnapshotsResponse.set(emptyGetSnapshotsResponse());
        mockCreateSnapshotResponse.set(createSuccessfulSnapshotResponse());

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        converter.maybeTakeSnapshot(indexName);

        assertGetSnapshotsRequest(REPO_NAME, snapshotName);
        assertCreateSnapshotRequest(REPO_NAME, snapshotName, indexName);
        assertThat(capturedDeleteSnapshotRequest.get(), is(nullValue()));
    }

    public void testMaybeTakeSnapshot_inProgressExceededTimeout_deletesAndRestarts() throws InterruptedException {
        long oldStartTime = clock.millis() - TimeValue.timeValueHours(13).millis();
        ProjectState projectState = createProjectStateWithInProgressSnapshot(oldStartTime);
        setClusterState(projectState);

        mockDeleteSnapshotResponse.set(AcknowledgedResponse.TRUE);
        mockCreateSnapshotResponse.set(createSuccessfulSnapshotResponse());

        DLMConvertToFrozen converter = createConverter();
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        converter.maybeTakeSnapshot(indexName);

        assertDeleteSnapshotRequest(REPO_NAME, snapshotName);
        assertCreateSnapshotRequest(REPO_NAME, snapshotName, indexName);
    }

}
