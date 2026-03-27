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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamLifecycleConvertToFrozenSnapshotTests extends ESTestCase {

    private static final String REPO_NAME = "my-repo";

    private ProjectId projectId;
    private String indexName;
    private XPackLicenseState licenseState;
    private ThreadPool threadPool;

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
        threadPool.shutdownNow();
    }

    public void testStartsNewSnapshotWhenNoSnapshotExistsInRepoAndNoneInProgress() {
        ProjectState projectState = createProjectState();
        // GetSnapshots returns empty (no existing snapshot)
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(), null, 0, 0));
        // CreateSnapshot succeeds
        mockCreateSnapshotResponse.set(createSuccessfulSnapshotResponse());

        DataStreamLifecycleConvertToFrozen converter = createConverter(projectState);
        converter.maybeTakeSnapshot(indexName);

        // Should have checked for existing snapshots
        assertThat(capturedGetSnapshotsRequest.get(), is(notNullValue()));
        // Should have created a new snapshot
        assertThat(capturedCreateSnapshotRequest.get(), is(notNullValue()));
        assertThat(capturedCreateSnapshotRequest.get().repository(), is(REPO_NAME));
        assertThat(capturedCreateSnapshotRequest.get().snapshot(), is(DataStreamLifecycleConvertToFrozen.snapshotName(indexName)));
        assertThat(capturedCreateSnapshotRequest.get().indices(), is(new String[] { indexName }));
        assertTrue(capturedCreateSnapshotRequest.get().waitForCompletion());
        assertFalse(capturedCreateSnapshotRequest.get().includeGlobalState());
    }

    public void testStartsNewSnapshotWhenSnapshotMissingExceptionOnGet() {
        ProjectState projectState = createProjectState();
        // GetSnapshots throws SnapshotMissingException
        mockGetSnapshotsFailure.set(new SnapshotMissingException(REPO_NAME, "not-found"));
        // CreateSnapshot succeeds
        mockCreateSnapshotResponse.set(createSuccessfulSnapshotResponse());

        DataStreamLifecycleConvertToFrozen converter = createConverter(projectState);
        converter.maybeTakeSnapshot(indexName);

        // Should have tried to create a new snapshot
        assertThat(capturedCreateSnapshotRequest.get(), is(notNullValue()));
    }

    public void testSkipsSnapshotWhenValidOrphanedSnapshotExists() {
        ProjectState projectState = createProjectState();
        String snapshotName = DataStreamLifecycleConvertToFrozen.snapshotName(indexName);
        // GetSnapshots returns a successful snapshot with 0 failed shards
        SnapshotInfo successfulSnapshot = new SnapshotInfo(
            new Snapshot(projectId, REPO_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            SnapshotState.SUCCESS
        );
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(successfulSnapshot), null, 1, 0));

        DataStreamLifecycleConvertToFrozen converter = createConverter(projectState);
        converter.maybeTakeSnapshot(indexName);

        // Should have checked for existing snapshots
        assertThat(capturedGetSnapshotsRequest.get(), is(notNullValue()));
        // Should NOT have tried to create a new snapshot (valid one already exists)
        assertThat(capturedCreateSnapshotRequest.get(), is(nullValue()));
        // Should NOT have tried to delete
        assertThat(capturedDeleteSnapshotRequest.get(), is(nullValue()));
    }

    public void testDeletesAndRestartsWhenOrphanedSnapshotIsFailed() {
        ProjectState projectState = createProjectState();
        String snapshotName = DataStreamLifecycleConvertToFrozen.snapshotName(indexName);
        // GetSnapshots returns a FAILED snapshot
        SnapshotInfo failedSnapshot = new SnapshotInfo(
            new Snapshot(projectId, REPO_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            SnapshotState.FAILED
        );
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(failedSnapshot), null, 1, 0));
        // Delete succeeds
        mockDeleteSnapshotResponse.set(AcknowledgedResponse.TRUE);
        // CreateSnapshot succeeds
        mockCreateSnapshotResponse.set(createSuccessfulSnapshotResponse());

        DataStreamLifecycleConvertToFrozen converter = createConverter(projectState);
        converter.maybeTakeSnapshot(indexName);

        // Should have deleted the failed snapshot
        assertThat(capturedDeleteSnapshotRequest.get(), is(notNullValue()));
        assertThat(capturedDeleteSnapshotRequest.get().snapshots(), is(new String[] { snapshotName }));
        // Should have created a new snapshot
        assertThat(capturedCreateSnapshotRequest.get(), is(notNullValue()));
    }

    public void testDeletesAndRestartsWhenOrphanedSnapshotIsPartial() {
        ProjectState projectState = createProjectState();
        String snapshotName = DataStreamLifecycleConvertToFrozen.snapshotName(indexName);
        // GetSnapshots returns a PARTIAL snapshot
        SnapshotInfo partialSnapshot = new SnapshotInfo(
            new Snapshot(projectId, REPO_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            SnapshotState.PARTIAL
        );
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(partialSnapshot), null, 1, 0));
        // Delete succeeds
        mockDeleteSnapshotResponse.set(AcknowledgedResponse.TRUE);
        // CreateSnapshot succeeds
        mockCreateSnapshotResponse.set(createSuccessfulSnapshotResponse());

        DataStreamLifecycleConvertToFrozen converter = createConverter(projectState);
        converter.maybeTakeSnapshot(indexName);

        // Should have deleted the partial snapshot and created a new one
        assertThat(capturedDeleteSnapshotRequest.get(), is(notNullValue()));
        assertThat(capturedCreateSnapshotRequest.get(), is(notNullValue()));
    }

    public void testLeavesSnapshotAloneWhenInProgressAndNotTimedOut() {
        // Snapshot started 1 minute ago — well within the 12-hour timeout
        long snapshotStartTime = clock.millis() - 60_000;
        ProjectState projectState = createProjectStateWithInProgressSnapshot(snapshotStartTime);

        DataStreamLifecycleConvertToFrozen converter = createConverter(projectState);
        converter.maybeTakeSnapshot(indexName);

        // Should NOT have tried to get, delete, or create snapshots (just leave the in-progress one alone)
        assertThat(capturedGetSnapshotsRequest.get(), is(nullValue()));
        assertThat(capturedDeleteSnapshotRequest.get(), is(nullValue()));
        assertThat(capturedCreateSnapshotRequest.get(), is(nullValue()));
    }

    public void testDeletesAndRestartsSnapshotWhenInProgressAndTimedOut() {
        // Snapshot started more than 12 hours ago
        long snapshotStartTime = clock.millis() - (13 * 60 * 60 * 1000L);
        ProjectState projectState = createProjectStateWithInProgressSnapshot(snapshotStartTime);

        // Delete succeeds
        mockDeleteSnapshotResponse.set(AcknowledgedResponse.TRUE);
        // CreateSnapshot succeeds
        mockCreateSnapshotResponse.set(createSuccessfulSnapshotResponse());

        DataStreamLifecycleConvertToFrozen converter = createConverter(projectState);
        converter.maybeTakeSnapshot(indexName);

        // Should have deleted the timed-out snapshot and started a new one
        assertThat(capturedDeleteSnapshotRequest.get(), is(notNullValue()));
        assertThat(capturedCreateSnapshotRequest.get(), is(notNullValue()));
    }

    public void testThrowsWhenCreateSnapshotFails() {
        ProjectState projectState = createProjectState();
        // GetSnapshots returns empty
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(), null, 0, 0));
        // CreateSnapshot fails
        mockCreateSnapshotFailure.set(new ElasticsearchException("snapshot creation failed"));

        DataStreamLifecycleConvertToFrozen converter = createConverter(projectState);

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> converter.maybeTakeSnapshot(indexName));
        assertThat(exception.getMessage(), containsString("snapshot creation failed"));
    }

    public void testThrowsWhenCreateSnapshotHasFailedShards() {
        ProjectState projectState = createProjectState();
        // GetSnapshots returns empty
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(), null, 0, 0));
        // CreateSnapshot returns with failed shards
        String snapshotName = DataStreamLifecycleConvertToFrozen.snapshotName(indexName);
        SnapshotInfo snapshotInfoWithFailures = new SnapshotInfo(
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
            0L,
            Map.of()
        );
        mockCreateSnapshotResponse.set(new CreateSnapshotResponse(snapshotInfoWithFailures));

        DataStreamLifecycleConvertToFrozen converter = createConverter(projectState);

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> converter.maybeTakeSnapshot(indexName));
        assertThat(exception.getMessage(), containsString("failed shards"));
    }

    public void testThrowsWhenGetSnapshotsFails() {
        ProjectState projectState = createProjectState();
        // GetSnapshots fails with a non-SnapshotMissing exception
        mockGetSnapshotsFailure.set(new ElasticsearchException("get snapshots failed"));

        DataStreamLifecycleConvertToFrozen converter = createConverter(projectState);

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> converter.maybeTakeSnapshot(indexName));
        assertThat(exception.getMessage(), containsString("get snapshots failed"));
    }

    public void testThrowsWhenDeleteSnapshotIsNotAcknowledged() {
        ProjectState projectState = createProjectState();
        String snapshotName = DataStreamLifecycleConvertToFrozen.snapshotName(indexName);
        // GetSnapshots returns a FAILED snapshot
        SnapshotInfo failedSnapshot = new SnapshotInfo(
            new Snapshot(projectId, REPO_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            SnapshotState.FAILED
        );
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(failedSnapshot), null, 1, 0));
        // Delete is not acknowledged
        mockDeleteSnapshotResponse.set(AcknowledgedResponse.FALSE);

        DataStreamLifecycleConvertToFrozen converter = createConverter(projectState);

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> converter.maybeTakeSnapshot(indexName));
        assertThat(exception.getMessage(), containsString("Failed to acknowledge delete of snapshot"));
    }

    public void testDeleteAndRestartHandlesSnapshotMissingOnDelete() {
        ProjectState projectState = createProjectState();
        String snapshotName = DataStreamLifecycleConvertToFrozen.snapshotName(indexName);
        // GetSnapshots returns a FAILED snapshot
        SnapshotInfo failedSnapshot = new SnapshotInfo(
            new Snapshot(projectId, REPO_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            SnapshotState.FAILED
        );
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(failedSnapshot), null, 1, 0));
        // Delete throws SnapshotMissing (already deleted)
        mockDeleteSnapshotFailure.set(new SnapshotMissingException(REPO_NAME, snapshotName));
        // CreateSnapshot succeeds
        mockCreateSnapshotResponse.set(createSuccessfulSnapshotResponse());

        DataStreamLifecycleConvertToFrozen converter = createConverter(projectState);
        converter.maybeTakeSnapshot(indexName);

        // Should have attempted to delete, then started a new snapshot after SnapshotMissingException
        assertThat(capturedDeleteSnapshotRequest.get(), is(notNullValue()));
        assertThat(capturedCreateSnapshotRequest.get(), is(notNullValue()));
    }

    public void testThrowsWhenDeleteSnapshotFails() {
        ProjectState projectState = createProjectState();
        String snapshotName = DataStreamLifecycleConvertToFrozen.snapshotName(indexName);
        SnapshotInfo failedSnapshot = new SnapshotInfo(
            new Snapshot(projectId, REPO_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            SnapshotState.FAILED
        );
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(failedSnapshot), null, 1, 0));
        // Delete fails with generic exception
        mockDeleteSnapshotFailure.set(new ElasticsearchException("delete snapshot failed"));

        DataStreamLifecycleConvertToFrozen converter = createConverter(projectState);

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> converter.maybeTakeSnapshot(indexName));
        assertThat(exception.getMessage(), containsString("delete snapshot failed"));
    }

    public void testCreateSnapshotResponseWithNullSnapshotInfoThrows() {
        ProjectState projectState = createProjectState();
        // GetSnapshots returns empty
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(), null, 0, 0));
        // CreateSnapshot returns null snapshotInfo (waitForCompletion=true should not do this, but testing defensively)
        mockCreateSnapshotResponse.set(new CreateSnapshotResponse((SnapshotInfo) null));

        DataStreamLifecycleConvertToFrozen converter = createConverter(projectState);

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> converter.maybeTakeSnapshot(indexName));
        assertThat(exception.getMessage(), containsString("failed shards"));
    }

    public void testSnapshotNameFormat() {
        String name = DataStreamLifecycleConvertToFrozen.snapshotName("my-index");
        assertThat(name, is("dlm-frozen-my-index"));
    }

    public void testCreateSnapshotRequestHasCorrectMetadata() {
        ProjectState projectState = createProjectState();
        mockGetSnapshotsResponse.set(new GetSnapshotsResponse(List.of(), null, 0, 0));
        mockCreateSnapshotResponse.set(createSuccessfulSnapshotResponse());

        DataStreamLifecycleConvertToFrozen converter = createConverter(projectState);
        converter.maybeTakeSnapshot(indexName);

        CreateSnapshotRequest req = capturedCreateSnapshotRequest.get();
        assertThat(req, is(notNullValue()));
        Map<String, Object> userMetadata = req.userMetadata();
        assertThat(userMetadata, is(notNullValue()));
        assertThat(userMetadata.get("dlm-managed"), is(Boolean.TRUE));
    }

    public void testSnapshotAtExactTimeoutBoundaryIsNotTimedOut() {
        // Snapshot started exactly 12 hours ago — at the boundary, (clock - start) == timeout, NOT > timeout
        long exactlyAtTimeout = clock.millis() - (12 * 60 * 60 * 1000L);
        ProjectState projectState = createProjectStateWithInProgressSnapshot(exactlyAtTimeout);

        DataStreamLifecycleConvertToFrozen converter = createConverter(projectState);
        converter.maybeTakeSnapshot(indexName);

        // Should NOT delete or restart — the condition is strictly > timeout
        assertThat(capturedDeleteSnapshotRequest.get(), is(nullValue()));
        assertThat(capturedCreateSnapshotRequest.get(), is(nullValue()));
    }

    public void testSnapshotOneMillisOverTimeoutIsTimedOut() {
        // Snapshot started 12h + 1ms ago
        long oneMillisOverTimeout = clock.millis() - (12 * 60 * 60 * 1000L + 1);
        ProjectState projectState = createProjectStateWithInProgressSnapshot(oneMillisOverTimeout);

        mockDeleteSnapshotResponse.set(AcknowledgedResponse.TRUE);
        mockCreateSnapshotResponse.set(createSuccessfulSnapshotResponse());

        DataStreamLifecycleConvertToFrozen converter = createConverter(projectState);
        converter.maybeTakeSnapshot(indexName);

        // Should have deleted and restarted
        assertThat(capturedDeleteSnapshotRequest.get(), is(notNullValue()));
        assertThat(capturedCreateSnapshotRequest.get(), is(notNullValue()));
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
                }
            }
        };
    }

    private DataStreamLifecycleConvertToFrozen createConverter(ProjectState projectState) {
        return new DataStreamLifecycleConvertToFrozen(indexName, createMockClient(), projectState, licenseState, clock);
    }

    private CreateSnapshotResponse createSuccessfulSnapshotResponse() {
        String snapshotName = DataStreamLifecycleConvertToFrozen.snapshotName(indexName);
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

    /**
     * Creates a ProjectState with the target index, a configured repository, and
     * an in-progress snapshot for the index that started at the given time.
     */
    private ProjectState createProjectStateWithInProgressSnapshot(long snapshotStartTime) {
        String snapshotName = DataStreamLifecycleConvertToFrozen.snapshotName(indexName);
        SnapshotsInProgress.Entry entry = SnapshotsInProgress.Entry.snapshot(
            new Snapshot(projectId, REPO_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            false,
            false,
            SnapshotsInProgress.State.STARTED,
            Map.of(),
            List.of(),
            List.of(),
            snapshotStartTime,
            randomNonNegativeLong(),
            Map.of(),
            null,
            Map.of("dlm-managed", true),
            IndexVersion.current()
        );
        SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.EMPTY.withAddedEntry(entry);
        return createProjectState(snapshotsInProgress);
    }

    private ProjectState createProjectState(SnapshotsInProgress snapshotsInProgress) {
        RepositoryMetadata repo = new RepositoryMetadata(REPO_NAME, "fs", Settings.EMPTY);

        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(
                IndexMetadata.builder(indexName)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
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
}
