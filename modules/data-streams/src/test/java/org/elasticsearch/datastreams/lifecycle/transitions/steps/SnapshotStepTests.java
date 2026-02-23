/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions.steps;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleErrorStore;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStepContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SnapshotStepTests extends ESTestCase {

    private static final String REPOSITORY_NAME = "test-repo";

    private ProjectId projectId;
    private String indexName;
    private Index index;
    private ThreadPool threadPool;
    private Client client;
    private DataStreamLifecycleErrorStore errorStore;
    private ResultDeduplicator<Tuple<ProjectId, TransportRequest>, Void> deduplicator;
    private long currentTime;

    private AtomicReference<ActionListener<?>> capturedListener;
    private AtomicReference<ActionRequest> capturedRequest;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        projectId = randomProjectIdOrDefault();
        indexName = randomAlphaOfLength(10);
        index = new Index(indexName, randomAlphaOfLength(10));
        errorStore = new DataStreamLifecycleErrorStore(System::currentTimeMillis);
        deduplicator = new ResultDeduplicator<>(threadPool.getThreadContext());
        currentTime = System.currentTimeMillis();
        capturedListener = new AtomicReference<>();
        capturedRequest = new AtomicReference<>();

        client = new NoOpClient(threadPool, TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext())) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                capturedRequest.set(request);
                capturedListener.set(listener);
            }
        };
    }

    @After
    public void cleanup() {
        threadPool.shutdownNow();
    }

    public void testStepCompletedWhenSettingIsPresent() {
        ProjectState projectState = createProjectStateWithSnapshotComplete();
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        assertTrue(step.stepCompleted(index, projectState));
    }

    public void testStepNotCompletedWhenSettingIsAbsent() {
        ProjectState projectState = createProjectState();
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        assertFalse(step.stepCompleted(index, projectState));
    }

    public void testStepNotCompletedWhenIndexMissing() {
        ProjectState projectState = createProjectState();
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        Index unknownIndex = new Index("unknown-index", "unknown-uuid");
        assertFalse(step.stepCompleted(unknownIndex, projectState));
    }

    public void testExecuteStartsSnapshotWhenNoPriorSnapshotExists() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        assertThat(capturedRequest.get(), is(notNullValue()));
        assertThat(capturedRequest.get(), instanceOf(GetSnapshotsRequest.class));

        @SuppressWarnings("unchecked")
        ActionListener<GetSnapshotsResponse> getListener = (ActionListener<GetSnapshotsResponse>) capturedListener.get();
        GetSnapshotsResponse emptyResponse = new GetSnapshotsResponse(List.of(), null, 0, 0);
        getListener.onResponse(emptyResponse);

        assertThat(capturedRequest.get(), instanceOf(CreateSnapshotRequest.class));
        CreateSnapshotRequest createRequest = (CreateSnapshotRequest) capturedRequest.get();
        assertThat(createRequest.repository(), is(REPOSITORY_NAME));
        assertThat(createRequest.snapshot(), is(SnapshotStep.SNAPSHOT_NAME_PREFIX + indexName));
        assertThat(createRequest.indices(), is(new String[] { indexName }));
        assertTrue(createRequest.waitForCompletion());
        assertFalse(createRequest.includeGlobalState());
    }

    public void testExecuteMarksCompleteWhenValidOrphanedSnapshotExists() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        assertThat(capturedRequest.get(), instanceOf(GetSnapshotsRequest.class));

        @SuppressWarnings("unchecked")
        ActionListener<GetSnapshotsResponse> getListener = (ActionListener<GetSnapshotsResponse>) capturedListener.get();
        String snapshotName = SnapshotStep.snapshotName(indexName);
        SnapshotInfo existingSnapshot = new SnapshotInfo(
            new Snapshot(projectId, REPOSITORY_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            null,
            0L,
            1,
            List.of(),
            true,
            null,
            0L,
            Map.of()
        );
        getListener.onResponse(new GetSnapshotsResponse(List.of(existingSnapshot), null, 1, 0));

        assertThat(capturedRequest.get(), instanceOf(UpdateSettingsRequest.class));
        UpdateSettingsRequest updateRequest = (UpdateSettingsRequest) capturedRequest.get();
        assertThat(updateRequest.indices(), is(new String[] { indexName }));
        assertTrue(updateRequest.settings().getAsBoolean(SnapshotStep.DLM_SNAPSHOT_COMPLETED_KEY, false));
    }

    public void testExecuteDeletesInvalidOrphanedSnapshotAndRecreates() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        assertThat(capturedRequest.get(), instanceOf(GetSnapshotsRequest.class));

        @SuppressWarnings("unchecked")
        ActionListener<GetSnapshotsResponse> getListener = (ActionListener<GetSnapshotsResponse>) capturedListener.get();
        String snapshotName = SnapshotStep.snapshotName(indexName);
        SnapshotShardFailure shardFailure = new SnapshotShardFailure(
            "node-1",
            new ShardId(indexName, randomAlphaOfLength(10), 0),
            "shard failed"
        );
        SnapshotInfo existingSnapshot = new SnapshotInfo(
            new Snapshot(projectId, REPOSITORY_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            null,
            0L,
            1,
            List.of(shardFailure),
            true,
            null,
            0L,
            Map.of()
        );
        getListener.onResponse(new GetSnapshotsResponse(List.of(existingSnapshot), null, 1, 0));

        assertThat(capturedRequest.get(), instanceOf(DeleteSnapshotRequest.class));
        DeleteSnapshotRequest deleteRequest = (DeleteSnapshotRequest) capturedRequest.get();
        assertThat(deleteRequest.repository(), is(REPOSITORY_NAME));
        assertThat(deleteRequest.snapshots(), is(new String[] { snapshotName }));

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> deleteListener = (ActionListener<AcknowledgedResponse>) capturedListener.get();
        deleteListener.onResponse(AcknowledgedResponse.TRUE);

        assertThat(capturedRequest.get(), instanceOf(CreateSnapshotRequest.class));
    }

    public void testExecuteHandlesGetSnapshotFailure() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        @SuppressWarnings("unchecked")
        ActionListener<GetSnapshotsResponse> getListener = (ActionListener<GetSnapshotsResponse>) capturedListener.get();
        getListener.onFailure(new ElasticsearchException("repository unavailable"));

        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(errorStore.getError(projectId, indexName).error(), containsString("repository unavailable"));
    }

    public void testExecuteHandlesSnapshotMissingExceptionOnGet() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        @SuppressWarnings("unchecked")
        ActionListener<GetSnapshotsResponse> getListener = (ActionListener<GetSnapshotsResponse>) capturedListener.get();
        getListener.onFailure(new SnapshotMissingException(REPOSITORY_NAME, "missing"));

        assertThat(capturedRequest.get(), instanceOf(CreateSnapshotRequest.class));
    }

    public void testExecuteHandlesSnapshotMissingExceptionOnDelete() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        @SuppressWarnings("unchecked")
        ActionListener<GetSnapshotsResponse> getListener = (ActionListener<GetSnapshotsResponse>) capturedListener.get();
        String snapshotName = SnapshotStep.snapshotName(indexName);
        SnapshotInfo existingSnapshot = createInvalidSnapshotInfo(snapshotName);
        getListener.onResponse(new GetSnapshotsResponse(List.of(existingSnapshot), null, 1, 0));

        assertThat(capturedRequest.get(), instanceOf(DeleteSnapshotRequest.class));

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> deleteListener = (ActionListener<AcknowledgedResponse>) capturedListener.get();
        deleteListener.onFailure(new SnapshotMissingException(REPOSITORY_NAME, snapshotName));

        assertThat(capturedRequest.get(), instanceOf(CreateSnapshotRequest.class));
    }

    public void testExecuteHandlesDeleteFailure() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        @SuppressWarnings("unchecked")
        ActionListener<GetSnapshotsResponse> getListener = (ActionListener<GetSnapshotsResponse>) capturedListener.get();
        String snapshotName = SnapshotStep.snapshotName(indexName);
        SnapshotInfo existingSnapshot = createInvalidSnapshotInfo(snapshotName);
        getListener.onResponse(new GetSnapshotsResponse(List.of(existingSnapshot), null, 1, 0));

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> deleteListener = (ActionListener<AcknowledgedResponse>) capturedListener.get();
        deleteListener.onFailure(new ElasticsearchException("delete failed"));

        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(errorStore.getError(projectId, indexName).error(), containsString("delete failed"));
    }

    public void testExecuteSkipsWhenSnapshotInProgressAndNotTimedOut() {
        long snapshotStartTime = currentTime - TimeValue.timeValueHours(1).millis();
        ProjectState projectState = createProjectStateWithSnapshotInProgress(snapshotStartTime);
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        assertThat(capturedRequest.get(), is(nullValue()));
        assertThat(errorStore.getError(projectId, indexName), is(nullValue()));
    }

    public void testExecuteRestartsWhenSnapshotInProgressAndTimedOut() {
        long snapshotStartTime = currentTime - TimeValue.timeValueHours(13).millis();
        ProjectState projectState = createProjectStateWithSnapshotInProgress(snapshotStartTime);
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        assertThat(capturedRequest.get(), is(notNullValue()));
        assertThat(capturedRequest.get(), instanceOf(DeleteSnapshotRequest.class));
    }

    public void testStepNotCompletedWhenSettingExplicitlyFalse() {
        ProjectState projectState = createProjectState(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(SnapshotStep.DLM_SNAPSHOT_COMPLETED_KEY, false)
                .build()
        );
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        assertFalse(step.stepCompleted(index, projectState));
    }

    public void testExecuteRestartsTimedOutSnapshotFullChain() {
        long snapshotStartTime = currentTime - TimeValue.timeValueHours(13).millis();
        ProjectState projectState = createProjectStateWithSnapshotInProgress(snapshotStartTime);
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        assertThat(capturedRequest.get(), instanceOf(DeleteSnapshotRequest.class));
        DeleteSnapshotRequest deleteRequest = (DeleteSnapshotRequest) capturedRequest.get();
        String snapshotName = SnapshotStep.snapshotName(indexName);
        assertThat(deleteRequest.repository(), is(REPOSITORY_NAME));
        assertThat(deleteRequest.snapshots(), is(new String[] { snapshotName }));

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> deleteListener = (ActionListener<AcknowledgedResponse>) capturedListener.get();
        deleteListener.onResponse(AcknowledgedResponse.TRUE);

        assertThat(capturedRequest.get(), instanceOf(CreateSnapshotRequest.class));
        CreateSnapshotRequest createRequest = (CreateSnapshotRequest) capturedRequest.get();
        assertThat(createRequest.repository(), is(REPOSITORY_NAME));
        assertThat(createRequest.snapshot(), is(snapshotName));
        assertThat(createRequest.indices(), is(new String[] { indexName }));
        assertTrue(createRequest.waitForCompletion());
        assertFalse(createRequest.includeGlobalState());
    }

    public void testSnapshotNameDerivation() {
        assertThat(SnapshotStep.snapshotName("my-index"), is("dlm-frozen-my-index"));
    }

    public void testStepName() {
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        assertThat(step.stepName(), is("Snapshot Index"));
    }

    public void testSuccessfulSnapshotMarksSettingComplete() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        @SuppressWarnings("unchecked")
        ActionListener<GetSnapshotsResponse> getListener = (ActionListener<GetSnapshotsResponse>) capturedListener.get();
        getListener.onResponse(new GetSnapshotsResponse(List.of(), null, 0, 0));

        assertThat(capturedRequest.get(), instanceOf(CreateSnapshotRequest.class));

        @SuppressWarnings("unchecked")
        ActionListener<CreateSnapshotResponse> createListener = (ActionListener<CreateSnapshotResponse>) capturedListener.get();
        String snapshotName = SnapshotStep.snapshotName(indexName);
        SnapshotInfo snapshotInfo = new SnapshotInfo(
            new Snapshot(projectId, REPOSITORY_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            null,
            0L,
            1,
            List.of(),
            true,
            null,
            0L,
            Map.of()
        );
        createListener.onResponse(new CreateSnapshotResponse(snapshotInfo));

        assertThat(capturedRequest.get(), instanceOf(UpdateSettingsRequest.class));
        UpdateSettingsRequest updateRequest = (UpdateSettingsRequest) capturedRequest.get();
        assertThat(updateRequest.indices(), is(new String[] { indexName }));
        assertTrue(updateRequest.settings().getAsBoolean(SnapshotStep.DLM_SNAPSHOT_COMPLETED_KEY, false));
    }

    public void testSnapshotWithFailedShardsRecordsError() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        @SuppressWarnings("unchecked")
        ActionListener<GetSnapshotsResponse> getListener = (ActionListener<GetSnapshotsResponse>) capturedListener.get();
        getListener.onResponse(new GetSnapshotsResponse(List.of(), null, 0, 0));

        @SuppressWarnings("unchecked")
        ActionListener<CreateSnapshotResponse> createListener = (ActionListener<CreateSnapshotResponse>) capturedListener.get();
        String snapshotName = SnapshotStep.snapshotName(indexName);
        SnapshotShardFailure shardFailure = new SnapshotShardFailure(
            "node-1",
            new ShardId(indexName, randomAlphaOfLength(10), 0),
            "shard failed"
        );
        SnapshotInfo snapshotInfo = new SnapshotInfo(
            new Snapshot(projectId, REPOSITORY_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            null,
            0L,
            1,
            List.of(shardFailure),
            true,
            null,
            0L,
            Map.of()
        );
        createListener.onResponse(new CreateSnapshotResponse(snapshotInfo));

        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(errorStore.getError(projectId, indexName).error(), containsString("failed shards"));
    }

    public void testSnapshotCreationFailureRecordsError() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        @SuppressWarnings("unchecked")
        ActionListener<GetSnapshotsResponse> getListener = (ActionListener<GetSnapshotsResponse>) capturedListener.get();
        getListener.onResponse(new GetSnapshotsResponse(List.of(), null, 0, 0));

        @SuppressWarnings("unchecked")
        ActionListener<CreateSnapshotResponse> createListener = (ActionListener<CreateSnapshotResponse>) capturedListener.get();
        createListener.onFailure(new ElasticsearchException("snapshot creation failed"));

        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(errorStore.getError(projectId, indexName).error(), containsString("snapshot creation failed"));
    }

    public void testUpdateSettingsFailureRecordsError() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        @SuppressWarnings("unchecked")
        ActionListener<GetSnapshotsResponse> getListener = (ActionListener<GetSnapshotsResponse>) capturedListener.get();
        getListener.onResponse(new GetSnapshotsResponse(List.of(), null, 0, 0));

        @SuppressWarnings("unchecked")
        ActionListener<CreateSnapshotResponse> createListener = (ActionListener<CreateSnapshotResponse>) capturedListener.get();
        String snapshotName = SnapshotStep.snapshotName(indexName);
        SnapshotInfo snapshotInfo = new SnapshotInfo(
            new Snapshot(projectId, REPOSITORY_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            null,
            0L,
            1,
            List.of(),
            true,
            null,
            0L,
            Map.of()
        );
        createListener.onResponse(new CreateSnapshotResponse(snapshotInfo));

        assertThat(capturedRequest.get(), instanceOf(UpdateSettingsRequest.class));

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> updateListener = (ActionListener<AcknowledgedResponse>) capturedListener.get();
        updateListener.onFailure(new ElasticsearchException("settings update failed"));

        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(errorStore.getError(projectId, indexName).error(), containsString("settings update failed"));
    }

    public void testUpdateSettingsNotAcknowledgedRecordsError() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        @SuppressWarnings("unchecked")
        ActionListener<GetSnapshotsResponse> getListener = (ActionListener<GetSnapshotsResponse>) capturedListener.get();
        getListener.onResponse(new GetSnapshotsResponse(List.of(), null, 0, 0));

        @SuppressWarnings("unchecked")
        ActionListener<CreateSnapshotResponse> createListener = (ActionListener<CreateSnapshotResponse>) capturedListener.get();
        String snapshotName = SnapshotStep.snapshotName(indexName);
        SnapshotInfo snapshotInfo = new SnapshotInfo(
            new Snapshot(projectId, REPOSITORY_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            null,
            0L,
            1,
            List.of(),
            true,
            null,
            0L,
            Map.of()
        );
        createListener.onResponse(new CreateSnapshotResponse(snapshotInfo));

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> updateListener = (ActionListener<AcknowledgedResponse>) capturedListener.get();
        updateListener.onResponse(AcknowledgedResponse.of(false));

        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(errorStore.getError(projectId, indexName).error(), containsString("not acknowledged"));
    }

    public void testSnapshotWithNullSnapshotInfoRecordsError() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        @SuppressWarnings("unchecked")
        ActionListener<GetSnapshotsResponse> getListener = (ActionListener<GetSnapshotsResponse>) capturedListener.get();
        getListener.onResponse(new GetSnapshotsResponse(List.of(), null, 0, 0));

        @SuppressWarnings("unchecked")
        ActionListener<CreateSnapshotResponse> createListener = (ActionListener<CreateSnapshotResponse>) capturedListener.get();
        createListener.onResponse(new CreateSnapshotResponse((SnapshotInfo) null));

        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(errorStore.getError(projectId, indexName).error(), containsString("failed shards"));
    }

    public void testSuccessfulSnapshotAcknowledgedCompletes() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        @SuppressWarnings("unchecked")
        ActionListener<GetSnapshotsResponse> getListener = (ActionListener<GetSnapshotsResponse>) capturedListener.get();
        getListener.onResponse(new GetSnapshotsResponse(List.of(), null, 0, 0));

        @SuppressWarnings("unchecked")
        ActionListener<CreateSnapshotResponse> createListener = (ActionListener<CreateSnapshotResponse>) capturedListener.get();
        String snapshotName = SnapshotStep.snapshotName(indexName);
        SnapshotInfo snapshotInfo = new SnapshotInfo(
            new Snapshot(projectId, REPOSITORY_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            null,
            0L,
            1,
            List.of(),
            true,
            null,
            0L,
            Map.of()
        );
        createListener.onResponse(new CreateSnapshotResponse(snapshotInfo));

        assertThat(capturedRequest.get(), instanceOf(UpdateSettingsRequest.class));

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> updateListener = (ActionListener<AcknowledgedResponse>) capturedListener.get();
        updateListener.onResponse(AcknowledgedResponse.TRUE);

        assertThat(errorStore.getError(projectId, indexName), is(nullValue()));
    }

    public void testOrphanedSnapshotMarkCompleteSuccessClearsError() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        errorStore.recordError(projectId, indexName, new ElasticsearchException("previous error"));
        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));

        step.execute(stepContext);

        @SuppressWarnings("unchecked")
        ActionListener<GetSnapshotsResponse> getListener = (ActionListener<GetSnapshotsResponse>) capturedListener.get();
        String snapshotName = SnapshotStep.snapshotName(indexName);
        SnapshotInfo existingSnapshot = new SnapshotInfo(
            new Snapshot(projectId, REPOSITORY_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            null,
            0L,
            1,
            List.of(),
            true,
            null,
            0L,
            Map.of()
        );
        getListener.onResponse(new GetSnapshotsResponse(List.of(existingSnapshot), null, 1, 0));

        assertThat(capturedRequest.get(), instanceOf(UpdateSettingsRequest.class));

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> updateListener = (ActionListener<AcknowledgedResponse>) capturedListener.get();
        updateListener.onResponse(AcknowledgedResponse.TRUE);

        assertThat(errorStore.getError(projectId, indexName), is(nullValue()));
    }

    public void testOrphanedSnapshotMarkCompleteFailureRecordsError() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        @SuppressWarnings("unchecked")
        ActionListener<GetSnapshotsResponse> getListener = (ActionListener<GetSnapshotsResponse>) capturedListener.get();
        String snapshotName = SnapshotStep.snapshotName(indexName);
        SnapshotInfo existingSnapshot = new SnapshotInfo(
            new Snapshot(projectId, REPOSITORY_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            null,
            0L,
            1,
            List.of(),
            true,
            null,
            0L,
            Map.of()
        );
        getListener.onResponse(new GetSnapshotsResponse(List.of(existingSnapshot), null, 1, 0));

        assertThat(capturedRequest.get(), instanceOf(UpdateSettingsRequest.class));

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> updateListener = (ActionListener<AcknowledgedResponse>) capturedListener.get();
        updateListener.onFailure(new ElasticsearchException("settings update failed"));

        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(errorStore.getError(projectId, indexName).error(), containsString("settings update failed"));
    }

    public void testOrphanedSnapshotMarkCompleteNotAcknowledgedRecordsError() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);
        SnapshotStep step = createStep(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));

        step.execute(stepContext);

        @SuppressWarnings("unchecked")
        ActionListener<GetSnapshotsResponse> getListener = (ActionListener<GetSnapshotsResponse>) capturedListener.get();
        String snapshotName = SnapshotStep.snapshotName(indexName);
        SnapshotInfo existingSnapshot = new SnapshotInfo(
            new Snapshot(projectId, REPOSITORY_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            null,
            0L,
            1,
            List.of(),
            true,
            null,
            0L,
            Map.of()
        );
        getListener.onResponse(new GetSnapshotsResponse(List.of(existingSnapshot), null, 1, 0));

        assertThat(capturedRequest.get(), instanceOf(UpdateSettingsRequest.class));

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> updateListener = (ActionListener<AcknowledgedResponse>) capturedListener.get();
        updateListener.onResponse(AcknowledgedResponse.of(false));

        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(errorStore.getError(projectId, indexName).error(), containsString("not acknowledged"));
    }

    private SnapshotStep createStep(Clock clock) {
        return new SnapshotStep(clock);
    }

    private ProjectState createProjectState() {
        return createProjectState(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build());
    }

    private ProjectState createProjectStateWithSnapshotComplete() {
        return createProjectState(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(SnapshotStep.DLM_SNAPSHOT_COMPLETED_KEY, true)
                .build()
        );
    }

    private ProjectState createProjectState(Settings indexSettings) {
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(IndexMetadata.builder(indexName).settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), false);

        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .persistentSettings(
                        Settings.builder().put(RepositoriesService.DEFAULT_REPOSITORY_SETTING.getKey(), REPOSITORY_NAME).build()
                    )
            )
            .putProjectMetadata(projectMetadataBuilder)
            .build()
            .projectState(projectId);
    }

    private ProjectState createProjectStateWithSnapshotInProgress(long snapshotStartTime) {
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(
                IndexMetadata.builder(indexName)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build(),
                false
            );

        String snapshotName = SnapshotStep.snapshotName(indexName);
        Snapshot snapshot = new Snapshot(projectId, REPOSITORY_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10)));
        SnapshotsInProgress.Entry entry = SnapshotsInProgress.startedEntry(
            snapshot,
            false,
            false,
            Collections.emptyMap(),
            Collections.emptyList(),
            snapshotStartTime,
            0L,
            Collections.emptyMap(),
            Collections.emptyMap(),
            IndexVersion.current(),
            Collections.emptyList()
        );

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .persistentSettings(
                        Settings.builder().put(RepositoriesService.DEFAULT_REPOSITORY_SETTING.getKey(), REPOSITORY_NAME).build()
                    )
            )
            .putProjectMetadata(projectMetadataBuilder)
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(entry))
            .build();

        return clusterState.projectState(projectId);
    }

    private DlmStepContext createStepContext(ProjectState projectState) {
        return new DlmStepContext(
            index,
            projectState,
            deduplicator,
            errorStore,
            randomIntBetween(1, 10),
            client,
            Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC)
        );
    }

    /**
     * Creates a {@link SnapshotInfo} with a shard failure, making it invalid for the "mark complete directly" path.
     */
    private SnapshotInfo createInvalidSnapshotInfo(String snapshotName) {
        SnapshotShardFailure shardFailure = new SnapshotShardFailure(
            "node-1",
            new ShardId(indexName, randomAlphaOfLength(10), 0),
            "shard failed"
        );
        return new SnapshotInfo(
            new Snapshot(projectId, REPOSITORY_NAME, new SnapshotId(snapshotName, randomAlphaOfLength(10))),
            List.of(indexName),
            List.of(),
            List.of(),
            null,
            0L,
            1,
            List.of(shardFailure),
            true,
            null,
            0L,
            Map.of()
        );
    }
}
