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
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleErrorStore;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStepContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.datastreams.lifecycle.transitions.steps.ForceMergeStep.DLM_FORCE_MERGE_COMPLETE_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ForceMergeStepTests extends ESTestCase {

    private ForceMergeStep forceMergeStep;
    private ProjectId projectId;
    private String indexName;
    private String indexUuid;
    private Index index;
    private ThreadPool threadPool;
    private Client client;
    private DataStreamLifecycleErrorStore errorStore;
    private ResultDeduplicator<Tuple<ProjectId, TransportRequest>, Void> deduplicator;
    private AtomicReference<ActionListener<AcknowledgedResponse>> capturedListener;
    private AtomicReference<UpdateSettingsRequest> capturedRequest;
    private AtomicReference<ActionListener<BroadcastResponse>> capturedForceMergeListener;
    private AtomicReference<ForceMergeRequest> capturedForceMergeRequest;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        forceMergeStep = new ForceMergeStep();
        projectId = randomProjectIdOrDefault();
        indexName = randomAlphaOfLength(10);
        indexUuid = randomAlphaOfLength(10);
        index = new Index(indexName, indexUuid);
        errorStore = new DataStreamLifecycleErrorStore(System::currentTimeMillis);
        deduplicator = new ResultDeduplicator<>(threadPool.getThreadContext());
        capturedListener = new AtomicReference<>();
        capturedRequest = new AtomicReference<>();
        capturedForceMergeListener = new AtomicReference<>();
        capturedForceMergeRequest = new AtomicReference<>();

        client = new NoOpClient(threadPool, TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext())) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof UpdateSettingsRequest) {
                    capturedRequest.set((UpdateSettingsRequest) request);
                    capturedListener.set((ActionListener<AcknowledgedResponse>) listener);
                } else if (request instanceof ForceMergeRequest) {
                    capturedForceMergeRequest.set((ForceMergeRequest) request);
                    capturedForceMergeListener.set((ActionListener<BroadcastResponse>) listener);
                }
            }
        };
    }

    @After
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    public void testStepCompletedWhenForceMergeSettingIsTrue() {
        ProjectState projectState = createProjectStateWithSetting(true);
        assertTrue(forceMergeStep.stepCompleted(index, projectState));
    }

    public void testStepNotCompletedWhenForceMergeSettingIsFalse() {
        ProjectState projectState = createProjectStateWithSetting(false);
        assertFalse(forceMergeStep.stepCompleted(index, projectState));
    }

    public void testStepNotCompletedWhenForceMergeSettingIsAbsent() {
        ProjectState projectState = createProjectState();
        assertFalse(forceMergeStep.stepCompleted(index, projectState));
    }

    public void testMarkDLMForceMergeCompleteHappyCase() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);

        forceMergeStep.markDLMForceMergeComplete(stepContext, ActionListener.noop());

        assertThat(capturedRequest.get(), is(notNullValue()));
        assertThat(capturedRequest.get().indices(), is(notNullValue()));
        assertThat(capturedRequest.get().indices().length, is(1));
        assertThat(capturedRequest.get().indices()[0], is(indexName));

        Settings settings = capturedRequest.get().settings();
        assertThat(DLM_FORCE_MERGE_COMPLETE_SETTING.get(settings), is(true));
    }

    public void testMaybeForceMergeSubmitsForceMergeRequest() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);

        forceMergeStep.maybeForceMerge(indexName, stepContext);

        assertThat(capturedForceMergeRequest.get(), is(notNullValue()));
        assertThat(capturedForceMergeRequest.get().indices().length, is(1));
        assertThat(capturedForceMergeRequest.get().indices()[0], is(indexName));
        assertThat(capturedForceMergeRequest.get().maxNumSegments(), is(1));
    }

    public void testMaybeForceMergeSuccessClearsErrorRecord() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);

        // Pre-populate the error store so we can verify it gets cleared on success
        errorStore.recordError(projectId, indexName, new RuntimeException("previous error"));
        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));

        forceMergeStep.maybeForceMerge(indexName, stepContext);

        BroadcastResponse response = Mockito.mock(BroadcastResponse.class);
        Mockito.when(response.getFailedShards()).thenReturn(0);
        capturedForceMergeListener.get().onResponse(response);

        // ErrorRecordingActionListener.onResponse clears the error record
        assertThat(errorStore.getError(projectId, indexName), is(nullValue()));
    }

    public void testMaybeForceMergeRecordsErrorOnListenerFailure() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);

        forceMergeStep.maybeForceMerge(indexName, stepContext);

        RuntimeException failure = new RuntimeException("force merge transport failure");
        capturedForceMergeListener.get().onFailure(failure);

        // The deduplicator's ErrorRecordingActionListener should have stored the error
        var errorRecord = errorStore.getError(projectId, indexName);
        assertNotNull(errorRecord);
        assertThat(errorRecord.error(), containsString("force merge transport failure"));
    }

    public void testForceMergeFailsWhenShardsHaveFailures() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);
        ForceMergeRequest forceMergeRequest = new ForceMergeRequest(indexName);

        AtomicReference<Exception> capturedFailure = new AtomicReference<>();
        forceMergeStep.forceMerge(projectId, forceMergeRequest, ActionListener.wrap(v -> {
            throw new AssertionError("expected failure but got success");
        }, capturedFailure::set), stepContext);

        DefaultShardOperationFailedException shardFailure = new DefaultShardOperationFailedException(
            indexName,
            0,
            new IllegalStateException("shard merge failed")
        );
        BroadcastResponse response = Mockito.mock(BroadcastResponse.class);
        Mockito.when(response.getFailedShards()).thenReturn(1);
        Mockito.when(response.getShardFailures()).thenReturn(new DefaultShardOperationFailedException[] { shardFailure });
        capturedForceMergeListener.get().onResponse(response);

        assertThat(capturedFailure.get(), is(notNullValue()));
        assertThat(capturedFailure.get(), instanceOf(ElasticsearchException.class));
        assertThat(capturedFailure.get().getMessage(), containsString(indexName));
        assertThat(capturedFailure.get().getMessage(), containsString("DLM failed while force merging"));
    }

    private ProjectState createProjectState() {
        return buildProjectState(Settings.EMPTY);
    }

    private ProjectState createProjectStateWithSetting(boolean forceMergeComplete) {
        return buildProjectState(Settings.builder().put(DLM_FORCE_MERGE_COMPLETE_SETTING.getKey(), forceMergeComplete).build());
    }

    private ProjectState buildProjectState(Settings additionalSettings) {
        IndexMetadata indexMetadata = buildIndexMetadata(additionalSettings);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(indexMetadata, false))
            .build();
        return clusterState.projectState(projectId);
    }

    private IndexMetadata buildIndexMetadata(Settings additionalSettings) {
        return IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
                    .put(additionalSettings)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    private DlmStepContext createStepContext(ProjectState projectState) {
        return new DlmStepContext(index, projectState, deduplicator, errorStore, randomIntBetween(1, 10), client, Clock.systemUTC());
    }
}
