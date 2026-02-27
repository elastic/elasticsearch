/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions.steps;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
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

import java.time.Clock;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.datastreams.lifecycle.transitions.steps.ForceMergeStep.DLM_FORCE_MERGE_COMPLETE_SETTING;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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

    public void testStepName() {
        assertThat(forceMergeStep.stepName(), is("Force Merge Index"));
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
