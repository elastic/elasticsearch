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
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.DataStream;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.datastreams.lifecycle.transitions.steps.CleanupStep.getFrozenIndexName;
import static org.elasticsearch.datastreams.lifecycle.transitions.steps.CloneStep.getDLMCloneIndexName;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CleanupStepTests extends ESTestCase {

    private CleanupStep cleanupStep;
    private ProjectId projectId;
    private String dataStreamName;
    private String indexName;
    private Index index;
    private String frozenIndexName;
    private String cloneIndexName;
    private ThreadPool threadPool;
    private Client client;
    private DataStreamLifecycleErrorStore errorStore;
    private ResultDeduplicator<Tuple<ProjectId, TransportRequest>, Void> deduplicator;
    private AtomicReference<ActionListener<AcknowledgedResponse>> capturedSwapListener;
    private AtomicReference<ActionListener<AcknowledgedResponse>> capturedDeleteListener;
    private AtomicReference<ModifyDataStreamsAction.Request> capturedSwapRequest;
    private AtomicReference<DeleteIndexRequest> capturedDeleteRequest;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        cleanupStep = new CleanupStep();
        projectId = randomProjectIdOrDefault();
        dataStreamName = "test-datastream";
        indexName = randomAlphaOfLength(10);
        index = new Index(indexName, randomAlphaOfLength(10));
        frozenIndexName = getFrozenIndexName(indexName);
        cloneIndexName = getDLMCloneIndexName(indexName);
        errorStore = new DataStreamLifecycleErrorStore(System::currentTimeMillis);
        deduplicator = new ResultDeduplicator<>(threadPool.getThreadContext());
        capturedSwapListener = new AtomicReference<>();
        capturedDeleteListener = new AtomicReference<>();
        capturedSwapRequest = new AtomicReference<>();
        capturedDeleteRequest = new AtomicReference<>();

        client = new NoOpClient(threadPool, TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext())) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof ModifyDataStreamsAction.Request modifyRequest) {
                    capturedSwapRequest.set(modifyRequest);
                    capturedSwapListener.set((ActionListener<AcknowledgedResponse>) listener);
                } else if (request instanceof DeleteIndexRequest deleteRequest) {
                    capturedDeleteRequest.set(deleteRequest);
                    capturedDeleteListener.set((ActionListener<AcknowledgedResponse>) listener);
                }
            }
        };
    }

    @After
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    public void testPossibleOutputIndexNamePatterns() {
        List<String> patterns = cleanupStep.possibleOutputIndexNamePatterns(indexName);
        assertThat(patterns, equalTo(List.of(frozenIndexName)));
    }

    public void testStepNotCompletedWhenOldIndexStillExists() {
        ProjectState projectState = projectStateBuilder().withOldIndexInDataStream().withFrozenIndexInDataStream().build();
        assertFalse(cleanupStep.stepCompleted(index, projectState));
    }

    public void testStepNotCompletedWhenOldIndexDeletedButFrozenNotInDataStream() {
        ProjectState projectState = projectStateBuilder().withFrozenIndex().build();
        assertFalse(cleanupStep.stepCompleted(index, projectState));
    }

    public void testStepNotCompletedWhenCloneIndexStillExists() {
        ProjectState projectState = projectStateBuilder().withFrozenIndexInDataStream().withCloneIndex().build();
        assertFalse(cleanupStep.stepCompleted(index, projectState));
    }

    public void testStepNotCompletedWhenNeitherIndexExists() {
        ProjectState projectState = projectStateBuilder().build();
        assertFalse(cleanupStep.stepCompleted(index, projectState));
    }

    public void testStepCompletedWhenOldIndexDeletedAndFrozenInDataStream() {
        ProjectState projectState = projectStateBuilder().withFrozenIndexInDataStream().build();
        assertTrue(cleanupStep.stepCompleted(index, projectState));
    }

    public void testExecuteSwapsBackingIndexAndIssuesDelete() {
        ProjectState projectState = projectStateBuilder().withOldIndexInDataStream().withFrozenIndexInDataStream().build();
        DlmStepContext stepContext = createStepContext(projectState);

        cleanupStep.execute(stepContext);

        // Should issue the swap request
        assertThat(capturedSwapRequest.get(), is(notNullValue()));
        assertThat(capturedSwapRequest.get().getActions().size(), equalTo(2));
        assertThat(capturedSwapRequest.get().getActions().get(0).getDataStream(), equalTo(dataStreamName));
        assertThat(capturedSwapRequest.get().getActions().get(0).getIndex(), equalTo(indexName));
        assertThat(capturedSwapRequest.get().getActions().get(1).getDataStream(), equalTo(dataStreamName));
        assertThat(capturedSwapRequest.get().getActions().get(1).getIndex(), equalTo(frozenIndexName));

        // Simulate acknowledged swap response
        capturedSwapListener.get().onResponse(AcknowledgedResponse.of(true));

        // After successful swap, should issue delete request for the old index and clone index
        assertThat(capturedDeleteRequest.get(), is(notNullValue()));
        assertThat(capturedDeleteRequest.get().indices(), arrayContainingInAnyOrder(indexName, cloneIndexName));
    }

    public void testExecuteDeleteSucceedsAfterSwap() {
        ProjectState projectState = projectStateBuilder().withOldIndexInDataStream().withFrozenIndexInDataStream().build();
        DlmStepContext stepContext = createStepContext(projectState);

        cleanupStep.execute(stepContext);

        // Simulate acknowledged swap
        capturedSwapListener.get().onResponse(AcknowledgedResponse.of(true));

        // Simulate acknowledged delete
        capturedDeleteListener.get().onResponse(AcknowledgedResponse.of(true));

        // No errors should be recorded
        assertThat(errorStore.getError(projectId, indexName), is(nullValue()));
    }

    public void testExecuteSwapNotAcknowledgedRecordsError() {
        ProjectState projectState = projectStateBuilder().withOldIndexInDataStream().withFrozenIndexInDataStream().build();
        DlmStepContext stepContext = createStepContext(projectState);

        cleanupStep.execute(stepContext);

        // Simulate unacknowledged swap response
        capturedSwapListener.get().onResponse(AcknowledgedResponse.of(false));

        // Error should be recorded
        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(Objects.requireNonNull(errorStore.getError(projectId, indexName)).error(), containsString("failed to acknowledge swap"));

        // Delete should NOT have been issued
        assertThat(capturedDeleteRequest.get(), is(nullValue()));
    }

    public void testExecuteSwapFailureRecordsError() {
        ProjectState projectState = projectStateBuilder().withOldIndexInDataStream().withFrozenIndexInDataStream().build();
        DlmStepContext stepContext = createStepContext(projectState);

        cleanupStep.execute(stepContext);

        // Simulate swap failure
        capturedSwapListener.get().onFailure(new ElasticsearchException("swap failed"));

        // Error should be recorded
        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(Objects.requireNonNull(errorStore.getError(projectId, indexName)).error(), containsString("swap"));

        // Delete should NOT have been issued
        assertThat(capturedDeleteRequest.get(), is(nullValue()));
    }

    public void testExecuteDeleteNotAcknowledgedRecordsError() {
        ProjectState projectState = projectStateBuilder().withOldIndexInDataStream().withFrozenIndexInDataStream().build();
        DlmStepContext stepContext = createStepContext(projectState);

        cleanupStep.execute(stepContext);

        // Simulate acknowledged swap
        capturedSwapListener.get().onResponse(AcknowledgedResponse.of(true));

        // Simulate unacknowledged delete
        capturedDeleteListener.get().onResponse(AcknowledgedResponse.of(false));

        // Error should be recorded
        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(
            Objects.requireNonNull(errorStore.getError(projectId, indexName)).error(),
            containsString("failed to acknowledge delete")
        );
    }

    public void testExecuteDeleteFailureRecordsError() {
        ProjectState projectState = projectStateBuilder().withOldIndexInDataStream().withFrozenIndexInDataStream().build();
        DlmStepContext stepContext = createStepContext(projectState);

        cleanupStep.execute(stepContext);

        // Simulate acknowledged swap
        capturedSwapListener.get().onResponse(AcknowledgedResponse.of(true));

        // Simulate delete failure
        capturedDeleteListener.get().onFailure(new ElasticsearchException("delete failed"));

        // Error should be recorded
        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(Objects.requireNonNull(errorStore.getError(projectId, indexName)).error(), containsString("delete"));
    }

    public void testExecuteSkipsWhenIndexNotInDataStream() {
        ProjectState projectState = projectStateBuilder().withOldIndex().build();
        DlmStepContext stepContext = createStepContext(projectState);

        cleanupStep.execute(stepContext);

        // No requests should have been issued
        assertThat(capturedSwapRequest.get(), is(nullValue()));
        assertThat(capturedDeleteRequest.get(), is(nullValue()));
    }

    /**
     * Creates a new {@link ProjectStateBuilder} for constructing test {@link ProjectState} instances.
     */
    private ProjectStateBuilder projectStateBuilder() {
        return new ProjectStateBuilder();
    }

    private DlmStepContext createStepContext(ProjectState projectState) {
        return new DlmStepContext(index, projectState, deduplicator, errorStore, randomIntBetween(1, 10), client, Clock.systemUTC());
    }

    /**
     * A fluent builder for constructing {@link ProjectState} instances in tests.
     * Allows composing which indices exist and whether they are part of a data stream.
     */
    private class ProjectStateBuilder {
        private boolean includeOldIndex = false;
        private boolean oldIndexInDataStream = false;
        private boolean includeFrozenIndex = false;
        private boolean frozenInDataStream = false;
        private boolean includeCloneIndex = false;

        ProjectStateBuilder withOldIndex() {
            this.includeOldIndex = true;
            return this;
        }

        ProjectStateBuilder withOldIndexInDataStream() {
            this.includeOldIndex = true;
            this.oldIndexInDataStream = true;
            return this;
        }

        ProjectStateBuilder withFrozenIndex() {
            this.includeFrozenIndex = true;
            return this;
        }

        ProjectStateBuilder withFrozenIndexInDataStream() {
            this.includeFrozenIndex = true;
            this.frozenInDataStream = true;
            return this;
        }

        ProjectStateBuilder withCloneIndex() {
            this.includeCloneIndex = true;
            return this;
        }

        ProjectState build() {
            ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);

            IndexMetadata oldIndexMeta = buildIndexMetadata(indexName, index.getUUID());
            IndexMetadata frozenIndexMeta = buildIndexMetadata(frozenIndexName);

            if (includeOldIndex) {
                projectBuilder.put(oldIndexMeta, false);
            }
            if (includeFrozenIndex) {
                projectBuilder.put(frozenIndexMeta, false);
            }
            if (includeCloneIndex) {
                projectBuilder.put(buildIndexMetadata(cloneIndexName), false);
            }

            List<Index> backingIndices = new ArrayList<>();
            if (oldIndexInDataStream) {
                backingIndices.add(oldIndexMeta.getIndex());
            }
            if (frozenInDataStream) {
                backingIndices.add(frozenIndexMeta.getIndex());
            }
            if (backingIndices.isEmpty() == false) {
                DataStream dataStream = DataStream.builder(dataStreamName, backingIndices).setGeneration(backingIndices.size()).build();
                projectBuilder.put(dataStream);
            }

            ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectBuilder).build();
            return clusterState.projectState(projectId);
        }

        private static IndexMetadata buildIndexMetadata(String name) {
            return buildIndexMetadata(name, null);
        }

        private static IndexMetadata buildIndexMetadata(String name, String uuid) {
            Settings.Builder settingsBuilder = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current());
            if (uuid != null) {
                settingsBuilder.put(IndexMetadata.SETTING_INDEX_UUID, uuid);
            }
            return IndexMetadata.builder(name).settings(settingsBuilder.build()).numberOfShards(1).numberOfReplicas(0).build();
        }
    }
}
