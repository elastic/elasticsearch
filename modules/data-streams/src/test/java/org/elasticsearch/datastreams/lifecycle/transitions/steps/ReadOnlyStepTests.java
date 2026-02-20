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
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleErrorStore;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStepContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ReadOnlyStepTests extends ESTestCase {

    private ReadOnlyStep readOnlyStep;
    private ProjectId projectId;
    private String indexName;
    private Index index;
    private ThreadPool threadPool;
    private Client client;
    private DataStreamLifecycleErrorStore errorStore;
    private ResultDeduplicator<Tuple<ProjectId, TransportRequest>, Void> deduplicator;
    private AtomicReference<ActionListener<AddIndexBlockResponse>> capturedListener;
    private AtomicReference<AddIndexBlockRequest> capturedRequest;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        readOnlyStep = new ReadOnlyStep();
        projectId = randomProjectIdOrDefault();
        indexName = randomAlphaOfLength(10);
        index = new Index(indexName, randomAlphaOfLength(10));
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
                if (request instanceof AddIndexBlockRequest) {
                    capturedRequest.set((AddIndexBlockRequest) request);
                    capturedListener.set((ActionListener<AddIndexBlockResponse>) listener);
                }
            }
        };
    }

    @After
    public void cleanup() {
        threadPool.shutdownNow();
    }

    public void testStepCompletedWhenIndexHasWriteBlock() {
        ClusterBlock writeBlock = WRITE.getBlock();

        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(
                IndexMetadata.builder(indexName)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build(),
                false
            );

        ClusterBlocks clusterBlocks = ClusterBlocks.builder().addIndexBlock(projectId, indexName, writeBlock).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(projectMetadataBuilder)
            .blocks(clusterBlocks)
            .build();
        ProjectState projectState = clusterState.projectState(projectId);

        assertTrue(readOnlyStep.stepCompleted(index, projectState));
    }

    public void testStepNotCompletedWhenIndexHasNoWriteBlock() {
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(
                IndexMetadata.builder(indexName)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build(),
                false
            );

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectMetadataBuilder).build();
        ProjectState projectState = clusterState.projectState(projectId);

        assertFalse(readOnlyStep.stepCompleted(index, projectState));
    }

    public void testExecuteCallsAddBlockWithCorrectParameters() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);

        readOnlyStep.execute(stepContext);

        assertThat(capturedListener.get(), is(notNullValue()));

        // Simulate successful response
        AddIndexBlockResponse response = new AddIndexBlockResponse(true, true, List.of(new AddIndexBlockResponse.AddBlockResult(index)));
        capturedListener.get().onResponse(response);

        // Error should be empty
        assertThat(errorStore.getError(projectId, indexName), is(nullValue()));
    }

    public void testExecuteWithAcknowledgedResponseClearsError() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);

        // Pre-populate error store
        errorStore.recordError(projectId, indexName, new RuntimeException("previous error"));
        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));

        readOnlyStep.execute(stepContext);

        AddIndexBlockResponse response = new AddIndexBlockResponse(true, true, List.of(new AddIndexBlockResponse.AddBlockResult(index)));
        capturedListener.get().onResponse(response);

        // Error should be cleared
        assertThat(errorStore.getError(projectId, indexName), is(nullValue()));
    }

    public void testExecuteWithUnacknowledgedResponseRecordsError() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);

        readOnlyStep.execute(stepContext);

        // Unacknowledged response without index result
        AddIndexBlockResponse response = new AddIndexBlockResponse(false, false, Collections.emptyList());
        capturedListener.get().onResponse(response);

        // Error should be recorded
        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(Objects.requireNonNull(errorStore.getError(projectId, indexName)).error(), containsString("not acknowledged"));
    }

    public void testExecuteWithUnacknowledgedResponseWithIndexResult() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);

        readOnlyStep.execute(stepContext);

        // Unacknowledged response with index result but no failures
        AddIndexBlockResponse response = new AddIndexBlockResponse(false, false, List.of(new AddIndexBlockResponse.AddBlockResult(index)));
        capturedListener.get().onResponse(response);

        // Error should be recorded
        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(Objects.requireNonNull(errorStore.getError(projectId, indexName)).error(), containsString("not acknowledged"));
    }

    public void testExecuteWithGlobalExceptionInBlockResult() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);

        readOnlyStep.execute(stepContext);

        ElasticsearchException exception = new ElasticsearchException("global failure");
        AddIndexBlockResponse response = new AddIndexBlockResponse(
            false,
            false,
            List.of(new AddIndexBlockResponse.AddBlockResult(index, exception))
        );
        capturedListener.get().onResponse(response);

        // Error should be recorded
        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(Objects.requireNonNull(errorStore.getError(projectId, indexName)).error(), containsString("global failure"));
    }

    public void testExecuteWithShardFailuresInBlockResult() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);

        readOnlyStep.execute(stepContext);

        AddIndexBlockResponse.AddBlockShardResult.Failure shardFailure = new AddIndexBlockResponse.AddBlockShardResult.Failure(
            indexName,
            0,
            new ElasticsearchException("shard failure")
        );

        AddIndexBlockResponse.AddBlockShardResult[] shardResults = new AddIndexBlockResponse.AddBlockShardResult[] {
            new AddIndexBlockResponse.AddBlockShardResult(0, new AddIndexBlockResponse.AddBlockShardResult.Failure[] { shardFailure }) };

        AddIndexBlockResponse response = new AddIndexBlockResponse(
            false,
            false,
            List.of(new AddIndexBlockResponse.AddBlockResult(index, shardResults))
        );
        capturedListener.get().onResponse(response);

        // Error should be recorded with shard failure details
        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(Objects.requireNonNull(errorStore.getError(projectId, indexName)).error(), containsString("shard failure"));
    }

    public void testExecuteWithIndexNotFoundExceptionClearsError() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);

        // Pre-populate error store
        errorStore.recordError(projectId, indexName, new RuntimeException("previous error"));
        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));

        readOnlyStep.execute(stepContext);

        capturedListener.get().onFailure(new IndexNotFoundException(indexName));

        // Error should be cleared since index was deleted
        assertThat(errorStore.getError(projectId, indexName), is(nullValue()));
    }

    public void testExecuteWithGenericFailureRecordsError() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);

        readOnlyStep.execute(stepContext);

        capturedListener.get().onFailure(new ElasticsearchException("some error"));

        // Error should be recorded
        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(Objects.requireNonNull(errorStore.getError(projectId, indexName)).error(), containsString("some error"));
    }

    public void testStepCompletedWithDifferentBlocks() {
        // Test that only WRITE block makes the step completed
        ClusterBlock readBlock = IndexMetadata.APIBlock.READ.getBlock();

        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(
                IndexMetadata.builder(indexName)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build(),
                false
            );

        ClusterBlocks clusterBlocks = ClusterBlocks.builder().addIndexBlock(projectId, indexName, readBlock).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(projectMetadataBuilder)
            .blocks(clusterBlocks)
            .build();
        ProjectState projectState = clusterState.projectState(projectId);

        // READ block should not make the step completed, only WRITE block should
        assertFalse(readOnlyStep.stepCompleted(index, projectState));
    }

    public void testExecuteWithMultipleShardFailures() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);

        readOnlyStep.execute(stepContext);

        AddIndexBlockResponse.AddBlockShardResult.Failure shardFailure1 = new AddIndexBlockResponse.AddBlockShardResult.Failure(
            indexName,
            0,
            new ElasticsearchException("shard 0 failure")
        );

        AddIndexBlockResponse.AddBlockShardResult.Failure shardFailure2 = new AddIndexBlockResponse.AddBlockShardResult.Failure(
            indexName,
            1,
            new ElasticsearchException("shard 1 failure")
        );

        AddIndexBlockResponse.AddBlockShardResult[] shardResults = new AddIndexBlockResponse.AddBlockShardResult[] {
            new AddIndexBlockResponse.AddBlockShardResult(0, new AddIndexBlockResponse.AddBlockShardResult.Failure[] { shardFailure1 }),
            new AddIndexBlockResponse.AddBlockShardResult(1, new AddIndexBlockResponse.AddBlockShardResult.Failure[] { shardFailure2 }) };

        AddIndexBlockResponse response = new AddIndexBlockResponse(
            false,
            false,
            List.of(new AddIndexBlockResponse.AddBlockResult(index, shardResults))
        );
        capturedListener.get().onResponse(response);

        // Error should be recorded with both shard failures
        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        String errorMessage = Objects.requireNonNull(errorStore.getError(projectId, indexName)).error();
        assertThat(errorMessage, containsString("shard 0 failure"));
        assertThat(errorMessage, containsString("shard 1 failure"));
    }

    public void testExecuteWithShardResultsButNoFailures() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);

        readOnlyStep.execute(stepContext);

        // Create shard results with no failures (empty failure arrays)
        AddIndexBlockResponse.AddBlockShardResult[] shardResults = new AddIndexBlockResponse.AddBlockShardResult[] {
            new AddIndexBlockResponse.AddBlockShardResult(0, new AddIndexBlockResponse.AddBlockShardResult.Failure[0]),
            new AddIndexBlockResponse.AddBlockShardResult(1, new AddIndexBlockResponse.AddBlockShardResult.Failure[0]) };

        // Response is not acknowledged, has index result with shards, but no failures
        AddIndexBlockResponse response = new AddIndexBlockResponse(
            false,
            false,
            List.of(new AddIndexBlockResponse.AddBlockResult(index, shardResults))
        );
        capturedListener.get().onResponse(response);

        // Error should be recorded because response was not acknowledged
        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(Objects.requireNonNull(errorStore.getError(projectId, indexName)).error(), containsString("not acknowledged"));
    }

    public void testAddIndexBlockRequestHasVerifiedSetToTrue() {
        ProjectState projectState = createProjectState();
        DlmStepContext stepContext = createStepContext(projectState);

        readOnlyStep.execute(stepContext);

        assertThat(capturedRequest.get(), is(notNullValue()));
        assertTrue("AddIndexBlockRequest should have verified set to true", capturedRequest.get().markVerified());
    }

    private ProjectState createProjectState() {
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(
                IndexMetadata.builder(indexName)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build(),
                false
            );

        ClusterState.Builder clusterStateBuilder = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectMetadataBuilder);

        return clusterStateBuilder.build().projectState(projectId);
    }

    private DlmStepContext createStepContext(ProjectState projectState) {
        return new DlmStepContext(index, projectState, deduplicator, errorStore, randomIntBetween(1, 10), client);
    }
}
