/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datastreams;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse;
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
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamLifecycleConvertToFrozenMarkReadOnlyTests extends ESTestCase {
    private ProjectId projectId;
    private String indexName;
    private Index index;
    private ThreadPool threadPool;
    private AtomicReference<AddIndexBlockRequest> capturedRequest;
    private AtomicReference<AddIndexBlockResponse> mockResponse;
    private AtomicReference<Exception> mockFailure;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        projectId = randomProjectIdOrDefault();
        indexName = randomAlphaOfLength(10);
        index = new Index(indexName, randomAlphaOfLength(10));
        capturedRequest = new AtomicReference<>();
        mockResponse = new AtomicReference<>();
        mockFailure = new AtomicReference<>();
    }

    @After
    public void cleanup() {
        threadPool.shutdownNow();
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
                if (request instanceof AddIndexBlockRequest) {
                    capturedRequest.set((AddIndexBlockRequest) request);
                    if (mockFailure.get() != null) {
                        listener.onFailure(mockFailure.get());
                    } else if (mockResponse.get() != null) {
                        listener.onResponse((Response) mockResponse.get());
                    }
                }
            }
        };
    }

    public void testSkipsMarkingReadOnlyWhenIndexHasWriteBlock() {
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

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(indexName, createMockClient(), projectState);
        converter.maybeMarkIndexReadOnly();

        // No AddIndexBlockRequest should have been sent since the index already has a write block
        assertThat(capturedRequest.get(), is(nullValue()));
    }

    public void testMarksIndexWhenIndexHasNoWriteBlock() {
        ProjectState projectState = createProjectState();
        mockResponse.set(new AddIndexBlockResponse(true, true, List.of(new AddIndexBlockResponse.AddBlockResult(index))));

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(indexName, createMockClient(), projectState);
        converter.maybeMarkIndexReadOnly();

        // AddIndexBlockRequest should have been sent since the index didn't have a write block
        assertThat(capturedRequest.get(), is(notNullValue()));
    }

    public void testCallsAddBlockWithCorrectParameters() {
        ProjectState projectState = createProjectState();
        mockResponse.set(new AddIndexBlockResponse(true, true, List.of(new AddIndexBlockResponse.AddBlockResult(index))));

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(indexName, createMockClient(), projectState);
        // Acknowledged response - should not throw
        converter.maybeMarkIndexReadOnly();

        assertThat(capturedRequest.get(), is(notNullValue()));
    }

    public void testThrowsExceptionWithUnacknowledgedResponse() {
        ProjectState projectState = createProjectState();
        mockResponse.set(new AddIndexBlockResponse(false, false, Collections.emptyList()));

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(indexName, createMockClient(), projectState);

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, converter::maybeMarkIndexReadOnly);
        assertThat(exception.getMessage(), containsString("not acknowledged"));
    }

    public void testThrowsWithGlobalExceptionInBlockResult() {
        ProjectState projectState = createProjectState();
        ElasticsearchException blockException = new ElasticsearchException("global failure");
        mockResponse.set(new AddIndexBlockResponse(false, false, List.of(new AddIndexBlockResponse.AddBlockResult(index, blockException))));

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(indexName, createMockClient(), projectState);

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, converter::maybeMarkIndexReadOnly);
        assertThat(exception.getMessage(), containsString("global failure"));
    }

    public void testThrowsWithShardFailuresInBlockResult() {
        ProjectState projectState = createProjectState();

        AddIndexBlockResponse.AddBlockShardResult.Failure shardFailure = new AddIndexBlockResponse.AddBlockShardResult.Failure(
            indexName,
            0,
            new ElasticsearchException("shard failure")
        );
        AddIndexBlockResponse.AddBlockShardResult[] shardResults = new AddIndexBlockResponse.AddBlockShardResult[] {
            new AddIndexBlockResponse.AddBlockShardResult(0, new AddIndexBlockResponse.AddBlockShardResult.Failure[] { shardFailure }) };
        mockResponse.set(new AddIndexBlockResponse(false, false, List.of(new AddIndexBlockResponse.AddBlockResult(index, shardResults))));

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(indexName, createMockClient(), projectState);

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, converter::maybeMarkIndexReadOnly);
        assertThat(exception.getMessage(), containsString("shard failure"));
    }

    public void testSucceedsWithIndexNotFoundException() {
        ProjectState projectState = createProjectState();
        mockFailure.set(new IndexNotFoundException(indexName));

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(indexName, createMockClient(), projectState);

        // IndexNotFoundException should be treated as success (index was already deleted) - should not throw
        converter.maybeMarkIndexReadOnly();
    }

    public void testThrowsWithGenericFailure() {
        ProjectState projectState = createProjectState();
        mockFailure.set(new ElasticsearchException("some error"));

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(indexName, createMockClient(), projectState);

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, converter::maybeMarkIndexReadOnly);
        assertThat(exception.getMessage(), containsString("some error"));
    }

    public void testDoesNotSkipWithNonWriteBlock() {
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
        mockResponse.set(new AddIndexBlockResponse(true, true, List.of(new AddIndexBlockResponse.AddBlockResult(index))));

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(indexName, createMockClient(), projectState);
        converter.maybeMarkIndexReadOnly();

        // READ block should NOT prevent the request from being sent - only WRITE block should
        assertThat(capturedRequest.get(), is(notNullValue()));
    }

    public void testThrowsWithMultipleShardFailures() {
        ProjectState projectState = createProjectState();

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
        mockResponse.set(new AddIndexBlockResponse(false, false, List.of(new AddIndexBlockResponse.AddBlockResult(index, shardResults))));

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(indexName, createMockClient(), projectState);

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, converter::maybeMarkIndexReadOnly);
        String errorMessage = exception.getMessage();
        assertThat(errorMessage, containsString("shard 0 failure"));
        assertThat(errorMessage, containsString("shard 1 failure"));
    }

    public void testThrowsWithShardResultsButNoFailures() {
        ProjectState projectState = createProjectState();

        AddIndexBlockResponse.AddBlockShardResult[] shardResults = new AddIndexBlockResponse.AddBlockShardResult[] {
            new AddIndexBlockResponse.AddBlockShardResult(0, new AddIndexBlockResponse.AddBlockShardResult.Failure[0]),
            new AddIndexBlockResponse.AddBlockShardResult(1, new AddIndexBlockResponse.AddBlockShardResult.Failure[0]) };
        mockResponse.set(new AddIndexBlockResponse(false, false, List.of(new AddIndexBlockResponse.AddBlockResult(index, shardResults))));

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(indexName, createMockClient(), projectState);

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, converter::maybeMarkIndexReadOnly);
        assertThat(exception.getMessage(), containsString("not acknowledged"));
    }

    public void testAddIndexBlockRequestHasVerifiedSetToTrue() {
        ProjectState projectState = createProjectState();
        mockResponse.set(new AddIndexBlockResponse(true, true, List.of(new AddIndexBlockResponse.AddBlockResult(index))));

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(indexName, createMockClient(), projectState);
        converter.maybeMarkIndexReadOnly();

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

}
