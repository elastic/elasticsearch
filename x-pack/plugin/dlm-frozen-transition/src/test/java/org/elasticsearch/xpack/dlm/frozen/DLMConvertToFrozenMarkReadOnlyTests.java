/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DLMConvertToFrozenMarkReadOnlyTests extends ESTestCase {
    private ProjectId projectId;
    private String indexName;
    private XPackLicenseState licenseState;
    private Index index;
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private AtomicReference<AddIndexBlockRequest> capturedRequest;
    private AtomicReference<AddIndexBlockResponse> mockResponse;
    private AtomicReference<Exception> mockFailure;
    private AtomicReference<ClusterHealthRequest> capturedHealthRequest;
    private AtomicReference<ClusterHealthResponse> mockHealthResponse;
    private AtomicReference<Exception> mockHealthFailure;

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
        index = new Index(indexName, randomAlphaOfLength(10));
        capturedRequest = new AtomicReference<>();
        mockResponse = new AtomicReference<>();
        mockFailure = new AtomicReference<>();
        capturedHealthRequest = new AtomicReference<>();
        mockHealthResponse = new AtomicReference<>(new ClusterHealthResponse()); // default: non-timed-out
        mockHealthFailure = new AtomicReference<>();
    }

    @After
    public void cleanup() {
        clusterService.close();
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
                } else if (request instanceof ClusterHealthRequest healthRequest) {
                    capturedHealthRequest.set(healthRequest);
                    if (mockHealthFailure.get() != null) {
                        listener.onFailure(mockHealthFailure.get());
                    } else if (mockHealthResponse.get() != null) {
                        listener.onResponse((Response) mockHealthResponse.get());
                    }
                }
            }
        };
    }

    public void testSkipsMarkingReadOnlyWhenIndexHasWriteBlock() throws InterruptedException {
        ClusterBlock writeBlock = WRITE.getBlock();

        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(
                IndexMetadata.builder(indexName)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putCustom(
                        DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                        Map.of(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, DEFAULT_REPO_NAME)
                    )
                    .build(),
                false
            );

        RepositoryMetadata repo = new RepositoryMetadata(DEFAULT_REPO_NAME, "fs", Settings.EMPTY);
        projectMetadataBuilder.putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(List.of(repo)));

        ClusterBlocks clusterBlocks = ClusterBlocks.builder().addIndexBlock(projectId, indexName, writeBlock).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(projectMetadataBuilder)
            .blocks(clusterBlocks)
            .build();
        setState(clusterService, clusterState);

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );
        converter.maybeMarkIndexReadOnly();

        // No AddIndexBlockRequest should have been sent since the index already has a write block
        assertThat(capturedRequest.get(), is(nullValue()));
    }

    public void testCallsAddBlockWithCorrectParameters() throws InterruptedException {
        createProjectState();
        mockResponse.set(new AddIndexBlockResponse(true, true, List.of(new AddIndexBlockResponse.AddBlockResult(index))));

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );
        // Acknowledged response - should not throw
        converter.maybeMarkIndexReadOnly();

        assertThat(capturedRequest.get(), is(notNullValue()));
        assertThat(capturedRequest.get().indices(), is(new String[] { indexName }));
        assertThat(capturedRequest.get().getBlock(), is(WRITE));
        assertTrue("AddIndexBlockRequest should have verified set to true", capturedRequest.get().markVerified());
    }

    public void testThrowsExceptionWithUnacknowledgedResponse() {
        createProjectState();
        mockResponse.set(new AddIndexBlockResponse(false, false, Collections.emptyList()));

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, converter::maybeMarkIndexReadOnly);
        assertThat(exception.getMessage(), containsString("unable to mark index"));
    }

    public void testThrowsWithGlobalExceptionInBlockResult() {
        createProjectState();
        ElasticsearchException blockException = new ElasticsearchException("global failure");
        mockResponse.set(new AddIndexBlockResponse(false, false, List.of(new AddIndexBlockResponse.AddBlockResult(index, blockException))));

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, converter::maybeMarkIndexReadOnly);
        assertThat(exception.getMessage(), containsString("unable to mark index"));
    }

    public void testThrowsWithShardFailuresInBlockResult() {
        createProjectState();

        AddIndexBlockResponse.AddBlockShardResult.Failure shardFailure = new AddIndexBlockResponse.AddBlockShardResult.Failure(
            indexName,
            0,
            new ElasticsearchException("shard failure")
        );
        AddIndexBlockResponse.AddBlockShardResult[] shardResults = new AddIndexBlockResponse.AddBlockShardResult[] {
            new AddIndexBlockResponse.AddBlockShardResult(0, new AddIndexBlockResponse.AddBlockShardResult.Failure[] { shardFailure }) };
        mockResponse.set(new AddIndexBlockResponse(false, false, List.of(new AddIndexBlockResponse.AddBlockResult(index, shardResults))));

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, converter::maybeMarkIndexReadOnly);
        assertThat(exception.getMessage(), containsString("unable to mark index"));
    }

    public void testThrowsWithGenericFailure() {
        createProjectState();
        mockFailure.set(new ElasticsearchException("some error"));

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, converter::maybeMarkIndexReadOnly);
        assertThat(exception.getMessage(), containsString("unable to mark index"));
    }

    public void testDoesNotSkipWithNonWriteBlock() throws InterruptedException {
        ClusterBlock readBlock = IndexMetadata.APIBlock.READ.getBlock();

        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(
                IndexMetadata.builder(indexName)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putCustom(
                        DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                        Map.of(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, DEFAULT_REPO_NAME)
                    )
                    .build(),
                false
            );

        RepositoryMetadata repo = new RepositoryMetadata(DEFAULT_REPO_NAME, "fs", Settings.EMPTY);
        projectMetadataBuilder.putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(List.of(repo)));

        ClusterBlocks clusterBlocks = ClusterBlocks.builder().addIndexBlock(projectId, indexName, readBlock).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(projectMetadataBuilder)
            .blocks(clusterBlocks)
            .build();
        setState(clusterService, clusterState);
        mockResponse.set(new AddIndexBlockResponse(true, true, List.of(new AddIndexBlockResponse.AddBlockResult(index))));

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );
        converter.maybeMarkIndexReadOnly();

        // READ block should NOT prevent the request from being sent - only WRITE block should
        assertThat(capturedRequest.get(), is(notNullValue()));
        assertThat(capturedRequest.get().indices(), is(new String[] { indexName }));
        assertThat(capturedRequest.get().getBlock(), is(WRITE));
        assertTrue("AddIndexBlockRequest should have verified set to true", capturedRequest.get().markVerified());

    }

    public void testThrowsWithMultipleShardFailures() {
        createProjectState();

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

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, converter::maybeMarkIndexReadOnly);
        String errorMessage = exception.getMessage();
        assertThat(errorMessage, containsString("unable to mark index"));
        assertThat(errorMessage, containsString("unable to mark index"));
    }

    public void testThrowsWithShardResultsButNoFailures() {
        createProjectState();

        AddIndexBlockResponse.AddBlockShardResult[] shardResults = new AddIndexBlockResponse.AddBlockShardResult[] {
            new AddIndexBlockResponse.AddBlockShardResult(0, new AddIndexBlockResponse.AddBlockShardResult.Failure[0]),
            new AddIndexBlockResponse.AddBlockShardResult(1, new AddIndexBlockResponse.AddBlockShardResult.Failure[0]) };
        mockResponse.set(new AddIndexBlockResponse(false, false, List.of(new AddIndexBlockResponse.AddBlockResult(index, shardResults))));

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, converter::maybeMarkIndexReadOnly);
        assertThat(exception.getMessage(), containsString("unable to mark index"));
    }

    public void testAddIndexBlockRequestHasVerifiedSetToTrue() throws InterruptedException {
        createProjectState();
        mockResponse.set(new AddIndexBlockResponse(true, true, List.of(new AddIndexBlockResponse.AddBlockResult(index))));

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );
        converter.maybeMarkIndexReadOnly();

        assertThat(capturedRequest.get(), is(notNullValue()));
        assertTrue("AddIndexBlockRequest should have verified set to true", capturedRequest.get().markVerified());
    }

    public void testIsEligibleThrowsWhenIndexDoesNotExist() {
        // Create project state without the target index
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectMetadataBuilder).build();
        setState(clusterService, clusterState);

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        assertThrows(IndexNotFoundException.class, converter::checkIfEligibleForConvertToFrozen);
    }

    public void testIsEligibleThrowsWhenRepositoryIsNotRegistered() {
        // Create project state with the index but without the repository registered
        String repoName = "my-repo";
        createProjectStateWithRepo(repoName, false);

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        DLMUnrecoverableException exception = expectThrows(DLMUnrecoverableException.class, converter::checkIfEligibleForConvertToFrozen);
        assertThat(exception.getMessage(), containsString(repoName));
    }

    public void testIsEligibleThrowsWhenRepositoryIsReadOnly() {
        String repoName = "my-repo";
        createProjectStateWithRepo(repoName, Settings.builder().put(BlobStoreRepository.READONLY_SETTING_KEY, true).build());

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        ElasticsearchException exception = expectThrows(DLMUnrecoverableException.class, converter::checkIfEligibleForConvertToFrozen);
        assertThat(exception.getMessage(), containsString(repoName));
    }

    public void testIsEligibleThrowsWhenLicenseDoesNotAllowSearchableSnapshots() {
        String repoName = "my-repo";
        createProjectStateWithRepo(repoName, true);

        // Use a BASIC license which does not allow searchable snapshots (requires ENTERPRISE)
        XPackLicenseState basicLicenseState = new XPackLicenseState(
            System::currentTimeMillis,
            new XPackLicenseStatus(License.OperationMode.BASIC, true, null)
        );

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> basicLicenseState,
            Clock.systemUTC()
        );

        ElasticsearchSecurityException exception = expectThrows(
            ElasticsearchSecurityException.class,
            converter::checkIfEligibleForConvertToFrozen
        );
        assertThat(exception.getMessage(), containsString("non-compliant"));
        assertThat(exception.getMessage(), containsString("searchable-snapshots"));
    }

    public void testIsEligibleSucceedsWhenAllConditionsAreMet() {
        String repoName = "my-repo";
        createProjectStateWithRepo(repoName, true);

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        // Should not throw any exception when all conditions are met
        converter.checkIfEligibleForConvertToFrozen();
    }

    public void testThrowsWhenYellowStatusTimeoutBreached() {
        createProjectState();
        ClusterHealthResponse timedOut = new ClusterHealthResponse();
        timedOut.setTimedOut(true);
        mockHealthResponse.set(timedOut);

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, converter::maybeMarkIndexReadOnly);
        assertThat(exception.getMessage(), containsString("timed out"));
        assertThat(exception.getMessage(), containsString(indexName));
        // No AddIndexBlockRequest should have been issued since we failed before reaching that step
        assertThat(capturedRequest.get(), is(nullValue()));
    }

    /**
     * Creates a ProjectState with the target index, a default repository setting on the cluster metadata,
     * and optionally registers a matching repository in the project's RepositoriesMetadata.
     */
    private void createProjectStateWithRepo(String repoName, boolean registerRepo) {
        createProjectStateWithRepo(repoName, registerRepo ? Settings.EMPTY : null);
    }

    private void createProjectStateWithRepo(String repoName, Settings repoSettings) {
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(
                IndexMetadata.builder(indexName)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putCustom(
                        DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                        Map.of(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, repoName)
                    )
                    .build(),
                false
            );

        if (repoSettings != null) {
            RepositoryMetadata repo = new RepositoryMetadata(repoName, "fs", repoSettings);
            projectMetadataBuilder.putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(List.of(repo)));
        }

        Metadata metadata = Metadata.builder()
            .persistentSettings(Settings.builder().put("repositories.default_repository", repoName).build())
            .put(projectMetadataBuilder)
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        setState(clusterService, clusterState);
    }

    private static final String DEFAULT_REPO_NAME = "my-repo";

    private void createProjectState() {
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(
                IndexMetadata.builder(indexName)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putCustom(
                        DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                        Map.of(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, DEFAULT_REPO_NAME)
                    )
                    .build(),
                false
            );

        RepositoryMetadata repo = new RepositoryMetadata(DEFAULT_REPO_NAME, "fs", Settings.EMPTY);
        projectMetadataBuilder.putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(List.of(repo)));

        ClusterState.Builder clusterStateBuilder = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectMetadataBuilder);

        setState(clusterService, clusterStateBuilder.build());
    }

}
