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
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DLMConvertToFrozenForceMergeTests extends ESTestCase {
    private ProjectId projectId;
    private String indexName;
    private String indexUuid;
    private XPackLicenseState licenseState;
    private Index index;
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private AtomicReference<ForceMergeRequest> capturedForceMergeRequest;
    private AtomicReference<BroadcastResponse> mockForceMergeResponse;
    private AtomicReference<Exception> mockForceMergeFailure;
    private AtomicReference<IndicesSegmentResponse> mockSegmentResponse;
    private AtomicReference<ClusterHealthRequest> capturedHealthRequest;
    private AtomicReference<ClusterHealthResponse> mockHealthResponse;
    private AtomicReference<Exception> mockHealthFailure;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = createClusterService(threadPool);
        projectId = randomProjectIdOrDefault();
        indexName = randomAlphaOfLength(10);
        indexUuid = randomAlphaOfLength(10);
        licenseState = new XPackLicenseState(
            System::currentTimeMillis,
            new XPackLicenseStatus(License.OperationMode.ENTERPRISE, true, null)
        );
        index = new Index(indexName, indexUuid);
        capturedForceMergeRequest = new AtomicReference<>();
        mockForceMergeResponse = new AtomicReference<>();
        mockForceMergeFailure = new AtomicReference<>();
        mockSegmentResponse = new AtomicReference<>();
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
                if (request instanceof ForceMergeRequest) {
                    capturedForceMergeRequest.set((ForceMergeRequest) request);
                    if (mockForceMergeFailure.get() != null) {
                        listener.onFailure(mockForceMergeFailure.get());
                    } else if (mockForceMergeResponse.get() != null) {
                        listener.onResponse((Response) mockForceMergeResponse.get());
                    }
                } else if (request instanceof IndicesSegmentsRequest) {
                    if (mockSegmentResponse.get() != null) {
                        listener.onResponse((Response) mockSegmentResponse.get());
                    } else {
                        // Default: no segments found (force merge not complete)
                        listener.onResponse((Response) new IndicesSegmentResponse(new ShardSegments[0], 0, 0, 0, List.of()));
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

    public void testSkipsForceMergeWhenAlreadyForceMergedToSingleSegment() throws InterruptedException {
        // Set up segment response showing single segment on primary shard
        ShardSegments shardSegments = new ShardSegments(
            TestShardRouting.newShardRouting(new ShardId(index, 0), "_node_id", true, ShardRoutingState.STARTED),
            List.of(new Segment("_0"))
        );
        mockSegmentResponse.set(new IndicesSegmentResponse(new ShardSegments[] { shardSegments }, 1, 1, 0, List.of()));

        createProjectState();
        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        converter.maybeForceMergeIndex(indexName);

        // No force merge request should have been sent since the index is already at 1 segment
        assertThat(capturedForceMergeRequest.get(), is(nullValue()));
    }

    public void testDoesNotSkipForceMergeWhenMultipleSegments() throws InterruptedException {
        ShardSegments shardSegments = new ShardSegments(
            TestShardRouting.newShardRouting(new ShardId(index, 0), "_node_id", true, ShardRoutingState.STARTED),
            List.of(new Segment("_0"), new Segment("_1"))
        );
        mockSegmentResponse.set(new IndicesSegmentResponse(new ShardSegments[] { shardSegments }, 1, 1, 0, List.of()));
        mockForceMergeResponse.set(new BroadcastResponse(1, 1, 0, List.of()));

        createProjectState();
        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        converter.maybeForceMergeIndex(indexName);

        assertThat(capturedForceMergeRequest.get(), is(notNullValue()));
        assertThat(capturedForceMergeRequest.get().indices().length, is(1));
        assertThat(capturedForceMergeRequest.get().indices()[0], is(indexName));
        assertThat(capturedForceMergeRequest.get().maxNumSegments(), is(1));
    }

    public void testForceMergeThrowsWhenShardsHaveFailures() {
        // Default mock segment response: no segments found → force merge not complete
        DefaultShardOperationFailedException shardFailure = new DefaultShardOperationFailedException(
            indexName,
            0,
            new IllegalStateException("shard merge failed")
        );
        mockForceMergeResponse.set(new BroadcastResponse(1, 0, 1, List.of(shardFailure)));

        createProjectState();
        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> converter.maybeForceMergeIndex(indexName));
        assertThat(exception.getMessage(), containsString(indexName));
        assertThat(exception.getMessage(), containsString("DLM failed to force merge"));
    }

    public void testForceMergeThrowsWhenShardsAreUnavailable() {
        // Default mock segment response: no segments found → force merge not complete
        mockForceMergeResponse.set(new BroadcastResponse(5, 3, 0, List.of()));

        createProjectState();
        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> converter.maybeForceMergeIndex(indexName));
        assertThat(exception.getMessage(), containsString(indexName));
        assertThat(exception.getMessage(), containsString("shards were unavailable"));
    }

    public void testForceMergeSucceedsWhenAllShardsSuccessful() throws InterruptedException {
        // Default mock segment response: no segments found → force merge not complete
        mockForceMergeResponse.set(new BroadcastResponse(1, 1, 0, List.of()));

        createProjectState();
        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        // Should not throw
        converter.maybeForceMergeIndex(indexName);

        assertThat(capturedForceMergeRequest.get(), is(notNullValue()));
    }

    public void testForceMergeWrapsNonElasticsearchException() {
        // Default mock segment response: no segments found → force merge not complete
        mockForceMergeFailure.set(new RuntimeException("transport failure"));

        createProjectState();
        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> converter.maybeForceMergeIndex(indexName));
        assertThat(exception.getCause().getMessage(), containsString("transport failure"));
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

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> converter.maybeForceMergeIndex(indexName));
        assertThat(exception.getMessage(), containsString("timed out"));
        assertThat(exception.getMessage(), containsString(indexName));
        // No force merge request should have been issued
        assertThat(capturedForceMergeRequest.get(), is(nullValue()));
    }

    private static final String REPO_NAME = "my-repo";

    private void createProjectState() {
        buildProjectState(Settings.EMPTY);
    }

    /**
     * Builds a {@link ProjectState} for the test index. Pass {@code null} to omit the index from metadata entirely.
     */
    private void buildProjectState(@Nullable Settings indexSettings) {
        ProjectMetadata.Builder projectMetadata = ProjectMetadata.builder(projectId);
        if (indexSettings != null) {
            projectMetadata.put(buildIndexMetadata(indexSettings), false);
        }

        RepositoryMetadata repo = new RepositoryMetadata(REPO_NAME, "fs", Settings.EMPTY);
        projectMetadata.putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(List.of(repo)));

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectMetadata.build()).build();
        setState(clusterService, clusterState);
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
            .putCustom(
                DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                Map.of(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, REPO_NAME)
            )
            .build();
    }
}
