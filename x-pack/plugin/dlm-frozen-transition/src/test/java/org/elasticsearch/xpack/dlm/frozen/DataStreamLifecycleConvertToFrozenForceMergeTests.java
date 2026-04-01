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
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
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

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamLifecycleConvertToFrozenForceMergeTests extends ESTestCase {
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
                }
            }
        };
    }

    public void testSkipsForceMergeWhenAlreadyForceMergedToSingleSegment() {
        // Set up segment response showing single segment on primary shard
        ShardSegments shardSegments = new ShardSegments(
            TestShardRouting.newShardRouting(new ShardId(index, 0), "_node_id", true, ShardRoutingState.STARTED),
            List.of(new Segment("_0"))
        );
        mockSegmentResponse.set(new IndicesSegmentResponse(new ShardSegments[] { shardSegments }, 1, 1, 0, List.of()));

        createProjectState();
        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            licenseState
        );

        converter.maybeForceMergeIndex(indexName);

        // No force merge request should have been sent since the index is already at 1 segment
        assertThat(capturedForceMergeRequest.get(), is(nullValue()));
    }

    public void testDoesNotSkipForceMergeWhenMultipleSegments() {
        ShardSegments shardSegments = new ShardSegments(
            TestShardRouting.newShardRouting(new ShardId(index, 0), "_node_id", true, ShardRoutingState.STARTED),
            List.of(new Segment("_0"), new Segment("_1"))
        );
        mockSegmentResponse.set(new IndicesSegmentResponse(new ShardSegments[] { shardSegments }, 1, 1, 0, List.of()));
        mockForceMergeResponse.set(new BroadcastResponse(1, 1, 0, List.of()));

        createProjectState();
        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            licenseState
        );

        converter.maybeForceMergeIndex(indexName);

        assertThat(capturedForceMergeRequest.get(), is(notNullValue()));
        assertThat(capturedForceMergeRequest.get().indices().length, is(1));
        assertThat(capturedForceMergeRequest.get().indices()[0], is(indexName));
        assertThat(capturedForceMergeRequest.get().maxNumSegments(), is(1));
    }

    public void testMaybeForceMergeSkipsWhenIndexNotInMetadata() {
        buildProjectState(null);

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            licenseState
        );

        converter.maybeForceMergeIndex(indexName);

        // No force merge request should be sent when the index is not in metadata
        assertThat(capturedForceMergeRequest.get(), is(nullValue()));
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
        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            licenseState
        );

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> converter.maybeForceMergeIndex(indexName));
        assertThat(exception.getMessage(), containsString(indexName));
        assertThat(exception.getMessage(), containsString("DLM failed to force merge"));
    }

    public void testForceMergeThrowsWhenShardsAreUnavailable() {
        // Default mock segment response: no segments found → force merge not complete
        mockForceMergeResponse.set(new BroadcastResponse(5, 3, 0, List.of()));

        createProjectState();
        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            licenseState
        );

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> converter.maybeForceMergeIndex(indexName));
        assertThat(exception.getMessage(), containsString(indexName));
        assertThat(exception.getMessage(), containsString("shards were unavailable"));
    }

    public void testForceMergeSucceedsWhenAllShardsSuccessful() {
        // Default mock segment response: no segments found → force merge not complete
        mockForceMergeResponse.set(new BroadcastResponse(1, 1, 0, List.of()));

        createProjectState();
        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            licenseState
        );

        // Should not throw
        converter.maybeForceMergeIndex(indexName);

        assertThat(capturedForceMergeRequest.get(), is(notNullValue()));
    }

    public void testForceMergeWrapsNonElasticsearchException() {
        // Default mock segment response: no segments found → force merge not complete
        mockForceMergeFailure.set(new RuntimeException("transport failure"));

        createProjectState();
        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            indexName,
            projectId,
            createMockClient(),
            clusterService,
            licenseState
        );

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> converter.maybeForceMergeIndex(indexName));
        assertThat(exception.getCause().getMessage(), containsString("transport failure"));
    }

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
            .build();
    }
}
