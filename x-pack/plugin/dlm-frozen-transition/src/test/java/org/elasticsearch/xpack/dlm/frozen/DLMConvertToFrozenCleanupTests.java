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
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
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
import org.elasticsearch.index.IndexVersion;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.elasticsearch.xpack.dlm.frozen.DLMConvertToFrozen.CLONE_INDEX_PREFIX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DLMConvertToFrozenCleanupTests extends ESTestCase {

    private static final String REPO_NAME = "my-repo";

    private ProjectId projectId;
    private String indexName;
    private Index index;
    private XPackLicenseState licenseState;
    private ThreadPool threadPool;
    private ClusterService clusterService;

    private List<DeleteIndexRequest> capturedDeleteRequests;
    private AtomicReference<ModifyDataStreamsAction.Request> capturedModifyDataStreamsRequest;
    private AtomicReference<AcknowledgedResponse> mockModifyDataStreamsResponse;
    private AtomicReference<Exception> mockModifyDataStreamsFailure;
    private AtomicReference<AcknowledgedResponse> mockDeleteResponse;
    private AtomicReference<Exception> mockDeleteFailure;
    private NoOpClient client;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = createClusterService(threadPool);
        projectId = randomProjectIdOrDefault();
        indexName = randomAlphaOfLength(10);
        index = new Index(indexName, randomAlphaOfLength(10));
        licenseState = new XPackLicenseState(
            System::currentTimeMillis,
            new XPackLicenseStatus(License.OperationMode.ENTERPRISE, true, null)
        );
        capturedDeleteRequests = new ArrayList<>();
        capturedModifyDataStreamsRequest = new AtomicReference<>();
        mockModifyDataStreamsResponse = new AtomicReference<>(AcknowledgedResponse.TRUE);
        mockModifyDataStreamsFailure = new AtomicReference<>();
        mockDeleteResponse = new AtomicReference<>(AcknowledgedResponse.TRUE);
        mockDeleteFailure = new AtomicReference<>();
        client = createMockClient();
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
                if (request instanceof ModifyDataStreamsAction.Request modifyRequest) {
                    capturedModifyDataStreamsRequest.set(modifyRequest);
                    if (mockModifyDataStreamsFailure.get() != null) {
                        listener.onFailure(mockModifyDataStreamsFailure.get());
                    } else if (mockModifyDataStreamsResponse.get() != null) {
                        listener.onResponse((Response) mockModifyDataStreamsResponse.get());
                    } else {
                        fail("No mock modify data streams response or failure configured");
                    }
                } else if (request instanceof DeleteIndexRequest deleteRequest) {
                    capturedDeleteRequests.add(deleteRequest);
                    if (mockDeleteFailure.get() != null) {
                        listener.onFailure(mockDeleteFailure.get());
                    } else if (mockDeleteResponse.get() != null) {
                        listener.onResponse((Response) mockDeleteResponse.get());
                    } else {
                        fail("No mock delete response or failure configured");
                    }
                } else {
                    fail("Unexpected request type [" + request.getClass().getName() + "] for action [" + action.name() + "]");
                }
            }
        };
    }

    /**
     * Tests that when the original index is part of a data stream and forceMergeIndex is different (clone case),
     * cleanup swaps the backing index, deletes the clone, and deletes the original.
     */
    public void testCleanupSwapsBackingIndexAndDeletesCloneAndOriginal() throws InterruptedException {
        String dataStreamName = "my-data-stream";
        String cloneIndexName = CLONE_INDEX_PREFIX + indexName;
        buildAndSetProjectState(dataStreamName, indexName, cloneIndexName);

        DLMConvertToFrozen convert = new DLMConvertToFrozen(
            indexName,
            projectId,
            client,
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );
        convert.maybeCleanup(cloneIndexName);

        ModifyDataStreamsAction.Request modifyRequest = capturedModifyDataStreamsRequest.get();
        assertThat(modifyRequest, is(notNullValue()));
        assertThat(modifyRequest.getActions().size(), is(2));
        // First action should remove the original index
        assertThat(modifyRequest.getActions().get(0).getType(), equalTo(DataStreamAction.Type.REMOVE_BACKING_INDEX));
        assertThat(modifyRequest.getActions().get(0).getDataStream(), equalTo(dataStreamName));
        assertThat(modifyRequest.getActions().get(0).getIndex(), equalTo(indexName));
        // Second action should add the frozen index
        String frozenIndexName = DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + indexName;
        assertThat(modifyRequest.getActions().get(1).getType(), equalTo(DataStreamAction.Type.ADD_BACKING_INDEX));
        assertThat(modifyRequest.getActions().get(1).getDataStream(), equalTo(dataStreamName));
        assertThat(modifyRequest.getActions().get(1).getIndex(), equalTo(frozenIndexName));

        // Should have issued two delete requests: one for the clone, one for the original
        assertThat(capturedDeleteRequests.size(), is(2));
        assertThat(capturedDeleteRequests.get(0).indices(), hasItemInArray(cloneIndexName));
        assertThat(capturedDeleteRequests.get(1).indices(), hasItemInArray(indexName));
    }

    /**
     * Tests that when the original index is NOT part of a data stream, cleanup skips the swap step
     * and just deletes the clone and the original index.
     */
    public void testCleanupWithNoDataStreamSkipsSwapAndDeletesBothIndices() throws InterruptedException {
        String cloneIndexName = CLONE_INDEX_PREFIX + indexName;
        buildAndSetProjectState(null, indexName, cloneIndexName);

        DLMConvertToFrozen convert = new DLMConvertToFrozen(
            indexName,
            projectId,
            client,
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );
        convert.maybeCleanup(cloneIndexName);

        // Should NOT have issued a modify data streams request
        assertThat(capturedModifyDataStreamsRequest.get(), is(nullValue()));

        // Should have issued two delete requests: clone and original
        assertThat(capturedDeleteRequests.size(), is(2));
        assertThat(capturedDeleteRequests.get(0).indices(), hasItemInArray(cloneIndexName));
        assertThat(capturedDeleteRequests.get(1).indices(), hasItemInArray(indexName));
    }

    /**
     * Tests that when the swap backing index step fails, an exception is thrown and delete is not called.
     */
    public void testCleanupThrowsWhenSwapFails() {
        String dataStreamName = "my-data-stream";
        String cloneIndexName = CLONE_INDEX_PREFIX + indexName;
        buildAndSetProjectState(dataStreamName, indexName, cloneIndexName);

        mockModifyDataStreamsFailure.set(new ElasticsearchException("swap failed"));

        DLMConvertToFrozen convert = new DLMConvertToFrozen(
            indexName,
            projectId,
            client,
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        expectThrows(ElasticsearchException.class, () -> convert.maybeCleanup(cloneIndexName));

        // Swap was attempted
        assertThat(capturedModifyDataStreamsRequest.get(), is(notNullValue()));

        // Delete should not have been called since the swap failed
        assertThat(capturedDeleteRequests.size(), is(0));
    }

    /**
     * Tests that when the swap succeeds but the swap is not acknowledged, no delete is called.
     */
    public void testCleanupDoesNotDeleteWhenSwapNotAcknowledged() {
        String dataStreamName = "my-data-stream";
        String cloneIndexName = CLONE_INDEX_PREFIX + indexName;
        buildAndSetProjectState(dataStreamName, indexName, cloneIndexName);

        mockModifyDataStreamsResponse.set(AcknowledgedResponse.FALSE);

        DLMConvertToFrozen convert = new DLMConvertToFrozen(
            indexName,
            projectId,
            client,
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        expectThrows(ElasticsearchException.class, () -> convert.maybeCleanup(cloneIndexName));

        // Swap was attempted
        assertThat(capturedModifyDataStreamsRequest.get(), is(notNullValue()));

        // No deletes should have been called since the swap was not acknowledged (exception thrown before delete)
        assertThat(capturedDeleteRequests.size(), is(0));
    }

    /**
     * Tests that when isCleanUpComplete returns true (original and clone indices are gone,
     * and the frozen index is part of the data stream), cleanup is skipped entirely.
     */
    public void testCleanupSkipsWhenAlreadyComplete() throws InterruptedException {
        String dataStreamName = "my-data-stream";
        String cloneIndexName = CLONE_INDEX_PREFIX + indexName;
        String frozenIndexName = DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + indexName;

        // Build a state where original and clone are gone, and the frozen index is in the data stream
        buildAndSetProjectState(dataStreamName, frozenIndexName);

        DLMConvertToFrozen convert = new DLMConvertToFrozen(
            indexName,
            projectId,
            client,
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );
        convert.maybeCleanup(cloneIndexName);

        // No swap or delete should have been attempted
        assertThat(capturedModifyDataStreamsRequest.get(), is(nullValue()));
        assertThat(capturedDeleteRequests.size(), is(0));
    }

    /**
     * Builds and sets a project cluster state. The {@code backingIndexName} is the index that will be added to the data stream
     * (if {@code dataStreamName} is non-null). Additional index names can be supplied via {@code extraIndexNames} and will be
     * added to the project as plain indices. The backing index gets the lifecycle custom metadata with the repo name and the
     * original index UUID; extra indices get minimal settings.
     */
    private void buildAndSetProjectState(String dataStreamName, String backingIndexName, String... extraIndexNames) {
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId);

        IndexMetadata backingIndexMetadata = IndexMetadata.builder(backingIndexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putCustom(
                DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                Map.of(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, REPO_NAME)
            )
            .build();
        projectMetadataBuilder.put(backingIndexMetadata, false);

        if (dataStreamName != null) {
            projectMetadataBuilder.put(
                DataStream.builder(dataStreamName, List.of(backingIndexMetadata.getIndex())).setGeneration(1).build()
            );
        }

        for (String extra : extraIndexNames) {
            IndexMetadata extraMetadata = IndexMetadata.builder(extra)
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            projectMetadataBuilder.put(extraMetadata, false);
        }

        RepositoryMetadata repo = new RepositoryMetadata(REPO_NAME, "fs", Settings.EMPTY);
        projectMetadataBuilder.putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(List.of(repo)));

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectMetadataBuilder).build();
        setState(clusterService, clusterState);
    }
}
