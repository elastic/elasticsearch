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
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
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

import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.dlm.frozen.DataStreamLifecycleConvertToFrozen.CLONE_INDEX_PREFIX;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DataStreamLifecycleConvertToFrozenCloneIndexTests extends ESTestCase {
    private ProjectId projectId;
    private String indexName;
    private Index index;
    private XPackLicenseState licenseState;
    private ThreadPool threadPool;
    private AtomicReference<ResizeRequest> capturedResizeRequest;
    private AtomicReference<CreateIndexResponse> mockCloneResponse;
    private AtomicReference<DeleteIndexRequest> capturedDeleteRequest;
    private AtomicReference<AcknowledgedResponse> mockDeleteResponse;
    private AtomicReference<Exception> mockCloneFailure;
    private AtomicReference<Exception> mockDeleteFailure;
    private AtomicReference<ClusterHealthRequest> capturedHealthRequest;
    private AtomicReference<ClusterHealthResponse> mockHealthResponse;
    private AtomicReference<Exception> mockHealthFailure;
    private NoOpClient client;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        projectId = randomProjectIdOrDefault();
        indexName = randomAlphaOfLength(10);
        index = new Index(indexName, randomAlphaOfLength(10));
        licenseState = new XPackLicenseState(
            System::currentTimeMillis,
            new XPackLicenseStatus(License.OperationMode.ENTERPRISE, true, null)
        );
        capturedResizeRequest = new AtomicReference<>();
        mockCloneResponse = new AtomicReference<>();
        capturedDeleteRequest = new AtomicReference<>();
        mockDeleteResponse = new AtomicReference<>();
        mockCloneFailure = new AtomicReference<>();
        mockDeleteFailure = new AtomicReference<>();
        capturedHealthRequest = new AtomicReference<>();
        mockHealthResponse = new AtomicReference<>();
        mockHealthFailure = new AtomicReference<>();
        client = createMockClient();
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
                if (request instanceof ResizeRequest resizeRequest) {
                    capturedResizeRequest.set(resizeRequest);
                    if (mockCloneFailure.get() != null) {
                        listener.onFailure(mockCloneFailure.get());
                    } else if (mockCloneResponse.get() != null) {
                        listener.onResponse((Response) mockCloneResponse.get());
                    }
                } else if (request instanceof DeleteIndexRequest deleteIndexRequest) {
                    capturedDeleteRequest.set(deleteIndexRequest);
                    if (mockDeleteFailure.get() != null) {
                        listener.onFailure(mockDeleteFailure.get());
                    } else if (mockDeleteResponse.get() != null) {
                        listener.onResponse((Response) mockDeleteResponse.get());
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

    public void testGetIndexForForceMergeReturnsCloneIndexWhenNoExistingClone() {
        ProjectState projectState = createProjectState(2);
        DataStreamLifecycleConvertToFrozen convert = new DataStreamLifecycleConvertToFrozen(indexName, client, projectState, licenseState);
        String indexForForceMerge = convert.getIndexForForceMerge();
        assertThat(indexForForceMerge, is(convert.getDLMCloneIndexName()));
    }

    public void testGetIndexForForceMergeReturnsCloneWhenCloneExists() {
        ProjectState projectState = createProjectStateWithClone(true);
        DataStreamLifecycleConvertToFrozen convert = new DataStreamLifecycleConvertToFrozen(indexName, client, projectState, licenseState);
        String indexForForceMerge = convert.getIndexForForceMerge();
        assertThat(indexForForceMerge, is(notNullValue()));
        assertThat(indexForForceMerge, equalTo(convert.getDLMCloneIndexName()));
    }

    public void testGetIndexForForceMergeWaitsForCloneWhenShardsInactive() {
        ProjectState projectState = createProjectStateWithClone(false);
        // Mock a successful non-timed-out health response so waitForCloneToBeActive succeeds
        ClusterHealthResponse healthResponse = new ClusterHealthResponse();
        mockHealthResponse.set(healthResponse);

        DataStreamLifecycleConvertToFrozen convert = new DataStreamLifecycleConvertToFrozen(indexName, client, projectState, licenseState);
        String indexForForceMerge = convert.getIndexForForceMerge();

        // Should have issued a health request to wait for clone to become active
        assertThat(capturedHealthRequest.get(), is(notNullValue()));
        // Should return the clone index name after waiting
        assertThat(indexForForceMerge, equalTo(convert.getDLMCloneIndexName()));
    }

    public void testGetIndexForForceMergeThrowsWhenWaitForCloneTimesOut() {
        ProjectState projectState = createProjectStateWithClone(false);
        // Mock a timed-out health response
        ClusterHealthResponse healthResponse = new ClusterHealthResponse();
        healthResponse.setTimedOut(true);
        mockHealthResponse.set(healthResponse);
        mockDeleteResponse.set(AcknowledgedResponse.of(true));

        DataStreamLifecycleConvertToFrozen convert = new DataStreamLifecycleConvertToFrozen(indexName, client, projectState, licenseState);

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, convert::getIndexForForceMerge);
        assertThat(exception.getMessage(), containsString("timed out waiting for clone index"));
    }

    public void testGetIndexForForceMergeReturnsOriginalIndexWhenZeroReplicas() {
        ProjectState projectState = createProjectState(0);
        DataStreamLifecycleConvertToFrozen convert = new DataStreamLifecycleConvertToFrozen(indexName, client, projectState, licenseState);
        String indexForForceMerge = convert.getIndexForForceMerge();
        assertThat(indexForForceMerge, is(notNullValue()));
        assertThat(indexForForceMerge, equalTo(indexName));
    }

    public void testMaybeCloneIndexCreatesCloneWithCorrectSettings() {
        ProjectState projectState = createProjectState(2); // replicas > 0 to trigger cloning
        DataStreamLifecycleConvertToFrozen convert = new DataStreamLifecycleConvertToFrozen(indexName, client, projectState, licenseState);
        mockCloneResponse.set(new CreateIndexResponse(true, true, convert.getDLMCloneIndexName()));
        convert.maybeCloneIndex();

        assertThat(capturedResizeRequest.get(), is(notNullValue()));
        assertThat(capturedResizeRequest.get().getSourceIndex(), equalTo(indexName));
        assertThat(capturedResizeRequest.get().getTargetIndexRequest().index(), containsString("dlm-clone-"));
        assertThat(capturedResizeRequest.get().getTargetIndexRequest().settings().get("index.number_of_replicas"), equalTo("0"));
        assertTrue(capturedResizeRequest.get().getTargetIndexRequest().settings().keySet().contains("index.auto_expand_replicas"));
        assertNull(capturedResizeRequest.get().getTargetIndexRequest().settings().get("index.auto_expand_replicas"));
    }

    public void testExecuteWithFailedCloneResponse() {
        ProjectState projectState = createProjectState(2); // replicas > 0 to trigger cloning
        mockCloneFailure.set(new ElasticsearchException("clone failed"));
        mockDeleteResponse.set(AcknowledgedResponse.of(true));

        DataStreamLifecycleConvertToFrozen convert = new DataStreamLifecycleConvertToFrozen(indexName, client, projectState, licenseState);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, convert::maybeCloneIndex);
        assertThat(exception.getMessage(), containsString("failed to clone"));

        // Should attempt to delete the clone index on failure
        String cloneIndexName = convert.getDLMCloneIndexName();
        assertThat(capturedDeleteRequest.get(), is(notNullValue()));
        assertThat(capturedDeleteRequest.get().indices()[0], equalTo(cloneIndexName));
    }

    public void testDeleteCloneSuccessfully() {
        ProjectState projectState = createProjectState(1);
        mockDeleteResponse.set(AcknowledgedResponse.of(true));

        DataStreamLifecycleConvertToFrozen convert = new DataStreamLifecycleConvertToFrozen(indexName, client, projectState, licenseState);
        convert.deleteIndex(convert.getDLMCloneIndexName());

        String cloneIndexName = convert.getDLMCloneIndexName();
        assertThat(capturedDeleteRequest.get(), is(notNullValue()));
        assertThat(capturedDeleteRequest.get().indices()[0], equalTo(cloneIndexName));
    }

    public void testDeleteCloneWithUnacknowledgedResponse() {
        ProjectState projectState = createProjectState(1);
        mockDeleteResponse.set(AcknowledgedResponse.of(false));

        DataStreamLifecycleConvertToFrozen convert = new DataStreamLifecycleConvertToFrozen(indexName, client, projectState, licenseState);

        ElasticsearchException exception = expectThrows(
            ElasticsearchException.class,
            () -> convert.deleteIndex(convert.getDLMCloneIndexName())
        );
        assertThat(exception.getMessage(), containsString("unable to delete index"));
    }

    public void testDeleteCloneWithFailure() {
        ProjectState projectState = createProjectState(1);
        mockDeleteFailure.set(new ElasticsearchException("delete failed"));

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            indexName,
            client,
            projectState,
            licenseState
        );

        ElasticsearchException exception = expectThrows(
            ElasticsearchException.class,
            () -> converter.deleteIndex(converter.getDLMCloneIndexName())
        );
        assertThat(exception.getMessage(), containsString("unable to delete index"));
    }

    public void testWaitForCloneToBeActiveSucceeds() {
        ProjectState projectState = createProjectState(1);
        // Mock a successful non-timed-out health response
        ClusterHealthResponse healthResponse = new ClusterHealthResponse();
        mockHealthResponse.set(healthResponse);

        DataStreamLifecycleConvertToFrozen convert = new DataStreamLifecycleConvertToFrozen(indexName, client, projectState, licenseState);
        // Should not throw
        convert.waitForCloneToBeActive();

        String cloneIndexName = convert.getDLMCloneIndexName();
        assertThat(capturedHealthRequest.get(), is(notNullValue()));
        assertThat(capturedHealthRequest.get().indices()[0], equalTo(cloneIndexName));
    }

    public void testWaitForCloneToBeActiveThrowsOnTimeout() {
        ProjectState projectState = createProjectState(1);
        // Mock a timed-out health response
        ClusterHealthResponse healthResponse = new ClusterHealthResponse();
        healthResponse.setTimedOut(true);
        mockHealthResponse.set(healthResponse);
        mockDeleteResponse.set(AcknowledgedResponse.of(true));

        DataStreamLifecycleConvertToFrozen convert = new DataStreamLifecycleConvertToFrozen(indexName, client, projectState, licenseState);

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, convert::waitForCloneToBeActive);
        assertThat(exception.getMessage(), containsString("timed out waiting for clone index"));
        assertThat(exception.getMessage(), containsString(convert.getDLMCloneIndexName()));
    }

    public void testWaitForCloneToBeActiveThrowsOnFailure() {
        ProjectState projectState = createProjectState(1);
        mockHealthFailure.set(new ElasticsearchException("health check failed"));
        mockDeleteResponse.set(AcknowledgedResponse.of(true));

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            indexName,
            client,
            projectState,
            licenseState
        );

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, converter::waitForCloneToBeActive);
        assertThat(exception.getMessage(), containsString("DLM failed waiting for clone index"));
    }

    public void testWaitForCloneToBeActiveRequestsGreenStatus() {
        ProjectState projectState = createProjectState(1);
        ClusterHealthResponse healthResponse = new ClusterHealthResponse();
        mockHealthResponse.set(healthResponse);

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            indexName,
            client,
            projectState,
            licenseState
        );
        converter.waitForCloneToBeActive();

        assertThat(capturedHealthRequest.get(), is(notNullValue()));
        assertThat(capturedHealthRequest.get().waitForStatus(), equalTo(org.elasticsearch.cluster.health.ClusterHealthStatus.GREEN));
    }

    /**
     * Creates a ProjectState with the target index having the specified number of replicas.
     */
    private ProjectState createProjectState(int numberOfReplicas) {
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(
                IndexMetadata.builder(indexName)
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                            .build()
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(numberOfReplicas)
                    .build(),
                false
            );

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectMetadataBuilder).build();
        return clusterState.projectState(projectId);
    }

    /**
     * Creates a ProjectState with the target index and a clone index, with routing table indicating
     * whether all primary shards are active.
     */
    private ProjectState createProjectStateWithClone(boolean allShardsActive) {
        String cloneIndexName = CLONE_INDEX_PREFIX + indexName;

        IndexMetadata originalIndexMetadata = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        IndexMetadata cloneIndexMetadata = IndexMetadata.builder(cloneIndexName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(originalIndexMetadata, false)
            .put(cloneIndexMetadata, false);

        ShardRouting primaryShard = TestShardRouting.newShardRouting(
            new ShardId(cloneIndexMetadata.getIndex(), 0),
            "node1",
            true,
            allShardsActive ? ShardRoutingState.STARTED : ShardRoutingState.INITIALIZING
        );
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(cloneIndexMetadata.getIndex())
            .addShard(primaryShard);
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTableBuilder).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(projectMetadataBuilder)
            .putRoutingTable(projectId, routingTable)
            .build();
        return clusterState.projectState(projectId);
    }
}
