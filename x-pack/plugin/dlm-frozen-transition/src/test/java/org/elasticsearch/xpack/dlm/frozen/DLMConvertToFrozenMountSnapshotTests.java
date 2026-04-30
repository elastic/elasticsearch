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
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DLMConvertToFrozenMountSnapshotTests extends ESTestCase {

    private static final String REPO_NAME = "my-repo";

    private ProjectId projectId;
    private String indexName;
    private Index index;
    private XPackLicenseState licenseState;
    private ThreadPool threadPool;
    private ClusterService clusterService;

    private AtomicReference<MountSearchableSnapshotRequest> capturedMountRequest;
    private AtomicReference<DeleteIndexRequest> capturedDeleteRequest;
    private AtomicReference<RestoreSnapshotResponse> mockMountResponse;
    private AtomicReference<Exception> mockMountFailure;
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
        capturedMountRequest = new AtomicReference<>();
        capturedDeleteRequest = new AtomicReference<>();
        mockMountResponse = new AtomicReference<>();
        mockMountFailure = new AtomicReference<>();
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
                if (request instanceof MountSearchableSnapshotRequest mountRequest) {
                    capturedMountRequest.set(mountRequest);
                    if (mockMountFailure.get() != null) {
                        listener.onFailure(mockMountFailure.get());
                    } else if (mockMountResponse.get() != null) {
                        listener.onResponse((Response) mockMountResponse.get());
                    } else {
                        fail("No mock mount response or failure configured for request [" + mountRequest + "]");
                    }
                } else if (request instanceof DeleteIndexRequest deleteRequest) {
                    capturedDeleteRequest.set(deleteRequest);
                    listener.onResponse((Response) AcknowledgedResponse.TRUE);
                } else {
                    fail("Unexpected request type [" + request.getClass().getName() + "] for action [" + action.name() + "]");
                }
            }
        };
    }

    public void testSkipsWhenSnapshotAlreadyMounted() throws InterruptedException {
        String snapshotName = DLMConvertToFrozen.snapshotName(indexName);
        createProjectStateWithMountedSnapshot(snapshotName);
        DLMConvertToFrozen convert = new DLMConvertToFrozen(
            indexName,
            projectId,
            client,
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        convert.maybeMountSearchableSnapshot(indexName);

        // No mount request should have been issued
        assertNull(capturedMountRequest.get());
    }

    public void testMountRequestHasCorrectParameters() throws InterruptedException {
        createProjectState();
        RestoreInfo restoreInfo = new RestoreInfo("snap", List.of(indexName), 1, 1);
        mockMountResponse.set(new RestoreSnapshotResponse(restoreInfo));

        DLMConvertToFrozen convert = new DLMConvertToFrozen(
            indexName,
            projectId,
            client,
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );
        convert.maybeMountSearchableSnapshot(indexName);

        MountSearchableSnapshotRequest req = capturedMountRequest.get();
        assertThat(req, is(notNullValue()));
        String expectedSnapshotName = DLMConvertToFrozen.snapshotName(indexName);
        assertThat(req.snapshotName(), equalTo(expectedSnapshotName));
        assertThat(req.repositoryName(), equalTo(REPO_NAME));
        assertThat(req.snapshotIndexName(), equalTo(indexName));
        assertThat(req.mountedIndexName(), equalTo(expectedSnapshotName));
        assertThat(req.storage(), equalTo(MountSearchableSnapshotRequest.Storage.SHARED_CACHE));
    }

    public void testThrowsWhenRestoreInfoIsNull() {
        createProjectState();
        mockMountResponse.set(new RestoreSnapshotResponse((RestoreInfo) null));

        DLMConvertToFrozen convert = new DLMConvertToFrozen(
            indexName,
            projectId,
            client,
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        ElasticsearchException exception = expectThrows(
            ElasticsearchException.class,
            () -> convert.maybeMountSearchableSnapshot(indexName)
        );
        assertThat(exception.getMessage(), containsString("restore info was missing"));
    }

    public void testThrowsWhenMountHasFailedShards() {
        createProjectState();
        RestoreInfo restoreInfo = new RestoreInfo("snap", List.of(indexName), 2, 1);
        mockMountResponse.set(new RestoreSnapshotResponse(restoreInfo));

        DLMConvertToFrozen convert = new DLMConvertToFrozen(
            indexName,
            projectId,
            client,
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        ElasticsearchException exception = expectThrows(
            ElasticsearchException.class,
            () -> convert.maybeMountSearchableSnapshot(indexName)
        );
        assertThat(exception.getMessage(), containsString("failed shards"));
    }

    public void testThrowsWhenMountHasZeroSuccessfulShards() {
        createProjectState();
        RestoreInfo restoreInfo = new RestoreInfo("snap", List.of(indexName), 0, 0);
        mockMountResponse.set(new RestoreSnapshotResponse(restoreInfo));

        DLMConvertToFrozen convert = new DLMConvertToFrozen(
            indexName,
            projectId,
            client,
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        ElasticsearchException exception = expectThrows(
            ElasticsearchException.class,
            () -> convert.maybeMountSearchableSnapshot(indexName)
        );
        assertThat(exception.getMessage(), containsString("failed shards or no successful shards"));
    }

    public void testThrowsWhenMountFails() {
        createProjectState();
        mockMountFailure.set(new ElasticsearchException("mount failed"));

        DLMConvertToFrozen convert = new DLMConvertToFrozen(
            indexName,
            projectId,
            client,
            clusterService,
            () -> licenseState,
            Clock.systemUTC()
        );

        ElasticsearchException exception = expectThrows(
            ElasticsearchException.class,
            () -> convert.maybeMountSearchableSnapshot(indexName)
        );
        assertThat(exception.getMessage(), containsString("mounting snapshot"));
    }

    /**
     * Creates a ProjectState with the target index and a configured repository but no mounted snapshot.
     */
    private void createProjectState() {
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
                    .numberOfReplicas(0)
                    .putCustom(
                        DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                        Map.of(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, REPO_NAME)
                    )
                    .build(),
                false
            );

        RepositoryMetadata repo = new RepositoryMetadata(REPO_NAME, "fs", Settings.EMPTY);
        projectMetadataBuilder.putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(List.of(repo)));

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectMetadataBuilder).build();
        setState(clusterService, clusterState);
    }

    /**
     * Creates a ProjectState with the target index and a mounted snapshot index that has all primary shards active.
     */
    private void createProjectStateWithMountedSnapshot(String mountedIndexName) {
        IndexMetadata originalIndexMetadata = IndexMetadata.builder(indexName)
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

        IndexMetadata mountedIndexMetadata = IndexMetadata.builder(mountedIndexName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(originalIndexMetadata, false)
            .put(mountedIndexMetadata, false);

        RepositoryMetadata repo = new RepositoryMetadata(REPO_NAME, "fs", Settings.EMPTY);
        projectMetadataBuilder.putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(List.of(repo)));

        ShardRouting primaryShard = TestShardRouting.newShardRouting(
            new ShardId(mountedIndexMetadata.getIndex(), 0),
            "node1",
            true,
            ShardRoutingState.STARTED
        );
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(mountedIndexMetadata.getIndex())
            .addShard(primaryShard);
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTableBuilder).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(projectMetadataBuilder)
            .putRoutingTable(projectId, routingTable)
            .build();
        setState(clusterService, clusterState);
    }
}
