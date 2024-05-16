/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.IndexMetaDataGenerations;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static java.util.Collections.emptyMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportMountSearchableSnapshotActionTests extends ESTestCase {

    private static final String REPOSITORY_NAME = "repositoryName";
    private static final String MOUNTED_INDEX_NAME = "mountedIndexName";
    private static final String SNAPSHOT_INDEX_NAME = "snapshotIndexName";
    private static final IndexId INDEX_ID = new IndexId(SNAPSHOT_INDEX_NAME, UUID.randomUUID().toString());
    private static final String SNAPSHOT_NAME = "snapshotName";
    private static final SnapshotId SNAPSHOT_ID = new SnapshotId(SNAPSHOT_NAME, UUID.randomUUID().toString());
    private static final String NODE_ID = "nodeId";

    private Settings settings;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder().put("node.name", TransportMountSearchableSnapshotActionTests.class.getSimpleName()).build();
        threadPool = new ThreadPool(settings, MeterRegistry.NOOP);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testRefreshSnapshotTasksAreLinkedWithParentMountTask() throws ExecutionException, InterruptedException, IOException {
        // Mock the client, capture the request
        ArgumentCaptor<RestoreSnapshotRequest> restoreSnapshotRequestCaptor = ArgumentCaptor.forClass(RestoreSnapshotRequest.class);
        Client client = mock(Client.class);
        when(client.admin()).thenReturn(mock(AdminClient.class));
        ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);
        when(client.admin().cluster()).thenReturn(clusterAdminClient);
        doAnswer(iom -> {
            ActionListener<RestoreSnapshotResponse> listener = iom.getArgument(1);
            listener.onResponse(new RestoreSnapshotResponse((RestoreInfo) null));
            return null;
        }).when(clusterAdminClient).restoreSnapshot(restoreSnapshotRequestCaptor.capture(), any());

        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.isSearchableSnapshot()).thenReturn(false);
        when(indexMetadata.getSettings()).thenReturn(settings);

        RepositoryData repositoryData = new RepositoryData(
            randomUUID(),
            randomLong(),
            Map.of(SNAPSHOT_ID.getUUID(), SNAPSHOT_ID),
            emptyMap(),
            Map.of(INDEX_ID, List.of(SNAPSHOT_ID)),
            ShardGenerations.EMPTY,
            IndexMetaDataGenerations.EMPTY,
            randomUUID()
        );

        BlobStoreRepository repository = mock(BlobStoreRepository.class);
        when(repository.getSnapshotIndexMetaData(repositoryData, SNAPSHOT_ID, INDEX_ID)).thenReturn(indexMetadata);
        RepositoriesService repositoriesService = mock(RepositoriesService.class);
        when(repositoriesService.repository(REPOSITORY_NAME)).thenReturn(repository);

        doAnswer(iom -> {
            Executor ex = iom.getArgument(0);
            ActionListener<RepositoryData> listener = iom.getArgument(1);
            ex.execute(() -> listener.onResponse(repositoryData));
            return null;
        }).when(repository).getRepositoryData(any(), any());

        TransportMountSearchableSnapshotAction action = new TransportMountSearchableSnapshotAction(
            mockTransportService(),
            mockClusterService(),
            client,
            threadPool,
            repositoriesService,
            new ActionFilters(Collections.emptySet()),
            mock(IndexNameExpressionResolver.class),
            enterpriseLicenseState(),
            mock(SystemIndices.class)
        );

        Task task = new Task(randomLong(), "type", "action", null, null, emptyMap());
        PlainActionFuture<RestoreSnapshotResponse> future = new PlainActionFuture<>();
        action.execute(
            task,
            new MountSearchableSnapshotRequest(
                MOUNTED_INDEX_NAME,
                REPOSITORY_NAME,
                SNAPSHOT_NAME,
                SNAPSHOT_INDEX_NAME,
                settings,
                new String[] {},
                true,
                MountSearchableSnapshotRequest.Storage.FULL_COPY
            ),
            future
        );
        future.get();
        RestoreSnapshotRequest restoreSnapshotRequest = restoreSnapshotRequestCaptor.getValue();
        assertEquals(new TaskId(NODE_ID, task.getId()), restoreSnapshotRequest.getParentTask());
    }

    private static XPackLicenseState enterpriseLicenseState() {
        return new XPackLicenseState(System::currentTimeMillis, new XPackLicenseStatus(License.OperationMode.ENTERPRISE, true, "none"));
    }

    private TransportService mockTransportService() {
        return new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNodeUtils.builder(randomUUID()).applySettings(settings).address(boundAddress.publishAddress()).build(),
            null,
            Collections.emptySet()
        );
    }

    private ClusterService mockClusterService() {
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(discoveryNodes.isLocalNodeElectedMaster()).thenReturn(true);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.localNode()).thenReturn(
            DiscoveryNode.createLocal(settings, new TransportAddress(TransportAddress.META_ADDRESS, 9212), NODE_ID)
        );
        return clusterService;
    }
}
