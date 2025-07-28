/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.async;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.concurrent.ExecutorService;

import static org.elasticsearch.core.TimeValue.timeValueHours;
import static org.elasticsearch.xpack.core.XPackPlugin.ASYNC_RESULTS_INDEX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AsyncTaskMaintenanceServiceTests extends ESTestCase {

    private TestThreadPool threadPool;

    @Before
    public void setUpThreadPool() throws Exception {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void stopThreadPool() throws Exception {
        threadPool.shutdown();
    }

    @SuppressWarnings("unchecked")
    public void testStartStopDuringClusterChanges() {
        final String localNodeId = randomIdentifier();
        final String alternateNodeId = randomIdentifier();

        final Metadata.Builder metadataBuilder = Metadata.builder();
        final GlobalRoutingTable.Builder grtBuilder = GlobalRoutingTable.builder();

        final ProjectId p1 = ProjectId.fromId("p1");
        metadataBuilder.put(ProjectMetadata.builder(p1).put(getIndexMetadata(), false));
        grtBuilder.put(p1, buildRoutingTableWithIndex(localNodeId));

        final ProjectId p2 = ProjectId.fromId("p2");
        metadataBuilder.put(ProjectMetadata.builder(p2).put(getIndexMetadata(), false));
        grtBuilder.put(p2, buildRoutingTableWithIndex(alternateNodeId));

        final ProjectId p3 = ProjectId.fromId("p3");
        grtBuilder.put(p3, RoutingTable.builder());

        final ProjectId p4 = ProjectId.fromId("p4");
        metadataBuilder.put(ProjectMetadata.builder(p4).put(getIndexMetadata(), false));
        grtBuilder.put(p4, buildRoutingTableWithIndex(localNodeId));

        metadataBuilder.put(ProjectMetadata.builder(p3));
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadataBuilder)
            .routingTable(grtBuilder.build())
            .build();

        final ClusterService clusterService = Mockito.mock(ClusterService.class);

        final ThreadPool threadPoolSpy = Mockito.spy(threadPool);

        final Client client = Mockito.mock(Client.class);
        Mockito.doAnswer(invocationOnMock -> {
            ActionListener<?> listener = invocationOnMock.getArgument(2, ActionListener.class);
            listener.onResponse(null);
            return null;
        }).when(client).execute(same(DeleteByQueryAction.INSTANCE), any(DeleteByQueryRequest.class), any(ActionListener.class));

        final ProjectResolver projectResolver = Mockito.spy(TestProjectResolvers.mustExecuteFirst());

        final AsyncTaskMaintenanceService service = new AsyncTaskMaintenanceService(
            clusterService,
            projectResolver,
            localNodeId,
            Settings.EMPTY,
            threadPoolSpy,
            client
        );

        // Service needs to be started before it can do anything. This adds a cluster state listener
        service.start();
        verify(clusterService, times(1)).addListener(same(service));

        // Trigger a cluster state change
        service.clusterChanged(new ClusterChangedEvent(getTestName(), clusterState, ClusterState.EMPTY_STATE));

        // The projects (p1, p4) have an async-search index, with the primary shard#0 on "local node"
        // p2 has an async-search index, but the primary shard is on a different node
        // p3 does not have an async-search index
        verify(projectResolver, times(1)).executeOnProject(eq(p1), any(CheckedRunnable.class));
        verify(projectResolver, times(1)).executeOnProject(eq(p4), any(CheckedRunnable.class));
        Mockito.verifyNoMoreInteractions(projectResolver);

        // Each of the two projects will trigger a delete (in different project scopes, see above)
        verify(client, times(2)).execute(same(DeleteByQueryAction.INSTANCE), any(DeleteByQueryRequest.class), any(ActionListener.class));
        Mockito.verifyNoMoreInteractions(client);

        // At the end, the service schedules itself to run again in 1 hour on the generic thread pool
        verify(threadPoolSpy, times(1)).generic();
        verify(threadPoolSpy, times(1)).executor(ThreadPool.Names.GENERIC);
        verify(threadPoolSpy, times(1)).schedule(any(Runnable.class), eq(timeValueHours(1)), any(ExecutorService.class));
        Mockito.verifyNoMoreInteractions(threadPoolSpy);

        // Update the cluster state to drop project 4
        var previousState = clusterState;
        clusterState = ClusterState.builder(previousState)
            .metadata(Metadata.builder(previousState.metadata()).removeProject(p4))
            .routingTable(GlobalRoutingTable.builder(previousState.globalRoutingTable()).removeProject(p4).build())
            .build();

        // Because the task is already scheduled on this node, nothing immediate happens when we update the cluster state
        service.clusterChanged(new ClusterChangedEvent(getTestName(), clusterState, previousState));
        Mockito.verifyNoMoreInteractions(projectResolver);
        Mockito.verifyNoMoreInteractions(client);
        Mockito.verifyNoMoreInteractions(threadPoolSpy);

        // Trigger the cleanup (as if the 1h schedule had elapsed)
        service.executeNextCleanup();
        // p1 has been executed a 2nd time, but p4 has not been executed again because it was removed in the previous cluster state update
        verify(projectResolver, times(2)).executeOnProject(eq(p1), any(CheckedRunnable.class));
        verify(projectResolver, times(1)).executeOnProject(eq(p4), any(CheckedRunnable.class));
        Mockito.verifyNoMoreInteractions(projectResolver);

        // There is a 3rd delete (on p1)
        verify(client, times(3)).execute(same(DeleteByQueryAction.INSTANCE), any(DeleteByQueryRequest.class), any(ActionListener.class));
        Mockito.verifyNoMoreInteractions(client);

        // The service schedules another run in 1 hour
        verify(threadPoolSpy, times(2)).generic();
        verify(threadPoolSpy, times(2)).executor(ThreadPool.Names.GENERIC);
        verify(threadPoolSpy, times(2)).schedule(any(Runnable.class), eq(timeValueHours(1)), any(ExecutorService.class));
        Mockito.verifyNoMoreInteractions(threadPoolSpy);

        // Update the cluster state to drop project 1
        previousState = clusterState;
        clusterState = ClusterState.builder(previousState)
            .metadata(Metadata.builder(previousState.metadata()).removeProject(p1))
            .routingTable(GlobalRoutingTable.builder(previousState.globalRoutingTable()).removeProject(p1).build())
            .build();

        // Now there are no projects on this node, so the task should be cancelled.
        // We don't test for cancellation (it's hard to intercept) but we do check that the task is started on the next cluster state update
        service.clusterChanged(new ClusterChangedEvent(getTestName(), clusterState, previousState));
        Mockito.verifyNoMoreInteractions(projectResolver);
        Mockito.verifyNoMoreInteractions(client);
        Mockito.verifyNoMoreInteractions(threadPoolSpy);

        // Update the cluster state to add project 4 again
        previousState = clusterState;
        clusterState = ClusterState.builder(previousState)
            .metadata(Metadata.builder(previousState.metadata()).put(ProjectMetadata.builder(p4).put(getIndexMetadata(), false)))
            .routingTable(
                GlobalRoutingTable.builder(previousState.globalRoutingTable()).put(p4, buildRoutingTableWithIndex(localNodeId)).build()
            )
            .build();

        // Because a project now exists on this node, the cleanup is immediately scheduled
        service.clusterChanged(new ClusterChangedEvent(getTestName(), clusterState, previousState));
        // p4 now has a 2nd update
        verify(projectResolver, times(2)).executeOnProject(eq(p4), any(CheckedRunnable.class));
        // p1 still has the 2 updates it previously had
        verify(projectResolver, times(2)).executeOnProject(eq(p1), any(CheckedRunnable.class));
        Mockito.verifyNoMoreInteractions(projectResolver);
        // There is a 4th delete (on p4)
        verify(client, times(4)).execute(same(DeleteByQueryAction.INSTANCE), any(DeleteByQueryRequest.class), any(ActionListener.class));
        Mockito.verifyNoMoreInteractions(client);

        // The service schedules another run in 1 hour
        verify(threadPoolSpy, times(3)).generic();
        verify(threadPoolSpy, times(3)).executor(ThreadPool.Names.GENERIC);
        verify(threadPoolSpy, times(3)).schedule(any(Runnable.class), eq(timeValueHours(1)), same(threadPool.generic()));
        Mockito.verifyNoMoreInteractions(threadPoolSpy);
    }

    private static IndexMetadata getIndexMetadata() {
        return IndexMetadata.builder(ASYNC_RESULTS_INDEX).settings(indexSettings(IndexVersion.current(), 1, 0)).build();
    }

    private static RoutingTable.Builder buildRoutingTableWithIndex(String nodeId) {
        final Index index = new Index(ASYNC_RESULTS_INDEX, randomUUID());
        return RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(index)
                    .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), nodeId, true, ShardRoutingState.STARTED))
                    .build()
            );
    }

}
