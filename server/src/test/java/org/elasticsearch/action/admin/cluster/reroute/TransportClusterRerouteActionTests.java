/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.mockito.Mockito;

import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportClusterRerouteActionTests extends ESTestCase {

    public void testValidateRequestForRetryFailedAndAllocationCommands() {
        final ClusterService clusterService = mock(ClusterService.class);
        final TransportService transportService = mock(TransportService.class);
        when(transportService.getLocalNode()).thenReturn(DiscoveryNodeUtils.create(randomUUID()));
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        final var request = new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        request.setRetryFailed(true);
        IntStream.range(0, between(1, 3)).forEach(i -> request.add(randomAllocationCommand(Metadata.DEFAULT_PROJECT_ID)));

        final var statefulAction = new TransportClusterRerouteAction(
            transportService,
            clusterService,
            threadPool,
            mock(AllocationService.class),
            mock(ActionFilters.class),
            DefaultProjectResolver.INSTANCE
        );
        Mockito.clearInvocations(transportService);
        statefulAction.masterOperation(mock(Task.class), request, ClusterState.builder(ClusterName.DEFAULT).build(), ActionListener.noop());
        if (request.getCommands().commands().stream().anyMatch(command -> command instanceof AllocateStalePrimaryAllocationCommand)) {
            verify(transportService).sendRequest(any(DiscoveryNode.class), anyString(), any(TransportRequest.class), any());
        } else {
            verify(clusterService).submitUnbatchedStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        }

        final var multiProjectAction = new TransportClusterRerouteAction(
            mock(TransportService.class),
            clusterService,
            mock(ThreadPool.class),
            mock(AllocationService.class),
            mock(ActionFilters.class),
            TestProjectResolvers.allProjects()
        );

        expectThrows(
            IllegalArgumentException.class,
            () -> multiProjectAction.masterOperation(
                mock(Task.class),
                request,
                ClusterState.builder(ClusterName.DEFAULT).build(),
                ActionListener.noop()
            )
        );
    }

    public void testValidateRequestProjectConsistency() {
        final var action = new TransportClusterRerouteAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(AllocationService.class),
            mock(ActionFilters.class),
            TestProjectResolvers.singleProject(randomProjectIdOrDefault())
        );

        final var request = new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        IntStream.range(0, between(1, 3)).forEach(i -> request.add(randomAllocationCommand(randomUniqueProjectId())));

        final var e = expectThrows(
            IllegalStateException.class,
            () -> action.masterOperation(
                mock(Task.class),
                request,
                ClusterState.builder(ClusterName.DEFAULT).build(),
                ActionListener.noop()
            )
        );
        assertThat(e.getMessage(), containsString("inconsistent project-id"));
    }

    private AllocationCommand randomAllocationCommand(ProjectId projectId) {
        final String index = randomIdentifier();
        final int shardId = between(0, 9);
        final String node = randomUUID();
        final String toNode = randomUUID();
        return randomFrom(
            new MoveAllocationCommand(index, shardId, node, toNode, projectId),
            new CancelAllocationCommand(index, shardId, node, randomBoolean(), projectId),
            new AllocateReplicaAllocationCommand(index, shardId, node, projectId),
            new AllocateEmptyPrimaryAllocationCommand(index, shardId, node, true, projectId),
            new AllocateStalePrimaryAllocationCommand(index, shardId, node, true, projectId)
        );
    }
}
