/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.TestDiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class NodeSeenServiceTests extends ESTestCase {

    private ClusterService clusterService;
    private TransportPutShutdownNodeAction action;

    // must use member mock for generic
    @Mock
    private ClusterStateTaskExecutor.TaskContext<TransportPutShutdownNodeAction.PutShutdownNodeTask> taskContext;

    @Mock
    private MasterServiceTaskQueue<TransportPutShutdownNodeAction.PutShutdownNodeTask> taskQueue;

    private ClusterState state;

    private DiscoveryNode localNode;

    private DiscoveryNode otherNode;

    @Before
    public void init() {
        MockitoAnnotations.openMocks(this);
        // TODO: it takes almost 2 seconds to create these mocks....WHY?!?
        var threadPool = mock(ThreadPool.class);
        var transportService = mock(TransportService.class);
        clusterService = mock(ClusterService.class);
        var actionFilters = mock(ActionFilters.class);
        var indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(
            clusterService.createTaskQueue(
                any(),
                any(),
                Mockito.<ClusterStateTaskExecutor<TransportPutShutdownNodeAction.PutShutdownNodeTask>>any()
            )
        ).thenReturn(taskQueue);
        action = new TransportPutShutdownNodeAction(
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
        localNode = TestDiscoveryNode.create("localNode");
        otherNode = TestDiscoveryNode.create("otherNode");
        state = ClusterState.builder(ClusterName.DEFAULT)
            .version(1L)
            .nodes(DiscoveryNodes.builder().add(localNode).add(otherNode).localNodeId(localNode.getId()).build())
            .metadata(
                Metadata.builder()
                    .clusterUUID("clusteruuid")
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .term(2)
                            .lastCommittedConfiguration(CoordinationMetadata.VotingConfiguration.EMPTY_CONFIG)
                            .lastAcceptedConfiguration(CoordinationMetadata.VotingConfiguration.EMPTY_CONFIG)
                            .build()
                    )
                    .build()
            )
            .stateUUID("stateuuid")
            .build();
    }

    public void testNoop() throws Exception {
        var type = randomFrom(
            SingleNodeShutdownMetadata.Type.REMOVE,
            SingleNodeShutdownMetadata.Type.REPLACE,
            SingleNodeShutdownMetadata.Type.RESTART
        );
        var allocationDelay = type == SingleNodeShutdownMetadata.Type.RESTART ? TimeValue.timeValueMinutes(randomIntBetween(1, 3)) : null;
        var targetNodeName = type == SingleNodeShutdownMetadata.Type.REPLACE ? randomAlphaOfLength(5) : null;
        var request = new PutShutdownNodeAction.Request("node1", type, "sunsetting", allocationDelay, targetNodeName, null);
        action.masterOperation(null, request, ClusterState.EMPTY_STATE, ActionListener.noop());
        var updateTask = ArgumentCaptor.forClass(TransportPutShutdownNodeAction.PutShutdownNodeTask.class);
        var taskExecutor = ArgumentCaptor.forClass(TransportPutShutdownNodeAction.PutShutdownNodeExecutor.class);
        verify(clusterService).createTaskQueue(any(), any(), taskExecutor.capture());
        verify(taskQueue).submitTask(any(), updateTask.capture(), any());
        when(taskContext.getTask()).thenReturn(updateTask.getValue());
        ClusterState stableState = taskExecutor.getValue()
            .execute(new ClusterStateTaskExecutor.BatchExecutionContext<>(ClusterState.EMPTY_STATE, List.of(taskContext), () -> null));

        // run the request again, there should be no call to submit an update task
        clearTaskQueueInvocations();
        action.masterOperation(null, request, stableState, ActionListener.noop());
        verifyNoInteractions(taskQueue);

        // run the request again with empty state, the update task should return the same state
        action.masterOperation(null, request, ClusterState.EMPTY_STATE, ActionListener.noop());
        verify(taskQueue).submitTask(any(), updateTask.capture(), any());
        when(taskContext.getTask()).thenReturn(updateTask.getValue());
        ClusterState gotState = taskExecutor.getValue()
            .execute(new ClusterStateTaskExecutor.BatchExecutionContext<>(stableState, List.of(taskContext), () -> null));
        assertThat(gotState, sameInstance(stableState));
    }

    private void clearTaskQueueInvocations() {
        clearInvocations(taskQueue);
    }
}
