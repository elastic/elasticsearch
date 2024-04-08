/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.StopILMRequest;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.mockito.ArgumentMatcher;

import static java.util.Collections.emptyMap;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportStopILMActionTests extends ESTestCase {

    public void testStopILMClusterStatePriorityIsImmediate() {
        ClusterService clusterService = mock(ClusterService.class);

        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        TransportStopILMAction transportStopILMAction = new TransportStopILMAction(
            transportService,
            clusterService,
            threadPool,
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class)
        );
        Task task = new Task(
            randomLong(),
            "transport",
            ILMActions.STOP.name(),
            "description",
            new TaskId(randomLong() + ":" + randomLong()),
            emptyMap()
        );
        StopILMRequest request = new StopILMRequest();
        transportStopILMAction.masterOperation(task, request, ClusterState.EMPTY_STATE, ActionListener.noop());

        verify(clusterService).submitUnbatchedStateUpdateTask(
            eq("ilm_operation_mode_update[stopping]"),
            argThat(new ArgumentMatcher<AckedClusterStateUpdateTask>() {

                Priority actualPriority = null;

                @Override
                public boolean matches(AckedClusterStateUpdateTask other) {
                    actualPriority = other.priority();
                    return actualPriority == Priority.IMMEDIATE;
                }
            })
        );
    }

}
