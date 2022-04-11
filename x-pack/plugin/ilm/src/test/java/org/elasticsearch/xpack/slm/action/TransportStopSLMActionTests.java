/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.action.StopILMAction;
import org.elasticsearch.xpack.core.slm.action.StopSLMAction;
import org.mockito.ArgumentMatcher;

import static java.util.Collections.emptyMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportStopSLMActionTests extends ESTestCase {

    public void testStopILMClusterStatePriorityIsImmediate() {
        ClusterService clusterService = mock(ClusterService.class);

        TransportStopSLMAction transportStopSLMAction = new TransportStopSLMAction(
            mock(TransportService.class),
            clusterService,
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class)
        );
        Task task = new Task(
            randomLong(),
            "transport",
            StopILMAction.NAME,
            "description",
            new TaskId(randomLong() + ":" + randomLong()),
            emptyMap()
        );
        StopSLMAction.Request request = new StopSLMAction.Request();
        transportStopSLMAction.masterOperation(task, request, ClusterState.EMPTY_STATE, ActionListener.noop());

        verify(clusterService).submitStateUpdateTask(
            eq("slm_operation_mode_update[stopping]"),
            argThat(new ArgumentMatcher<AckedClusterStateUpdateTask>() {

                Priority actualPriority = null;

                @Override
                public boolean matches(AckedClusterStateUpdateTask other) {
                    actualPriority = other.priority();
                    return actualPriority == Priority.IMMEDIATE;
                }
            }),
            any()
        );
    }

}
