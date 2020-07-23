/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
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
import org.elasticsearch.xpack.core.ilm.StopILMRequest;
import org.elasticsearch.xpack.core.ilm.action.StopILMAction;
import org.hamcrest.Description;
import org.mockito.ArgumentMatcher;

import static java.util.Collections.emptyMap;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportStopILMActionTests extends ESTestCase {

    private static final ActionListener<AcknowledgedResponse> EMPTY_LISTENER = new ActionListener<>() {
        @Override
        public void onResponse(AcknowledgedResponse response) {

        }

        @Override
        public void onFailure(Exception e) {

        }
    };

    @SuppressWarnings("unchecked")
    public void testStopILMClusterStatePriorityIsImmediate() {
        ClusterService clusterService = mock(ClusterService.class);

        TransportStopILMAction transportStopILMAction = new TransportStopILMAction(mock(TransportService.class),
            clusterService, mock(ThreadPool.class), mock(ActionFilters.class), mock(IndexNameExpressionResolver.class));
        Task task = new Task(randomLong(), "transport", StopILMAction.NAME, "description",
            new TaskId(randomLong() + ":" + randomLong()), emptyMap());
        StopILMRequest request = new StopILMRequest();
        transportStopILMAction.masterOperation(task, request, ClusterState.EMPTY_STATE, EMPTY_LISTENER);

        verify(clusterService).submitStateUpdateTask(
            eq("ilm_operation_mode_update"),
            argThat(new ArgumentMatcher<AckedClusterStateUpdateTask<AcknowledgedResponse>>() {

                Priority actualPriority = null;

                @Override
                public boolean matches(Object argument) {
                    if (argument instanceof AckedClusterStateUpdateTask == false) {
                        return false;
                    }
                    actualPriority = ((AckedClusterStateUpdateTask<AcknowledgedResponse>) argument).priority();
                    return actualPriority == Priority.IMMEDIATE;
                }

                @Override
                public void describeTo(Description description) {
                    description.appendText("the cluster state update task priority must be URGENT but got: ")
                        .appendText(actualPriority.name());
                }
            })
        );
    }

}
