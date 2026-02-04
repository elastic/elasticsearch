/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.tasks;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.TransportTasksActionTests.TestTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.TransportTasksActionTests.TestTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.TransportTasksActionTests.TestTasksResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class TransportTasksProjectActionTests extends ESTestCase {

    public void testNullTaskProjectIdAndDefaultProject() {
        ProjectResolver projectResolver = TestProjectResolvers.singleProject(ProjectId.DEFAULT);
        TestTransportTasksProjectAction action = createAction(projectResolver);

        Task task = createTask(null);
        assertThat(action.match(task), is(true));
    }

    public void testNullTaskProjectIdAndNonDefaultProject() {
        ProjectId projectId = randomUniqueProjectId();
        ProjectResolver projectResolver = TestProjectResolvers.singleProject(projectId);
        TestTransportTasksProjectAction action = createAction(projectResolver);

        Task task = createTask(null);
        assertThat(action.match(task), is(false));
    }

    public void testMatchingProject() {
        ProjectId projectId = randomProjectIdOrDefault();
        ProjectResolver projectResolver = TestProjectResolvers.singleProject(projectId);
        TestTransportTasksProjectAction action = createAction(projectResolver);

        Task task = createTask(projectId.id());
        assertThat(action.match(task), is(true));
    }

    public void testNonMatchingProject() {
        ProjectId projectId = randomProjectIdOrDefault();
        ProjectId otherProjectId = randomUniqueProjectId();
        ProjectResolver projectResolver = TestProjectResolvers.singleProject(projectId);
        TestTransportTasksProjectAction action = createAction(projectResolver);

        Task task = createTask(otherProjectId.id());
        assertThat(action.match(task), is(false));
    }

    private TestTransportTasksProjectAction createAction(ProjectResolver projectResolver) {
        return new TestTransportTasksProjectAction(
            "test-action",
            mock(),
            mock(),
            mock(),
            TestTasksRequest::new,
            TestTaskResponse::new,
            mock(),
            projectResolver
        );
    }

    private Task createTask(String projectId) {
        Map<String, String> headers = new HashMap<>();
        if (projectId != null) {
            headers.put(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId);
        }
        return new Task(randomLong(), "test", "test-action", "test", TaskId.EMPTY_TASK_ID, headers);
    }

    private static class TestTransportTasksProjectAction extends TransportTasksProjectAction<
        Task,
        TestTasksRequest,
        TestTasksResponse,
        TestTaskResponse> {

        TestTransportTasksProjectAction(
            String actionName,
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            Writeable.Reader<TestTasksRequest> requestReader,
            Writeable.Reader<TestTaskResponse> responseReader,
            Executor executor,
            ProjectResolver projectResolver
        ) {
            super(actionName, clusterService, transportService, actionFilters, requestReader, responseReader, executor, projectResolver);
        }

        @Override
        protected TestTasksResponse newResponse(
            TestTasksRequest request,
            List<TestTaskResponse> tasks,
            List<TaskOperationFailure> taskOperationFailures,
            List<FailedNodeException> failedNodeExceptions
        ) {
            return new TestTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
        }

        @Override
        protected void taskOperation(
            CancellableTask actionTask,
            TestTasksRequest request,
            Task task,
            ActionListener<TestTaskResponse> listener
        ) {
            listener.onResponse(new TestTaskResponse("status"));
        }
    }
}
