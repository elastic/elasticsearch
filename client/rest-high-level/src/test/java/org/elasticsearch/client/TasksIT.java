/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskGroup;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;

import java.io.IOException;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class TasksIT extends ESRestHighLevelClientTestCase {

    public void testListTasks() throws IOException {
        ListTasksRequest request = new ListTasksRequest();
        ListTasksResponse response = execute(request, highLevelClient().tasks()::list, highLevelClient().tasks()::listAsync);

        assertThat(response, notNullValue());
        assertThat(response.getNodeFailures(), equalTo(emptyList()));
        assertThat(response.getTaskFailures(), equalTo(emptyList()));
        // It's possible that there are other tasks except 'cluster:monitor/tasks/lists[n]' and 'action":"cluster:monitor/tasks/lists'
        assertThat(response.getTasks().size(), greaterThanOrEqualTo(2));
        boolean listTasksFound = false;
        for (TaskGroup taskGroup : response.getTaskGroups()) {
            TaskInfo parent = taskGroup.getTaskInfo();
            if ("cluster:monitor/tasks/lists".equals(parent.getAction())) {
                assertThat(taskGroup.getChildTasks().size(), equalTo(1));
                TaskGroup childGroup = taskGroup.getChildTasks().iterator().next();
                assertThat(childGroup.getChildTasks().isEmpty(), equalTo(true));
                TaskInfo child = childGroup.getTaskInfo();
                assertThat(child.getAction(), equalTo("cluster:monitor/tasks/lists[n]"));
                assertThat(child.getParentTaskId(), equalTo(parent.getTaskId()));
                listTasksFound = true;
            }
        }
        assertTrue("List tasks were not found", listTasksFound);
    }

    public void testCancelTasks() throws IOException {
        ListTasksRequest listRequest = new ListTasksRequest();
        ListTasksResponse listResponse = execute(
            listRequest,
            highLevelClient().tasks()::list,
            highLevelClient().tasks()::listAsync
        );
        // in this case, probably no task will actually be cancelled.
        // this is ok, that case is covered in TasksIT.testTasksCancellation
        TaskInfo firstTask = listResponse.getTasks().get(0);
        String node = listResponse.getPerNodeTasks().keySet().iterator().next();

        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        cancelTasksRequest.setTaskId(new TaskId(node, firstTask.getId()));
        cancelTasksRequest.setReason("testreason");
        CancelTasksResponse response = execute(cancelTasksRequest,
            highLevelClient().tasks()::cancel,
            highLevelClient().tasks()::cancelAsync);
        // Since the task may or may not have been cancelled, assert that we received a response only
        // The actual testing of task cancellation is covered by TasksIT.testTasksCancellation
        assertThat(response, notNullValue());
    }
}
