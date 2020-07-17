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

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.client.tasks.CancelTasksRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class TasksRequestConvertersTests extends ESTestCase {

    public void testCancelTasks() {
        Map<String, String> expectedParams = new HashMap<>();
        org.elasticsearch.client.tasks.TaskId taskId =
            new org.elasticsearch.client.tasks.TaskId(randomAlphaOfLength(5), randomNonNegativeLong());
        org.elasticsearch.client.tasks.TaskId parentTaskId =
            new org.elasticsearch.client.tasks.TaskId(randomAlphaOfLength(5), randomNonNegativeLong());
        CancelTasksRequest.Builder builder = new CancelTasksRequest.Builder().withTaskId(taskId).withParentTaskId(parentTaskId);
        expectedParams.put("task_id", taskId.toString());
        expectedParams.put("parent_task_id", parentTaskId.toString());
        if (randomBoolean()) {
            boolean waitForCompletion = randomBoolean();
            builder.withWaitForCompletion(waitForCompletion);
            expectedParams.put("wait_for_completion", Boolean.toString(waitForCompletion));
        }
        Request httpRequest = TasksRequestConverters.cancelTasks(builder.build());
        assertThat(httpRequest, notNullValue());
        assertThat(httpRequest.getMethod(), equalTo(HttpPost.METHOD_NAME));
        assertThat(httpRequest.getEntity(), nullValue());
        assertThat(httpRequest.getEndpoint(), equalTo("/_tasks/_cancel"));
        assertThat(httpRequest.getParameters(), equalTo(expectedParams));
    }

    public void testListTasks() {
        {
            ListTasksRequest request = new ListTasksRequest();
            Map<String, String> expectedParams = new HashMap<>();
            if (randomBoolean()) {
                request.setDetailed(randomBoolean());
                if (request.getDetailed()) {
                    expectedParams.put("detailed", "true");
                }
            }

            request.setWaitForCompletion(randomBoolean());
            expectedParams.put("wait_for_completion", Boolean.toString(request.getWaitForCompletion()));

            if (randomBoolean()) {
                String timeout = randomTimeValue();
                request.setTimeout(timeout);
                expectedParams.put("timeout", timeout);
            }
            if (randomBoolean()) {
                if (randomBoolean()) {
                    TaskId taskId = new TaskId(randomAlphaOfLength(5), randomNonNegativeLong());
                    request.setParentTaskId(taskId);
                    expectedParams.put("parent_task_id", taskId.toString());
                } else {
                    request.setParentTask(TaskId.EMPTY_TASK_ID);
                }
            }
            if (randomBoolean()) {
                String[] nodes = generateRandomStringArray(10, 8, false);
                request.setNodes(nodes);
                if (nodes.length > 0) {
                    expectedParams.put("nodes", String.join(",", nodes));
                }
            }
            if (randomBoolean()) {
                String[] actions = generateRandomStringArray(10, 8, false);
                request.setActions(actions);
                if (actions.length > 0) {
                    expectedParams.put("actions", String.join(",", actions));
                }
            }
            expectedParams.put("group_by", "none");
            Request httpRequest = TasksRequestConverters.listTasks(request);
            assertThat(httpRequest, notNullValue());
            assertThat(httpRequest.getMethod(), equalTo(HttpGet.METHOD_NAME));
            assertThat(httpRequest.getEntity(), nullValue());
            assertThat(httpRequest.getEndpoint(), equalTo("/_tasks"));
            assertThat(httpRequest.getParameters(), equalTo(expectedParams));
        }
        {
            ListTasksRequest request = new ListTasksRequest();
            request.setTaskId(new TaskId(randomAlphaOfLength(5), randomNonNegativeLong()));
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, ()
                -> TasksRequestConverters.listTasks(request));
            assertEquals("TaskId cannot be used for list tasks request", exception.getMessage());
        }
    }
}
