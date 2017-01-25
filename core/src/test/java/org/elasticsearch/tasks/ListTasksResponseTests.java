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

package org.elasticsearch.tasks;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.test.ESTestCase;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class ListTasksResponseTests extends ESTestCase {

    public void testEmptyToString() {
        assertEquals("{\"tasks\":{}}", new ListTasksResponse().toString());
    }

    public void testNonEmptyToString() {
        TaskInfo info = new TaskInfo(
            new TaskId("node1", 1), "dummy-type", "dummy-action", "dummy-description", null, 0, 1, true, new TaskId("node1", 0));
        ListTasksResponse tasksResponse = new ListTasksResponse(singletonList(info), emptyList(), emptyList());
        assertEquals("{\"tasks\":{\"node1:1\":{\"node\":\"node1\",\"id\":1,\"type\":\"dummy-type\",\"action\":\"dummy-action\","
                + "\"description\":\"dummy-description\",\"start_time_in_millis\":0,\"running_time_in_nanos\":1,\"cancellable\":true,"
                + "\"parent_task_id\":\"node1:0\"}}}", tasksResponse.toString());
    }
}
