/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.cancel;

import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

public class CancelTasksRequestTests extends ESTestCase {

    public void testGetDescription() {
        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        cancelTasksRequest.setActions("action1", "action2");
        cancelTasksRequest.setNodes("node1", "node2");
        cancelTasksRequest.setTaskId(new TaskId("node1", 1));
        cancelTasksRequest.setParentTaskId(new TaskId("node1", 0));
        assertEquals("reason[by user request], waitForCompletion[false], taskId[node1:1], " +
            "parentTaskId[node1:0], nodes[node1, node2], actions[action1, action2]", cancelTasksRequest.getDescription());
        Task task = cancelTasksRequest.createTask(1, "type", "action", null, Collections.emptyMap());
        assertEquals(cancelTasksRequest.getDescription(), task.getDescription());
    }
}
