/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
