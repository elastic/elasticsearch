/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.cancel;

import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;

public class CancelTasksRequestTests extends ESTestCase {

    public void testGetDescription_NoTruncation() {
        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        cancelTasksRequest.setActions("action1", "action2");
        cancelTasksRequest.setNodes("node1", "node2");
        cancelTasksRequest.setTargetTaskId(new TaskId("node1", 1));
        cancelTasksRequest.setTargetParentTaskId(new TaskId("node1", 0));
        assertEquals(
            "reason[by user request], waitForCompletion[false], targetTaskId[node1:1], "
                + "targetParentTaskId[node1:0], nodes[node1, node2], actions[action1, action2]",
            cancelTasksRequest.getDescription()
        );
        Task task = cancelTasksRequest.createTask(1, "type", "action", null, Collections.emptyMap());
        assertEquals(cancelTasksRequest.getDescription(), task.getDescription());
    }

    public void testGetDescription_BoundedCollectorTruncation() {
        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        cancelTasksRequest.setActions("action1", "action2");
        cancelTasksRequest.setTargetTaskId(new TaskId("node1", 1));
        cancelTasksRequest.setTargetParentTaskId(new TaskId("node1", 0));

        String huge = "n".repeat(800);
        String[] nodes = IntStream.rangeClosed(1, 5).mapToObj(i -> "node" + i + "-" + huge).toArray(String[]::new);
        cancelTasksRequest.setNodes(nodes);

        String description = cancelTasksRequest.getDescription();

        assertThat(description, containsString("], nodes["));
        assertThat(description, containsString("... (5 in total, "));
        assertThat(description, containsString(" omitted)]"));
        assertThat(description, containsString(", actions[action1, action2]"));

        Task task = cancelTasksRequest.createTask(1, "type", "action", null, Collections.emptyMap());
        assertEquals(description, task.getDescription());
    }
}
