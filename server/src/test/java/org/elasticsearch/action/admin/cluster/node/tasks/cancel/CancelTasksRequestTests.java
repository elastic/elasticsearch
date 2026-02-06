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

    public void testGetDescription_TruncatesNodesOnly() {
        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();

        String[] nodes = IntStream.rangeClosed(1, 12).mapToObj(i -> "node" + i).toArray(String[]::new);
        cancelTasksRequest.setNodes(nodes);
        cancelTasksRequest.setActions("action1", "action2");
        cancelTasksRequest.setTargetTaskId(new TaskId("node1", 1));
        cancelTasksRequest.setTargetParentTaskId(new TaskId("node1", 0));

        assertEquals(
            "reason[by user request], waitForCompletion[false], targetTaskId[node1:1], "
                + "targetParentTaskId[node1:0], "
                + "nodes[node1, node2, node3, node4, node5, node6, node7, node8, node9, node10, ... (2 more)], "
                + "actions[action1, action2]",
            cancelTasksRequest.getDescription()
        );

        Task task = cancelTasksRequest.createTask(1, "type", "action", null, Collections.emptyMap());
        assertEquals(cancelTasksRequest.getDescription(), task.getDescription());
    }

    public void testGetDescription_TruncatesActionsOnly() {
        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();

        String[] actions = IntStream.rangeClosed(1, 15).mapToObj(i -> "action" + i).toArray(String[]::new);
        cancelTasksRequest.setActions(actions);
        cancelTasksRequest.setNodes("node1", "node2");
        cancelTasksRequest.setTargetTaskId(new TaskId("node1", 1));
        cancelTasksRequest.setTargetParentTaskId(new TaskId("node1", 0));

        assertEquals(
            "reason[by user request], waitForCompletion[false], targetTaskId[node1:1], "
                + "targetParentTaskId[node1:0], "
                + "nodes[node1, node2], "
                + "actions[action1, action2, action3, action4, action5, action6, action7, action8, action9, action10, ... (5 more)]",
            cancelTasksRequest.getDescription()
        );

        Task task = cancelTasksRequest.createTask(1, "type", "action", null, Collections.emptyMap());
        assertEquals(cancelTasksRequest.getDescription(), task.getDescription());
    }

    public void testGetDescription_TruncatesNodesAndActions() {
        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();

        String[] nodes = IntStream.rangeClosed(1, 11).mapToObj(i -> "node" + i).toArray(String[]::new);
        String[] actions = IntStream.rangeClosed(1, 20).mapToObj(i -> "action" + i).toArray(String[]::new);

        cancelTasksRequest.setNodes(nodes);
        cancelTasksRequest.setActions(actions);
        cancelTasksRequest.setTargetTaskId(new TaskId("node1", 1));
        cancelTasksRequest.setTargetParentTaskId(new TaskId("node1", 0));

        assertEquals(
            "reason[by user request], waitForCompletion[false], targetTaskId[node1:1], "
                + "targetParentTaskId[node1:0], "
                + "nodes[node1, node2, node3, node4, node5, node6, node7, node8, node9, node10, ... (1 more)], "
                + "actions[action1, action2, action3, action4, action5, action6, action7, action8, action9, action10, ... (10 more)]",
            cancelTasksRequest.getDescription()
        );

        Task task = cancelTasksRequest.createTask(1, "type", "action", null, Collections.emptyMap());
        assertEquals(cancelTasksRequest.getDescription(), task.getDescription());
    }

    public void testGetDescription_ExactlyMaxDoesNotTruncate() {
        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();

        String[] nodes = IntStream.rangeClosed(1, 10).mapToObj(i -> "node" + i).toArray(String[]::new);
        String[] actions = IntStream.rangeClosed(1, 10).mapToObj(i -> "action" + i).toArray(String[]::new);

        cancelTasksRequest.setNodes(nodes);
        cancelTasksRequest.setActions(actions);
        cancelTasksRequest.setTargetTaskId(new TaskId("node1", 1));
        cancelTasksRequest.setTargetParentTaskId(new TaskId("node1", 0));

        assertEquals(
            "reason[by user request], waitForCompletion[false], targetTaskId[node1:1], "
                + "targetParentTaskId[node1:0], "
                + "nodes[node1, node2, node3, node4, node5, node6, node7, node8, node9, node10], "
                + "actions[action1, action2, action3, action4, action5, action6, action7, action8, action9, action10]",
            cancelTasksRequest.getDescription()
        );
    }
}
