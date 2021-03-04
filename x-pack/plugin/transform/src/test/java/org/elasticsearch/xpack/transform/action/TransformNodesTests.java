/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;

import java.util.Arrays;
import java.util.Collections;

public class TransformNodesTests extends ESTestCase {

    public void testTransformNodes() {
        String transformIdFoo = "df-id-foo";
        String transformIdBar = "df-id-bar";
        String transformIdFailed = "df-id-failed";
        String transformIdBaz = "df-id-baz";
        String transformIdOther = "df-id-other";
        String transformIdStopped = "df-id-stopped";

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        tasksBuilder.addTask(
            transformIdFoo,
            TransformField.TASK_NAME,
            new TransformTaskParams(transformIdFoo, Version.CURRENT, null, false),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment")
        );
        tasksBuilder.addTask(
            transformIdBar,
            TransformField.TASK_NAME,
            new TransformTaskParams(transformIdBar, Version.CURRENT, null, false),
            new PersistentTasksCustomMetadata.Assignment("node-2", "test assignment")
        );
        tasksBuilder.addTask("test-task1", "testTasks", new PersistentTaskParams() {
            @Override
            public String getWriteableName() {
                return "testTasks";
            }

            @Override
            public Version getMinimalSupportedVersion() {
                return null;
            }

            @Override
            public void writeTo(StreamOutput out) {

            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) {
                return null;
            }
        }, new PersistentTasksCustomMetadata.Assignment("node-3", "test assignment"));
        tasksBuilder.addTask(
            transformIdFailed,
            TransformField.TASK_NAME,
            new TransformTaskParams(transformIdFailed, Version.CURRENT, null, false),
            new PersistentTasksCustomMetadata.Assignment(null, "awaiting reassignment after node loss")
        );
        tasksBuilder.addTask(
            transformIdBaz,
            TransformField.TASK_NAME,
            new TransformTaskParams(transformIdBaz, Version.CURRENT, null, false),
            new PersistentTasksCustomMetadata.Assignment("node-2", "test assignment")
        );
        tasksBuilder.addTask(
            transformIdOther,
            TransformField.TASK_NAME,
            new TransformTaskParams(transformIdOther, Version.CURRENT, null, false),
            new PersistentTasksCustomMetadata.Assignment("node-3", "test assignment")
        );

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();

        // don't ask for transformIdOther
        TransformNodeAssignments transformNodeAssignments = TransformNodes.transformTaskNodes(
            Arrays.asList(transformIdFoo, transformIdBar, transformIdFailed, transformIdBaz, transformIdStopped),
            cs
        );
        assertEquals(2, transformNodeAssignments.getExecutorNodes().size());
        assertTrue(transformNodeAssignments.getExecutorNodes().contains("node-1"));
        assertTrue(transformNodeAssignments.getExecutorNodes().contains("node-2"));
        assertFalse(transformNodeAssignments.getExecutorNodes().contains(null));
        assertFalse(transformNodeAssignments.getExecutorNodes().contains("node-3"));
        assertEquals(1, transformNodeAssignments.getWaitingForAssignment().size());
        assertTrue(transformNodeAssignments.getWaitingForAssignment().contains(transformIdFailed));
        assertEquals(3, transformNodeAssignments.getAssigned().size());
        assertTrue(transformNodeAssignments.getAssigned().contains(transformIdFoo));
        assertTrue(transformNodeAssignments.getAssigned().contains(transformIdBar));
        assertTrue(transformNodeAssignments.getAssigned().contains(transformIdBaz));
        assertFalse(transformNodeAssignments.getAssigned().contains(transformIdFailed));
        assertEquals(1, transformNodeAssignments.getStopped().size());
        assertTrue(transformNodeAssignments.getStopped().contains(transformIdStopped));

        transformNodeAssignments = TransformNodes.transformTaskNodes(
            Arrays.asList(transformIdFoo, transformIdFailed),
            cs
        );

        assertEquals(1, transformNodeAssignments.getExecutorNodes().size());
        assertTrue(transformNodeAssignments.getExecutorNodes().contains("node-1"));
        assertEquals(1, transformNodeAssignments.getWaitingForAssignment().size());
        assertTrue(transformNodeAssignments.getWaitingForAssignment().contains(transformIdFailed));
        assertEquals(1, transformNodeAssignments.getAssigned().size());
        assertTrue(transformNodeAssignments.getAssigned().contains(transformIdFoo));
        assertFalse(transformNodeAssignments.getAssigned().contains(transformIdFailed));
        assertEquals(0, transformNodeAssignments.getStopped().size());
    }

    public void testTransformNodes_NoTasks() {
        ClusterState emptyState = ClusterState.builder(new ClusterName("_name")).build();
        TransformNodeAssignments transformNodeAssignments = TransformNodes.transformTaskNodes(
            Collections.singletonList("df-id"),
            emptyState
        );

        assertEquals(0, transformNodeAssignments.getExecutorNodes().size());
        assertEquals(1, transformNodeAssignments.getStopped().size());
        assertTrue(transformNodeAssignments.getStopped().contains("df-id"));
    }
}
