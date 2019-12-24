/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.hasItemInArray;

public class TransformNodesTests extends ESTestCase {

    public void testTransformNodes() {
        String transformIdFoo = "df-id-foo";
        String transformIdBar = "df-id-bar";

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        tasksBuilder.addTask(transformIdFoo,
                TransformField.TASK_NAME, new TransformTaskParams(transformIdFoo, Version.CURRENT, null),
                new PersistentTasksCustomMetaData.Assignment("node-1", "test assignment"));
        tasksBuilder.addTask(transformIdBar,
                TransformField.TASK_NAME, new TransformTaskParams(transformIdBar, Version.CURRENT, null),
                new PersistentTasksCustomMetaData.Assignment("node-2", "test assignment"));
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
            },
            new PersistentTasksCustomMetaData.Assignment("node-3", "test assignment"));

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()))
                .build();

        String[] nodes = TransformNodes.transformTaskNodes(Arrays.asList(transformIdFoo, transformIdBar), cs);
        assertEquals(2, nodes.length);
        assertThat(nodes, hasItemInArray("node-1"));
        assertThat(nodes, hasItemInArray("node-2"));
    }

    public void testTransformNodes_NoTasks() {
        ClusterState emptyState = ClusterState.builder(new ClusterName("_name")).build();
        String[] nodes = TransformNodes.transformTaskNodes(Collections.singletonList("df-id"), emptyState);
        assertEquals(0, nodes.length);
    }
}
