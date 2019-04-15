/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransform;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.hasItemInArray;

public class TransportStopDataFrameTransformActionTests extends ESTestCase {

    public void testDataframeNodes() {
        String dataFrameIdFoo = "df-id-foo";
        String dataFrameIdBar = "df-id-bar";

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        tasksBuilder.addTask(dataFrameIdFoo,
                DataFrameField.TASK_NAME, new DataFrameTransform(dataFrameIdFoo),
                new PersistentTasksCustomMetaData.Assignment("node-1", "test assignment"));
        tasksBuilder.addTask(dataFrameIdBar,
                DataFrameField.TASK_NAME, new DataFrameTransform(dataFrameIdBar),
                new PersistentTasksCustomMetaData.Assignment("node-2", "test assignment"));
        tasksBuilder.addTask(MlTasks.jobTaskId("foo-1"), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams("foo-1"),
                new PersistentTasksCustomMetaData.Assignment("node-3", "test assignment"));

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()))
                .build();

        String[] nodes = TransportStopDataFrameTransformAction.dataframeNodes(Arrays.asList(dataFrameIdFoo, dataFrameIdBar), cs);
        assertEquals(2, nodes.length);
        assertThat(nodes, hasItemInArray("node-1"));
        assertThat(nodes, hasItemInArray("node-2"));
    }

    public void testDataframeNodes_NoTasks() {
        ClusterState emptyState = ClusterState.builder(new ClusterName("_name")).build();
        String[] nodes = TransportStopDataFrameTransformAction.dataframeNodes(Collections.singletonList("df-id"), emptyState);
        assertEquals(0, nodes.length);
    }
}
