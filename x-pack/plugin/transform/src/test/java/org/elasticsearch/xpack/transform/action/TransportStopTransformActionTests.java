/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;

import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.rest.RestStatus.CONFLICT;
import static org.hamcrest.Matchers.equalTo;

public class TransportStopTransformActionTests extends ESTestCase {

    private MetaData.Builder buildMetadata(PersistentTasksCustomMetaData ptasks) {
        return MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE, ptasks);
    }

    public void testTaskStateValidationWithNoTasks() {
        MetaData.Builder metaData = MetaData.builder();
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name")).metaData(metaData);
        TransportStopTransformAction.validateTaskState(csBuilder.build(), Collections.singletonList("non-failed-task"), false);

        PersistentTasksCustomMetaData.Builder pTasksBuilder = PersistentTasksCustomMetaData.builder();
        csBuilder = ClusterState.builder(new ClusterName("_name")).metaData(buildMetadata(pTasksBuilder.build()));
        TransportStopTransformAction.validateTaskState(csBuilder.build(), Collections.singletonList("non-failed-task"), false);
    }

    public void testTaskStateValidationWithTransformTasks() {
        // Test with the task state being null
        PersistentTasksCustomMetaData.Builder pTasksBuilder = PersistentTasksCustomMetaData.builder()
            .addTask("non-failed-task",
                TransformTaskParams.NAME,
                new TransformTaskParams("transform-task-1", Version.CURRENT, null),
                new PersistentTasksCustomMetaData.Assignment("current-data-node-with-1-tasks", ""));
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name")).metaData(buildMetadata(pTasksBuilder.build()));

        TransportStopTransformAction.validateTaskState(csBuilder.build(), Collections.singletonList("non-failed-task"), false);

        // test again with a non failed task but this time it has internal state
        pTasksBuilder.updateTaskState("non-failed-task", new TransformState(TransformTaskState.STOPPED,
            IndexerState.STOPPED,
            null,
            0L,
            null,
            null));
        csBuilder = ClusterState.builder(new ClusterName("_name")).metaData(buildMetadata(pTasksBuilder.build()));

        TransportStopTransformAction.validateTaskState(csBuilder.build(), Collections.singletonList("non-failed-task"), false);

        pTasksBuilder.addTask("failed-task",
            TransformTaskParams.NAME,
            new TransformTaskParams("transform-task-1", Version.CURRENT, null),
            new PersistentTasksCustomMetaData.Assignment("current-data-node-with-1-tasks", ""))
            .updateTaskState("failed-task", new TransformState(TransformTaskState.FAILED,
                IndexerState.STOPPED,
                null,
                0L,
                "task has failed",
                null));
        csBuilder = ClusterState.builder(new ClusterName("_name")).metaData(buildMetadata(pTasksBuilder.build()));

        TransportStopTransformAction.validateTaskState(csBuilder.build(), Arrays.asList("non-failed-task", "failed-task"), true);

        TransportStopTransformAction.validateTaskState(csBuilder.build(), Collections.singletonList("non-failed-task"), false);

        ClusterState.Builder csBuilderFinal = ClusterState.builder(new ClusterName("_name")).metaData(buildMetadata(pTasksBuilder.build()));
        ElasticsearchStatusException ex = expectThrows(ElasticsearchStatusException.class,
            () -> TransportStopTransformAction.validateTaskState(csBuilderFinal.build(),
                Collections.singletonList("failed-task"),
                false));

        assertThat(ex.status(), equalTo(CONFLICT));
        assertThat(ex.getMessage(),
            equalTo(TransformMessages.getMessage(TransformMessages.CANNOT_STOP_FAILED_TRANSFORM,
                "failed-task",
                "task has failed")));
    }

}
