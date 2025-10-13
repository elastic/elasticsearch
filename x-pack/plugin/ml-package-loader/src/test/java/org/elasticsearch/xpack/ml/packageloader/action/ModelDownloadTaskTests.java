/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.elasticsearch.xpack.core.ml.MlTasks.MODEL_IMPORT_TASK_ACTION;
import static org.elasticsearch.xpack.core.ml.MlTasks.MODEL_IMPORT_TASK_TYPE;
import static org.elasticsearch.xpack.core.ml.MlTasks.downloadModelTaskDescription;
import static org.hamcrest.Matchers.containsString;

public class ModelDownloadTaskTests extends ESTestCase {
    public void testStatus() {
        var task = testTask();

        task.setProgress(100, 0);
        var taskInfo = task.taskInfo("node", true);
        var status = Strings.toString(taskInfo.status());
        assertThat(status, containsString("{\"total_parts\":100,\"downloaded_parts\":0}"));

        task.setProgress(100, 1);
        taskInfo = task.taskInfo("node", true);
        status = Strings.toString(taskInfo.status());
        assertThat(status, containsString("{\"total_parts\":100,\"downloaded_parts\":1}"));
    }

    public static ModelDownloadTask testTask() {
        return new ModelDownloadTask(
            0L,
            MODEL_IMPORT_TASK_TYPE,
            MODEL_IMPORT_TASK_ACTION,
            downloadModelTaskDescription("foo"),
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
    }
}
