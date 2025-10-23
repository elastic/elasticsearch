/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class GetAllSampleConfigurationActionRequestTests extends ESTestCase {

    protected GetAllSampleConfigurationAction.Request createTestInstance() {
        return new GetAllSampleConfigurationAction.Request(randomBoundedTimeValue());
    }

    public void testGettersAndSetters() {
        TimeValue timeout = randomBoundedTimeValue();
        GetAllSampleConfigurationAction.Request request = new GetAllSampleConfigurationAction.Request(timeout);

        assertThat(request.masterTimeout(), equalTo(timeout));
    }

    public void testCreateTaskReturnsCancellableTask() {
        GetAllSampleConfigurationAction.Request request = createTestInstance();

        long taskId = randomLong();
        String type = randomAlphaOfLength(10);
        String action = GetAllSampleConfigurationAction.NAME;
        TaskId parentTaskId = new TaskId(randomAlphaOfLength(10), randomLong());
        Map<String, String> headers = Map.of("header1", "value1");

        Task task = request.createTask(taskId, type, action, parentTaskId, headers);

        assertThat(task, notNullValue());
        assertThat(task, instanceOf(org.elasticsearch.tasks.CancellableTask.class));
        assertThat(task.getId(), equalTo(taskId));
        assertThat(task.getType(), equalTo(type));
        assertThat(task.getAction(), equalTo(action));
        assertThat(task.getParentTaskId(), equalTo(parentTaskId));
    }

    private TimeValue randomBoundedTimeValue() {
        return TimeValue.timeValueSeconds(randomIntBetween(5, 10));
    }
}
